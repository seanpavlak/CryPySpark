"""Encrypt / decrypt Spark columns of any type with AES-256-GCM.

Every encrypted cell is ``aes_encrypt(GCM)`` of the JSON envelope::

    {"t": "<compact type tag>", "v": <value>}

The ``t`` tag travels inside every value, so a cell can always be decrypted
back to the exact value *and* type it started as, even after the column has
been written out, read back, or moved somewhere schema metadata is lost.

Everything is built from native Spark SQL expressions (no Python UDFs), so the
work runs inside the JVM / Photon with no serialization to Python workers.
"""

import hashlib
from typing import Dict, Iterable, List, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, DataType, StringType, StructField, StructType

from crypyspark import _codec, _tags

KeyLike = Union[str, bytes, bytearray, Column]
TypeLike = Union[DataType, str]

METADATA_KEY = "crypyspark"
_TAG = "t"
_VALUE = "v"

_default_key: Optional[KeyLike] = None


# --------------------------------------------------------------------------- keys


def set_key(key: Optional[KeyLike]) -> None:
    """Set the key used whenever ``key=`` is not passed. ``None`` clears it."""
    global _default_key
    _default_key = key


def secret_key(scope: str, key: str) -> Column:
    """Key read from a Databricks secret *inside the engine*.

    The secret never passes through Python or appears in the query plan, unlike
    a key fetched with ``dbutils.secrets.get`` and passed as a string.
    """
    scope_sql = scope.replace("'", "\\'")
    key_sql = key.replace("'", "\\'")
    return F.expr(f"secret('{scope_sql}', '{key_sql}')")


def _key_column(key: Optional[KeyLike]) -> Column:
    """Any passphrase/bytes/Column becomes a 32-byte AES-256 key via SHA-256."""
    key = _default_key if key is None else key
    if key is None:
        raise ValueError("No key given. Pass key=... or call crypyspark.set_key(...) first.")
    if isinstance(key, Column):
        return F.unhex(F.sha2(key, 256))
    if isinstance(key, str):
        key = key.encode("utf-8")
    if not isinstance(key, (bytes, bytearray)) or not key:
        raise TypeError("key must be a non-empty str, bytes or Column")
    # Derived once on the driver; the cluster only ever sees a constant literal.
    return F.lit(bytearray(hashlib.sha256(key).digest()))


# --------------------------------------------------------------------------- types


def parse_type(t: TypeLike) -> DataType:
    """Accept a DataType, a crypyspark type tag, or a DDL string like ``"array<int>"``."""
    if isinstance(t, DataType):
        return t
    try:
        return _tags.from_tag(t)
    except (ValueError, KeyError, IndexError):
        pass
    try:
        return DataType.fromDDL(t)  # pyspark >= 4.0
    except AttributeError:
        from pyspark.sql.types import _parse_datatype_string

        return _parse_datatype_string(t)


def _envelope_type(dt: DataType) -> StructType:
    return StructType([StructField(_TAG, StringType()), StructField(_VALUE, _codec.safe_type(dt))])


# --------------------------------------------------------------------------- columns


def _as_col(col: Union[Column, str]) -> Column:
    return F.col(col) if isinstance(col, str) else col


def encrypt_column(
    col: Union[Column, str], dtype: TypeLike, key: Optional[KeyLike] = None, as_string: bool = False
) -> Column:
    """Encrypt ``col`` (of Spark type ``dtype``) into a BINARY column.

    ``as_string=True`` base64-encodes the result (for CSV/JSON sinks); binary is
    smaller and faster. Prefer ``df.encrypt(...)``, which figures out ``dtype``.
    """
    dt = parse_type(dtype)
    envelope = F.struct(F.lit(_tags.to_tag(dt)).alias(_TAG), _codec.encode(_as_col(col), dt).alias(_VALUE))
    cipher = F.aes_encrypt(F.to_json(envelope).cast("binary"), _key_column(key), F.lit("GCM"))
    return F.base64(cipher) if as_string else cipher


def _plaintext(c: Column, key: Optional[KeyLike], on_error: str, as_string: bool) -> Column:
    if on_error not in ("raise", "null"):
        raise ValueError("on_error must be 'raise' or 'null'")
    fn = F.aes_decrypt if on_error == "raise" else F.try_aes_decrypt
    return fn(F.unbase64(c) if as_string else c, _key_column(key), F.lit("GCM")).cast("string")


def decrypt_column(
    col: Union[Column, str],
    dtype: TypeLike,
    key: Optional[KeyLike] = None,
    on_error: str = "raise",
    as_string: bool = False,
) -> Column:
    """Decrypt a column produced by ``encrypt_column`` back into ``dtype``.

    ``on_error="null"`` yields NULL for cells that fail to decrypt (wrong key,
    tampered data) instead of failing the job.
    """
    dt = parse_type(dtype)
    parsed = F.from_json(_plaintext(_as_col(col), key, on_error, as_string), _envelope_type(dt))
    value = parsed.getField(_VALUE)
    if not _codec.needs_rewrite(dt):
        return value
    # Decoding references the value several times (once per struct field, ...).
    # Binding it to a lambda variable guarantees decrypt + parse run once per row.
    return F.transform(F.array(value), lambda v: _codec.decode(v, dt))[0]


def type_tag_column(col: Union[Column, str], key: Optional[KeyLike] = None, as_string: bool = False) -> Column:
    """The original type tag carried inside each encrypted cell (NULL if undecryptable)."""
    return F.get_json_object(_plaintext(_as_col(col), key, "null", as_string), f"$.{_TAG}")


# --------------------------------------------------------------------------- dataframes


def _flatten(columns: Tuple) -> List[str]:
    out: List[str] = []
    for c in columns:
        if isinstance(c, str):
            out.append(c)
        elif isinstance(c, Iterable):
            out.extend(c)
        else:
            raise TypeError(f"column names must be strings, got {c!r}")
    return out


def _resolve(df: DataFrame, name: str) -> Tuple[List[str], StructField]:
    """Find a top-level column or a dotted path into nested structs."""
    schema = df.schema
    if name in schema.names:
        return [name], schema[name]
    path = name.split(".")
    current: DataType = schema
    field = None
    for part in path:
        if not isinstance(current, StructType) or part not in current.names:
            raise ValueError(f"Column {name!r} not found (nested paths may only go through structs)")
        field = current[part]
        current = field.dataType
    return path, field


def _get(df: DataFrame, path: List[str]) -> Column:
    c = df[path[0]]
    for part in path[1:]:
        c = c.getField(part)
    return c


def _apply(df: DataFrame, updates: List[Tuple[List[str], Column, Optional[dict]]]) -> DataFrame:
    """Apply every column replacement in ONE projection, keeping column order."""
    replaced: Dict[str, Column] = {}
    metadata: Dict[str, dict] = {}
    for path, value, md in updates:
        top = path[0]
        if len(path) == 1:
            replaced[top] = value
            if md is not None:
                metadata[top] = md
        else:
            base = replaced.get(top, df[top])
            replaced[top] = base.withField(".".join(path[1:]), value)
    projection = []
    for f in df.schema.fields:
        if f.name in replaced:
            projection.append(replaced[f.name].alias(f.name, metadata=metadata.get(f.name, f.metadata or {})))
        else:
            projection.append(df[f.name])
    return df.select(*projection)


def encrypt(
    df: DataFrame,
    *columns: Union[str, Iterable[str]],
    key: Optional[KeyLike] = None,
    as_string: bool = False,
) -> DataFrame:
    """Encrypt the given columns in place (they become BINARY, or STRING with ``as_string``).

    Columns may be any type, including structs, arrays and maps, nested to any
    depth. Use ``"parent.child"`` to encrypt a single field inside a struct.

        df = df.encrypt("ssn", "address", key=my_key)
    """
    names = _flatten(columns)
    if not names:
        raise ValueError("encrypt() needs at least one column name")
    updates = []
    for name in names:
        path, field = _resolve(df, name)
        md = dict(field.metadata or {})
        md[METADATA_KEY] = {"type": _tags.to_tag(field.dataType)}
        value = encrypt_column(_get(df, path), field.dataType, key, as_string)
        updates.append((path, value, md))
    return _apply(df, updates)


def _discover_types(
    df: DataFrame, targets: List[Tuple[str, List[str], bool]], key: Optional[KeyLike]
) -> Dict[str, DataType]:
    """Read type tags out of encrypted cells: one tiny ``LIMIT 1`` job for all columns."""
    if not targets:
        return {}
    cols = [(_get(df, path), is_str) for _, path, is_str in targets]
    tags = [type_tag_column(c, key, is_str).alias(f"_t{i}") for i, (c, is_str) in enumerate(cols)]
    any_present = cols[0][0].isNotNull()
    for c, _ in cols[1:]:
        any_present = any_present | c.isNotNull()
    row = df.where(any_present).select(*tags).limit(1).first()
    found: Dict[str, DataType] = {}
    for i, (name, path, is_str) in enumerate(targets):
        tag = row[i] if row is not None else None
        if tag is None:  # this column happened to be NULL in that row; look it up on its own
            c = cols[i][0]
            single = df.where(c.isNotNull()).select(type_tag_column(c, key, is_str)).limit(1).first()
            tag = single[0] if single is not None else None
        if tag is None:
            raise ValueError(
                f"Could not detect the original type of {name!r} (no decryptable rows). "
                "Pass dtypes={...} explicitly or check the key."
            )
        found[name] = _tags.from_tag(tag)
    return found


def decrypt(
    df: DataFrame,
    *columns: Union[str, Iterable[str]],
    key: Optional[KeyLike] = None,
    dtypes: Optional[Dict[str, TypeLike]] = None,
    on_error: str = "raise",
) -> DataFrame:
    """Decrypt columns back to their exact original values and types.

    With no column names, every column ``encrypt`` tagged is decrypted. The
    original type comes from ``dtypes`` if given, else the column's schema
    metadata, else the type tag stored inside the encrypted data itself.

        df = df.decrypt("ssn", "address", key=my_key)
    """
    dtypes = dict(dtypes or {})
    names = _flatten(columns) or [f.name for f in df.schema.fields if METADATA_KEY in (f.metadata or {})]
    if not names:
        raise ValueError("No column names given and no columns carry crypyspark metadata")

    resolved = []
    known: Dict[str, DataType] = {}
    unknown = []
    for name in names:
        path, field = _resolve(df, name)
        if not isinstance(field.dataType, (BinaryType, StringType)):
            raise TypeError(f"Column {name!r} is {field.dataType.simpleString()}, not an encrypted column")
        is_str = isinstance(field.dataType, StringType)
        md = dict(field.metadata or {})
        tagged = md.pop(METADATA_KEY, None)
        if name in dtypes:
            known[name] = parse_type(dtypes[name])
        elif tagged and "type" in tagged:
            known[name] = _tags.from_tag(tagged["type"])
        else:
            unknown.append((name, path, is_str))
        resolved.append((name, path, is_str, md))
    known.update(_discover_types(df, unknown, key))

    updates = [
        (path, decrypt_column(_get(df, path), known[name], key, on_error, is_str), md)
        for name, path, is_str, md in resolved
    ]
    return _apply(df, updates)
