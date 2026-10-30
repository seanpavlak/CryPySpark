"""Unity Catalog integration: governance tags on encrypted columns.

    from crypyspark import databricks as cdb

    cdb.save_encrypted_table(df, "main.hr.people", ["ssn", "address"], key=k)
    people = cdb.read_decrypted_table("main.hr.people", key=k)

Encrypted columns get these column tags, visible in Catalog Explorer, in
``information_schema.column_tags`` and usable by tag-based (ABAC) policies:

    crypyspark_encrypted = "aes-256-gcm"
    crypyspark_type      = "<compact type tag>"   (omitted if longer than 255 chars)

Any extra tags you pass (e.g. ``{"pii": "ssn"}``) are applied as well.
"""

from typing import Dict, Iterable, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession

from crypyspark import _core, _tags

TAG_ENCRYPTED = "crypyspark_encrypted"
TAG_TYPE = "crypyspark_type"
_ALGORITHM = "aes-256-gcm"
_MAX_TAG_VALUE = 255


def _spark(spark: Optional[SparkSession]) -> SparkSession:
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")
    return spark


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _table_parts(spark: SparkSession, table: str) -> List[str]:
    parts = [p.strip("`") for p in table.split(".")]
    if len(parts) == 3:
        return parts
    if len(parts) == 2:
        return [spark.catalog.currentCatalog()] + parts
    return [spark.catalog.currentCatalog(), spark.catalog.currentDatabase()] + parts


def tag_encrypted_columns(
    table: str,
    columns: Optional[Iterable[str]] = None,
    extra_tags: Optional[Dict[str, str]] = None,
    spark: Optional[SparkSession] = None,
) -> List[str]:
    """Set crypyspark governance tags on a table's encrypted columns.

    With no ``columns``, every column whose schema metadata marks it as
    encrypted by crypyspark is tagged. Returns the column names tagged.
    """
    spark = _spark(spark)
    schema = spark.table(table).schema
    if columns is None:
        columns = [f.name for f in schema.fields if _core.METADATA_KEY in (f.metadata or {})]
    columns = list(columns)
    for name in columns:
        tags = {TAG_ENCRYPTED: _ALGORITHM}
        info = (schema[name].metadata or {}).get(_core.METADATA_KEY) or {}
        if len(info.get("type", "")) <= _MAX_TAG_VALUE and info.get("type"):
            tags[TAG_TYPE] = info["type"]
        tags.update(extra_tags or {})
        assignments = ", ".join(f"{_literal(k)} = {_literal(v)}" for k, v in tags.items())
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {_ident(name)} SET TAGS ({assignments})")
    return columns


def encrypted_columns(table: str, spark: Optional[SparkSession] = None) -> Dict[str, Optional[str]]:
    """``{column: type_tag_or_None}`` for every column tagged as crypyspark-encrypted."""
    spark = _spark(spark)
    catalog, schema, name = _table_parts(spark, table)
    rows = spark.sql(
        f"SELECT column_name, tag_name, tag_value FROM {_ident(catalog)}.information_schema.column_tags "
        f"WHERE schema_name = {_literal(schema)} AND table_name = {_literal(name)} "
        f"AND tag_name IN ({_literal(TAG_ENCRYPTED)}, {_literal(TAG_TYPE)})"
    ).collect()
    found: Dict[str, Optional[str]] = {}
    for r in rows:
        found.setdefault(r.column_name, None)
        if r.tag_name == TAG_TYPE:
            found[r.column_name] = r.tag_value
    return found


def save_encrypted_table(
    df: DataFrame,
    table: str,
    columns: Union[str, Iterable[str]],
    key: Optional[_core.KeyLike] = None,
    mode: str = "overwrite",
    extra_tags: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Encrypt ``columns``, write ``df`` to a Unity Catalog table and tag the columns."""
    columns = [columns] if isinstance(columns, str) else list(columns)
    encrypted = _core.encrypt(df, columns, key=key)
    encrypted.write.mode(mode).saveAsTable(table)
    # UC tags live on top-level columns; encrypted nested fields stay self-describing via their data.
    tag_encrypted_columns(table, [c for c in columns if c in df.columns], extra_tags, df.sparkSession)
    return encrypted


def read_decrypted_table(
    table: str,
    key: Optional[_core.KeyLike] = None,
    columns: Optional[Iterable[str]] = None,
    on_error: str = "raise",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read a table and decrypt its encrypted columns.

    Columns come from ``columns``, else the Unity Catalog tags, else schema
    metadata. Types come from the UC type tag when present (so no data has to
    be sampled), else metadata, else the tag stored in the data itself.
    """
    spark = _spark(spark)
    df = spark.table(table)
    try:
        tagged = encrypted_columns(table, spark)
    except Exception:  # not Unity Catalog (e.g. hive_metastore / local Spark)
        tagged = {}
    if columns is None:
        columns = list(tagged) or [f.name for f in df.schema.fields if _core.METADATA_KEY in (f.metadata or {})]
    dtypes = {c: _tags.from_tag(t) for c, t in tagged.items() if t and c in set(columns)}
    return _core.decrypt(df, list(columns), key=key, dtypes=dtypes, on_error=on_error)
