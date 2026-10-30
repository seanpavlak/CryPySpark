"""Lossless, type-preserving conversion between any Spark value and JSON.

``to_json`` / ``from_json`` already round-trip most types, but a few lose
information (timestamp precision, map keys that aren't strings, ...). Every
value is first rewritten into a "JSON-safe" shape using native Spark
expressions, so no Python UDFs are involved and everything runs in the JVM
(works on Databricks classic, shared and serverless compute).
"""

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    CalendarIntervalType,
    DataType,
    DateType,
    DayTimeIntervalType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    YearMonthIntervalType,
)

_STRING_CAST_TYPES = (TimestampNTZType, DayTimeIntervalType, YearMonthIntervalType)


def _check_supported(dt: DataType) -> None:
    if isinstance(dt, CalendarIntervalType) or type(dt).__name__ == "VariantType":
        raise TypeError(f"crypyspark does not support columns of type {dt.simpleString()}")


def needs_rewrite(dt: DataType) -> bool:
    """True when ``dt`` (or anything nested inside it) can't go through JSON as-is."""
    _check_supported(dt)
    if isinstance(dt, ArrayType):
        return needs_rewrite(dt.elementType)
    if isinstance(dt, StructType):
        return any(needs_rewrite(f.dataType) for f in dt.fields)
    # Maps always become arrays of entries so non-string keys survive.
    return isinstance(
        dt, (MapType, BinaryType, DateType, TimestampType, NullType) + _STRING_CAST_TYPES
    )


def safe_type(dt: DataType) -> DataType:
    """The JSON-safe type that ``encode`` produces for ``dt``."""
    _check_supported(dt)
    if not needs_rewrite(dt):
        return dt
    if isinstance(dt, ArrayType):
        return ArrayType(safe_type(dt.elementType), True)
    if isinstance(dt, MapType):
        entry = StructType(
            [StructField("k", safe_type(dt.keyType)), StructField("v", safe_type(dt.valueType))]
        )
        return ArrayType(entry, True)
    if isinstance(dt, StructType):
        return StructType([StructField(f.name, safe_type(f.dataType)) for f in dt.fields])
    if isinstance(dt, DateType):
        return IntegerType()
    if isinstance(dt, TimestampType):
        return LongType()
    return StringType()  # binary, null, timestamp_ntz, ANSI intervals


def encode(c: Column, dt: DataType) -> Column:
    """Rewrite column ``c`` of type ``dt`` into ``safe_type(dt)``."""
    if not needs_rewrite(dt):
        return c
    if isinstance(dt, ArrayType):
        return F.transform(c, lambda x: encode(x, dt.elementType))
    if isinstance(dt, MapType):
        return F.transform(
            F.map_entries(c),
            lambda e: F.struct(
                encode(e["key"], dt.keyType).alias("k"),
                encode(e["value"], dt.valueType).alias("v"),
            ),
        )
    if isinstance(dt, StructType):
        fields = [encode(c.getField(f.name), f.dataType).alias(f.name) for f in dt.fields]
        return F.when(c.isNull(), F.lit(None)).otherwise(F.struct(*fields))
    if isinstance(dt, BinaryType):
        return F.base64(c)
    if isinstance(dt, DateType):
        return F.unix_date(c)
    if isinstance(dt, TimestampType):
        return F.unix_micros(c)
    return c.cast(StringType())  # null, timestamp_ntz, ANSI intervals


def decode(c: Column, dt: DataType) -> Column:
    """Inverse of ``encode``: turn ``safe_type(dt)`` back into ``dt``."""
    if not needs_rewrite(dt):
        return c
    if isinstance(dt, ArrayType):
        return F.transform(c, lambda x: decode(x, dt.elementType))
    if isinstance(dt, MapType):
        return F.map_from_entries(
            F.transform(
                c,
                lambda e: F.struct(
                    decode(e["k"], dt.keyType).alias("key"),
                    decode(e["v"], dt.valueType).alias("value"),
                ),
            )
        )
    if isinstance(dt, StructType):
        fields = [decode(c.getField(f.name), f.dataType).alias(f.name) for f in dt.fields]
        return F.when(c.isNull(), F.lit(None)).otherwise(F.struct(*fields))
    if isinstance(dt, BinaryType):
        return F.unbase64(c)
    if isinstance(dt, DateType):
        return F.date_from_unix_date(c)
    if isinstance(dt, TimestampType):
        return F.timestamp_micros(c)
    if isinstance(dt, NullType):
        return F.lit(None)
    return c.cast(dt)  # timestamp_ntz, ANSI intervals
