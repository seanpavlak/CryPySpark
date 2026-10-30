import datetime as dt
import math
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    YearMonthIntervalType,
)

import crypyspark

KEY = "correct horse battery staple"

ADDRESS = StructType(
    [
        StructField("street", StringType()),
        StructField("zip", IntegerType()),
        StructField("geo", ArrayType(DoubleType())),
    ]
)

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("bool", BooleanType()),
        StructField("byte", ByteType()),
        StructField("short", ShortType()),
        StructField("long", LongType()),
        StructField("float", FloatType()),
        StructField("double", DoubleType()),
        StructField("dec", DecimalType(38, 18)),
        StructField("str", StringType()),
        StructField("bin", BinaryType()),
        StructField("date", DateType()),
        StructField("ts", TimestampType()),
        StructField("ts_ntz", TimestampNTZType()),
        StructField("dti", DayTimeIntervalType()),
        # Python can't build year-month intervals; this int becomes one in the fixture.
        StructField("ymi", IntegerType()),
        StructField("arr", ArrayType(StringType())),
        StructField("map_int_keys", MapType(IntegerType(), StringType())),
        StructField("map_date_keys", MapType(DateType(), ArrayType(BinaryType()))),
        StructField("address", ADDRESS),
        StructField(
            "deep",
            ArrayType(
                MapType(
                    StringType(),
                    StructType(
                        [
                            StructField("when", TimestampType()),
                            StructField("tags", ArrayType(MapType(LongType(), DateType()))),
                            StructField("addr", ADDRESS),
                        ]
                    ),
                )
            ),
        ),
    ]
)

ROWS = [
    (
        1,
        True,
        -128,
        32767,
        2**63 - 1,
        3.1415927,
        math.nan,
        Decimal("12345678901234567890.123456789012345678"),
        "héllo, 世界 \"quoted\" \n\t\u0001 🎉",
        bytes(range(256)),
        dt.date(1, 1, 1),
        dt.datetime(2024, 2, 29, 23, 59, 59, 123456),
        dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
        dt.timedelta(days=3, hours=4, microseconds=5),
        14,
        ["a", None, ""],
        {1: "one", -2: None},
        {dt.date(2020, 1, 1): [b"\x00\xff", None]},
        ("1 Main St", 12345, [1.5, -0.0]),
        [
            {
                "k1": (
                    dt.datetime(1970, 1, 1, 0, 0, 0, 1),
                    [{7: dt.date(1999, 12, 31)}, {}],
                    (None, None, []),
                ),
                "k2": None,
            },
            None,
        ],
    ),
    (
        2,
        False,
        0,
        0,
        0,
        math.inf,
        -math.inf,
        Decimal("-0.000000000000000001"),
        "",
        b"",
        dt.date(9999, 12, 31),
        dt.datetime(1900, 1, 1, 0, 0, 0, 1),
        dt.datetime(1, 1, 1),
        dt.timedelta(microseconds=-1),
        -1,
        [],
        {},
        {},
        (None, None, None),
        [],
    ),
    tuple([3] + [None] * (len(SCHEMA.fields) - 1)),
]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def df(spark):
    df = spark.createDataFrame(ROWS, SCHEMA)
    return df.withColumn("ymi", F.make_ym_interval(F.lit(0), F.col("ymi")))


def strip_nullability(t):
    if isinstance(t, StructType):
        return StructType([StructField(f.name, strip_nullability(f.dataType)) for f in t.fields])
    if isinstance(t, ArrayType):
        return ArrayType(strip_nullability(t.elementType))
    if isinstance(t, MapType):
        return MapType(strip_nullability(t.keyType), strip_nullability(t.valueType))
    return t


def rows(df):
    # repr() makes NaN compare equal to NaN and keeps exact Decimal/datetime/bytes values.
    # Year-month intervals can't be collected into Python, so compare their string form.
    cols = [F.col(c).cast("string").alias(c) if isinstance(df.schema[c].dataType, YearMonthIntervalType) else F.col(c)
            for c in df.columns]
    return [repr(r) for r in df.select(*cols).orderBy("id").collect()]


COLUMNS = [f.name for f in SCHEMA.fields if f.name != "id"]


def test_roundtrip_every_type(df):
    enc = df.encrypt(COLUMNS, key=KEY)
    for name in COLUMNS:
        assert enc.schema[name].dataType == BinaryType()
    assert enc.columns == df.columns

    dec = enc.decrypt(COLUMNS, key=KEY)
    assert strip_nullability(dec.schema) == strip_nullability(df.schema)
    assert rows(dec) == rows(df)


def test_decrypt_all_tagged_columns_by_default(df):
    dec = df.encrypt("address", "deep", "ts", key=KEY).decrypt(key=KEY)
    assert rows(dec) == rows(df)
    assert crypyspark.METADATA_KEY not in dec.schema["address"].metadata


def test_type_is_recovered_from_data_when_metadata_is_lost(df, spark, tmp_path):
    enc = df.encrypt(COLUMNS, key=KEY, as_string=True)
    assert enc.schema["str"].dataType == StringType()
    path = str(tmp_path / "csv")
    enc.write.option("header", True).option("escape", '"').csv(path)
    loaded = spark.read.option("header", True).option("escape", '"').option("multiLine", True).csv(path)
    loaded = loaded.withColumn("id", F.col("id").cast("int"))
    assert all(crypyspark.METADATA_KEY not in f.metadata for f in loaded.schema.fields)

    dec = loaded.decrypt(COLUMNS, key=KEY)
    assert strip_nullability(dec.schema) == strip_nullability(df.schema)
    assert rows(dec) == rows(df)


def test_nested_field(df):
    enc = df.encrypt("address.zip", key=KEY)
    assert enc.schema["address"].dataType["zip"].dataType == BinaryType()
    assert enc.select("address.street").collect() == df.select("address.street").collect()
    dec = enc.decrypt("address.zip", key=KEY)
    assert rows(dec) == rows(df)


def test_ciphertext_is_randomised(spark):
    one = spark.createDataFrame([("same",), ("same",)], "s string").encrypt("s", key=KEY)
    a, b = [bytes(r.s) for r in one.collect()]
    assert a != b


def test_nulls_are_encrypted_too(df):
    enc = df.encrypt("str", key=KEY)
    assert enc.where(F.col("str").isNull()).count() == 0
    assert rows(enc.decrypt("str", key=KEY)) == rows(df)


def test_wrong_key(df):
    enc = df.encrypt("str", key=KEY)
    with pytest.raises(Exception):
        enc.decrypt("str", key="nope").collect()
    nulls = enc.decrypt("str", key="nope", dtypes={"str": "string"}, on_error="null")
    assert nulls.where(F.col("str").isNotNull()).count() == 0


def test_default_key_and_column_api(df):
    crypyspark.set_key(b"\x01" * 32)
    try:
        enc = df.select("id", crypyspark.encrypt_column("dec", DecimalType(38, 18)).alias("dec"))
        dec = enc.select("id", crypyspark.decrypt_column("dec", "decimal(38,18)").alias("dec"))
        assert rows(dec) == rows(df.select("id", "dec"))
    finally:
        crypyspark.set_key(None)


def test_column_key_matches_string_key(df):
    enc = df.encrypt("str", key=KEY)
    dec = enc.decrypt("str", key=F.lit(KEY))
    assert rows(dec) == rows(df)


def test_explicit_dtypes_skip_discovery(df):
    enc = df.encrypt("address", key=KEY).withMetadata("address", {})
    dec = enc.decrypt("address", key=KEY, dtypes={"address": ADDRESS})
    assert rows(dec) == rows(df)


def test_missing_key_raises(df):
    with pytest.raises(ValueError, match="No key"):
        df.encrypt("str")


def test_type_tags_roundtrip():
    from crypyspark import _tags

    for field in SCHEMA.fields + [StructField("w", StructType([StructField('we"ird.,:name', YearMonthIntervalType())]))]:
        tag = _tags.to_tag(field.dataType)
        assert strip_nullability(_tags.from_tag(tag)) == strip_nullability(field.dataType)
    assert _tags.to_tag(IntegerType()) == "i"
    assert crypyspark.parse_type("array<int>") == ArrayType(IntegerType())


def test_one_discovery_job_for_many_columns(df, spark):
    enc = df.encrypt("str", "address", "deep", key=KEY)
    stripped = enc.select(*[F.col(c).alias(c, metadata={}) for c in enc.columns])
    dec = stripped.decrypt("str", "address", "deep", key=KEY)
    assert rows(dec) == rows(df)


def test_databricks_helpers_fall_back_off_unity_catalog(df, spark):
    from crypyspark import databricks as cdb

    assert cdb._literal("it's") == "'it\\'s'"
    assert cdb._ident("we`ird") == "`we``ird`"
    enc = df.encrypt("str", "address", key=KEY)
    enc.write.mode("overwrite").saveAsTable("crypyspark_people")
    dec = cdb.read_decrypted_table("crypyspark_people", key=KEY)
    assert rows(dec) == rows(df)
