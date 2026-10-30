"""Compact, exact type tags stored inside every encrypted cell.

Atomic types are a single character, so the per-row cost is a few bytes:

    int -> "i"    decimal(10,2) -> "D10,2"    array<string> -> "a<s>"
    map<int,date> -> "m<i,t>"    struct<a:int,b:string> -> 'r{"a":i,"b":s}'

Field names are JSON-quoted, so any name (dots, commas, quotes...) is safe.
"""

import json
from typing import Tuple

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    YearMonthIntervalType,
)

_ATOMIC = {
    BooleanType: "b",
    ByteType: "y",
    ShortType: "h",
    IntegerType: "i",
    LongType: "l",
    FloatType: "f",
    DoubleType: "d",
    StringType: "s",
    BinaryType: "B",
    DateType: "t",
    TimestampType: "T",
    TimestampNTZType: "N",
    NullType: "0",
}
_ATOMIC_BY_CODE = {code: cls for cls, code in _ATOMIC.items()}
_json_decoder = json.JSONDecoder()


def to_tag(dt: DataType) -> str:
    if isinstance(dt, ArrayType):
        return f"a<{to_tag(dt.elementType)}>"
    if isinstance(dt, MapType):
        return f"m<{to_tag(dt.keyType)},{to_tag(dt.valueType)}>"
    if isinstance(dt, StructType):
        fields = ",".join(f"{json.dumps(f.name)}:{to_tag(f.dataType)}" for f in dt.fields)
        return f"r{{{fields}}}"
    if isinstance(dt, DecimalType):
        return f"D{dt.precision},{dt.scale}"
    if isinstance(dt, DayTimeIntervalType):
        return f"v{dt.startField},{dt.endField}"
    if isinstance(dt, YearMonthIntervalType):
        return f"u{dt.startField},{dt.endField}"
    code = _ATOMIC.get(type(dt))
    if code is None:  # char/varchar and anything else Spark treats as a string
        if dt.typeName() in ("char", "varchar"):
            return "s"
        raise TypeError(f"crypyspark does not support columns of type {dt.simpleString()}")
    return code


def from_tag(tag: str) -> DataType:
    dt, end = _parse(tag, 0)
    if end != len(tag):
        raise ValueError(f"Invalid crypyspark type tag: {tag!r}")
    return dt


def _expect(s: str, i: int, ch: str) -> int:
    if s[i : i + 1] != ch:
        raise ValueError(f"Invalid crypyspark type tag {s!r}: expected {ch!r} at {i}")
    return i + 1


def _ints(s: str, i: int) -> Tuple[int, int, int]:
    j = i
    while j < len(s) and (s[j].isdigit() or s[j] == ","):
        j += 1
    a, b = s[i:j].split(",")
    return int(a), int(b), j


def _parse(s: str, i: int) -> Tuple[DataType, int]:
    code = s[i : i + 1]
    i += 1
    if code == "a":
        elem, i = _parse(s, _expect(s, i, "<"))
        return ArrayType(elem), _expect(s, i, ">")
    if code == "m":
        k, i = _parse(s, _expect(s, i, "<"))
        v, i = _parse(s, _expect(s, i, ","))
        return MapType(k, v), _expect(s, i, ">")
    if code == "r":
        i = _expect(s, i, "{")
        fields = []
        while s[i : i + 1] != "}":
            if fields:
                i = _expect(s, i, ",")
            name, i = _json_decoder.raw_decode(s, i)
            ftype, i = _parse(s, _expect(s, i, ":"))
            fields.append(StructField(name, ftype))
        return StructType(fields), i + 1
    if code in ("D", "v", "u"):
        a, b, i = _ints(s, i)
        cls = {"D": DecimalType, "v": DayTimeIntervalType, "u": YearMonthIntervalType}[code]
        return cls(a, b), i
    if code in _ATOMIC_BY_CODE:
        return _ATOMIC_BY_CODE[code](), i
    raise ValueError(f"Invalid crypyspark type tag {s!r} at {i - 1}")
