"""CryPySpark: encrypt and decrypt PySpark columns of any type.

    import crypyspark

    df = df.encrypt("ssn", "address", key="my passphrase")
    df = df.decrypt("ssn", "address", key="my passphrase")

Importing the package adds ``DataFrame.encrypt`` and ``DataFrame.decrypt``.
Unity Catalog helpers (governance tags) live in ``crypyspark.databricks``.
"""

from crypyspark._core import (
    METADATA_KEY,
    decrypt,
    decrypt_column,
    encrypt,
    encrypt_column,
    parse_type,
    secret_key,
    set_key,
    type_tag_column,
)

__all__ = [
    "METADATA_KEY",
    "decrypt",
    "decrypt_column",
    "encrypt",
    "encrypt_column",
    "parse_type",
    "secret_key",
    "set_key",
    "type_tag_column",
]
__version__ = "0.1.0"


def _install() -> None:
    """Attach ``.encrypt`` / ``.decrypt`` to classic and Spark Connect DataFrames."""
    classes = []
    from pyspark.sql import DataFrame

    classes.append(DataFrame)
    try:  # Spark Connect (Databricks serverless / shared clusters, DBR 13+)
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        classes.append(ConnectDataFrame)
    except Exception:
        pass
    for cls in classes:
        cls.encrypt = encrypt
        cls.decrypt = decrypt


_install()
