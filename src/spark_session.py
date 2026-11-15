"""Creación de la SparkSession local y utilidades de salida."""
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_JAVA17 = "/opt/homebrew/opt/openjdk@17"


def get_spark(app_name: str = "egresos-hospitalarios") -> SparkSession:
    if "JAVA_HOME" not in os.environ and Path(_JAVA17).exists():
        os.environ["JAVA_HOME"] = _JAVA17
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def guardar_csv(df: DataFrame, ruta: Path, orden=None) -> None:
    """Guarda una tabla agregada (pequeña) como un único CSV UTF-8 vía pandas."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    if orden:
        pdf = pdf.sort_values(orden).reset_index(drop=True)
    pdf.to_csv(ruta, index=False, encoding="utf-8")
    print(f"  -> {ruta.relative_to(ruta.parents[2])} ({len(pdf)} filas)")
