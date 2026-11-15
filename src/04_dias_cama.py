"""Paso 4 / Objetivo 1: diagnósticos que generan más días-cama.

Salidas (output/tablas/):
  dias_cama_<nivel>.csv          ranking completo por año (y periodo completo) para cada
                                 nivel CIE-10 (subcategoría, categoría, grupo, capítulo)
  top_dias_cama_<nivel>.csv      top N por año
  dias_cama_categoria_sector.csv top N categorías por año y sector (SNSS / No SNSS)
  dias_cama_categoria_sexo.csv   top N categorías por año y sexo
  dias_cama_capitulo_edad.csv    días-cama por capítulo y grupo de edad
"""
from pyspark.sql import functions as F

from config import EGRESOS_PARQUET, TABLAS_DIR, TOP_N
from metricas import NIVELES_CIE, agregar, con_total_periodo, ranking
from spark_session import get_spark, guardar_csv


def main() -> None:
    spark = get_spark("04_dias_cama")
    df = con_total_periodo(spark.read.parquet(str(EGRESOS_PARQUET))).cache()

    for nivel, claves in NIVELES_CIE.items():
        print(f"Nivel {nivel}")
        r = ranking(agregar(df, ["anio"] + claves), ["anio"]).cache()
        guardar_csv(r, TABLAS_DIR / f"dias_cama_{nivel}.csv", orden=["anio", "rank"])
        guardar_csv(r.filter(F.col("rank") <= TOP_N), TABLAS_DIR / f"top_dias_cama_{nivel}.csv",
                    orden=["anio", "rank"])
        # Control: no se pierden días-cama al agregar
        tot = r.groupBy("anio").agg(F.sum("dias_cama").alias("d")).collect()
        tot_base = {x["anio"]: x["d"] for x in df.groupBy("anio").agg(F.sum("dias_estada").alias("d")).collect()}
        assert all(x["d"] == tot_base[x["anio"]] for x in tot), f"Días-cama no cuadran en {nivel}"

    claves_cat = NIVELES_CIE["categoria"]
    print("Por sector")
    sec = df.filter(F.col("sector") != "SIN_DATO")
    r = ranking(agregar(sec, ["anio", "sector"] + claves_cat), ["anio", "sector"])
    guardar_csv(r.filter(F.col("rank") <= TOP_N), TABLAS_DIR / "dias_cama_categoria_sector.csv",
                orden=["anio", "sector", "rank"])

    print("Por sexo")
    sx = df.filter(F.col("sexo").isin("H", "M"))
    r = ranking(agregar(sx, ["anio", "sexo"] + claves_cat), ["anio", "sexo"])
    guardar_csv(r.filter(F.col("rank") <= TOP_N), TABLAS_DIR / "dias_cama_categoria_sexo.csv",
                orden=["anio", "sexo", "rank"])

    print("Capítulo x edad")
    ed = df.filter(F.col("edad_grupo") != "SIN_DATO")
    guardar_csv(agregar(ed, ["anio", "edad_grupo"] + NIVELES_CIE["capitulo"]),
                TABLAS_DIR / "dias_cama_capitulo_edad.csv", orden=["anio", "edad_grupo", "capitulo"])

    spark.stop()


if __name__ == "__main__":
    main()
