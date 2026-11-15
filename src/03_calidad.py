"""Paso 3: perfilado y controles de calidad sobre el Parquet limpio.

Falla (exit != 0) si los totales no coinciden con los medidos directamente en los CSV.
"""
import sys

from pyspark.sql import functions as F

from config import CALIDAD_DIR, EGRESOS_PARQUET, TOTALES_ESPERADOS
from spark_session import get_spark, guardar_csv


def main() -> int:
    spark = get_spark("03_calidad")
    df = spark.read.parquet(str(EGRESOS_PARQUET)).cache()

    print("\n== Totales por año")
    totales = (df.groupBy("anio").agg(
        F.count("*").alias("filas"),
        F.sum("fallecido").alias("fallecidos"),
        F.sum("dias_estada").alias("dias"),
        F.sum(F.col("anonimizado").cast("int")).alias("anonimizados"),
        F.sum(F.col("estadia_extrema").cast("int")).alias("estadias_extremas"),
        F.sum(F.col("dias_estada").isNull().cast("int")).alias("dias_nulos"),
        F.sum((F.col("capitulo") == "SIN_CIE").cast("int")).alias("diag_sin_cie"),
        F.sum((F.col("region_cod") == "99").cast("int")).alias("region_ignorada"),
        F.sum((F.col("edad_grupo") == "SIN_DATO").cast("int")).alias("edad_sin_dato"),
        F.sum(F.col("sexo").isin("SIN_DATO", "OTRO").cast("int")).alias("sexo_sin_dato_u_otro"),
        F.max("dias_estada").alias("dias_max"),
    ).orderBy("anio"))
    totales.show(truncate=False)
    guardar_csv(totales, CALIDAD_DIR / "totales_por_anio.csv")

    errores = []
    for r in totales.collect():
        esp = TOTALES_ESPERADOS.get(r["anio"])
        if not esp:
            continue
        for k in ("filas", "fallecidos", "dias"):
            if r[k] != esp[k]:
                errores.append(f"{r['anio']} {k}: obtenido {r[k]} vs esperado {esp[k]}")

    print("== Distribución de variables armonizadas")
    for col in ("sector", "sexo", "edad_grupo", "region_nombre", "prevision"):
        t = df.groupBy("anio", col).count()
        guardar_csv(t, CALIDAD_DIR / f"dist_{col}.csv", orden=["anio", col])

    # Grupos de edad de origen -> armonizado (para revisar el mapeo)
    guardar_csv(df.groupBy("anio", "edad_orig", "edad_grupo").count(),
                CALIDAD_DIR / "mapeo_edad.csv", orden=["anio", "edad_grupo"])

    sin_cie = (df.filter(F.col("capitulo") == "SIN_CIE")
               .groupBy("diag1").count().orderBy(F.desc("count")))
    guardar_csv(sin_cie, CALIDAD_DIR / "diagnosticos_sin_cie.csv")

    pct = (df.groupBy("anio").agg(*[
        F.expr(f"percentile_approx(dias_estada, {p}, 10000)").alias(nombre)
        for p, nombre in ((0.5, "p50"), (0.75, "p75"), (0.9, "p90"), (0.95, "p95"),
                          (0.99, "p99"), (0.999, "p99_9"))
    ]).orderBy("anio"))
    pct.show()
    guardar_csv(pct, CALIDAD_DIR / "percentiles_dias_estada.csv")

    spark.stop()
    if errores:
        print("\nCONTROLES FALLIDOS:\n  " + "\n  ".join(errores))
        return 1
    print("\nOK: filas, fallecidos y días coinciden con los CSV originales.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
