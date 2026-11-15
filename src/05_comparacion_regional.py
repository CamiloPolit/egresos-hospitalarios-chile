"""Paso 5 / Objetivo 2: estadía y mortalidad del mismo diagnóstico entre regiones de
residencia y entre sectores (SNSS vs No SNSS), cruda y ajustada por edad y sexo.

Ajuste: estandarización indirecta. Para cada año y estrato (categoría CIE-10 x grupo de
edad x sexo) se calcula la tasa nacional de letalidad y la estadía media nacional; el valor
esperado de cada región/sector es la suma de sus egresos por estrato x la tasa nacional.
  razon_letalidad = fallecidos observados / esperados   (SMR; IC 95% de Byar)
  razon_estadia   = días observados / días esperados    (sólo estadías <= 365 días)
Valores > 1 indican más mortalidad / estadía que la esperada dado el perfil de edad, sexo
y diagnóstico de sus pacientes.

Notas:
- Región = región de RESIDENCIA del paciente (los datos no traen región del hospital).
- Los análisis crudos por región incluyen filas anonimizadas (región recuperada desde la
  comuna); los ajustados las excluyen porque no tienen edad ni sexo.

Salidas (output/tablas/):
  top_diagnosticos_comparacion.csv
  region_diagnostico_cruda.csv / sector_diagnostico_cruda.csv
  region_diagnostico_ajustada.csv / sector_diagnostico_ajustada.csv
  region_todas_causas_ajustada.csv / sector_todas_causas_ajustada.csv
  sector_region_todas_causas_ajustada.csv
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (EGRESOS_PARQUET, PERIODO, ESTADIA_EXTREMA_DIAS, MIN_EGRESOS_COMPARACION,
                    TABLAS_DIR, TOP_DIAG_COMPARACION)
import harmonize as H
from metricas import agregar, con_total_periodo
from spark_session import get_spark, guardar_csv

ESTRATO_DIAG = ["cod_cat", "edad_grupo", "sexo"]


def ic_byar(obs: str, esp: str):
    """IC 95% aproximado de Byar para una razón observado/esperado Poisson."""
    o, e = F.col(obs), F.col(esp)
    o1 = o + 1
    inf = F.when(o == 0, F.lit(0.0)).otherwise(
        o * F.pow(1 - 1 / (9 * o) - 1.96 / (3 * F.sqrt(o)), 3) / e)
    sup = o1 * F.pow(1 - 1 / (9 * o1) + 1.96 / (3 * F.sqrt(o1)), 3) / e
    return F.round(inf, 3), F.round(sup, 3)


def estandarizar(df: DataFrame, grupo: list, salida: list, estrato: list) -> DataFrame:
    """Estandarización indirecta.

    df      : egresos con edad y sexo conocidos
    grupo   : columnas que definen la unidad comparada (p.ej. ['region_cod'])
    salida  : columnas de agregación final además de anio + grupo (p.ej. ['cod_cat'])
    estrato : columnas del estrato de referencia nacional
    """
    ok = F.col("dias_estada") <= ESTADIA_EXTREMA_DIAS
    base = df.withColumn("_dias_ok", F.when(ok, F.col("dias_estada")).otherwise(0)) \
             .withColumn("_n_ok", ok.cast("int"))

    ref = (base.groupBy("anio", *estrato)
           .agg(F.count("*").alias("n_ref"), F.sum("fallecido").alias("m_ref"),
                F.sum("_dias_ok").alias("d_ref"), F.sum("_n_ok").alias("nok_ref"))
           .withColumn("tasa_m", F.col("m_ref") / F.col("n_ref"))
           .withColumn("media_d", F.when(F.col("nok_ref") > 0, F.col("d_ref") / F.col("nok_ref")).otherwise(0))
           .select("anio", *estrato, "tasa_m", "media_d"))

    claves_estrato = list(dict.fromkeys(["anio"] + grupo + salida + estrato))
    obs = (base.groupBy(*claves_estrato)
           .agg(F.count("*").alias("n"), F.sum("fallecido").alias("m"),
                F.sum("_dias_ok").alias("d"), F.sum("_n_ok").alias("nok")))

    res = (obs.join(ref, ["anio"] + estrato)
           .withColumn("m_esp", F.col("n") * F.col("tasa_m"))
           .withColumn("d_esp", F.col("nok") * F.col("media_d"))
           .groupBy("anio", *grupo, *salida)
           .agg(F.sum("n").alias("egresos"),
                F.sum("m").alias("fallecidos"),
                F.round(F.sum("m_esp"), 2).alias("fallecidos_esperados"),
                F.sum("d").alias("dias_obs"),
                F.round(F.sum("d_esp"), 1).alias("dias_esperados"),
                F.sum("nok").alias("egresos_estadia_ok")))
    inf, sup = ic_byar("fallecidos", "fallecidos_esperados")
    return (res
            .withColumn("letalidad_pct", F.round(100 * F.col("fallecidos") / F.col("egresos"), 3))
            .withColumn("razon_letalidad", F.round(F.col("fallecidos") / F.col("fallecidos_esperados"), 3))
            .withColumn("razon_letalidad_ic_inf", inf)
            .withColumn("razon_letalidad_ic_sup", sup)
            .withColumn("letalidad_ajustada_pct",  # razón x letalidad nacional del mismo conjunto
                        F.round(F.col("razon_letalidad") * 100 * F.col("fallecidos_esperados")
                                / F.col("egresos"), 3))
            .withColumn("estadia_media", F.round(F.col("dias_obs") / F.col("egresos_estadia_ok"), 2))
            .withColumn("estadia_esperada", F.round(F.col("dias_esperados") / F.col("egresos_estadia_ok"), 2))
            .withColumn("razon_estadia", F.round(F.col("dias_obs") / F.col("dias_esperados"), 3))
            .withColumn("significativo_letalidad",
                        F.when(F.col("razon_letalidad_ic_inf") > 1, "mayor")
                        .when(F.col("razon_letalidad_ic_sup") < 1, "menor").otherwise("ns"))
            .filter(F.col("egresos") >= MIN_EGRESOS_COMPARACION))


def main() -> None:
    spark = get_spark("05_comparacion_regional")
    df = con_total_periodo(spark.read.parquet(str(EGRESOS_PARQUET))).cache()

    # Diagnósticos (categoría) con más días-cama en el periodo completo
    top = (df.filter(F.col("anio") == PERIODO)
           .groupBy("cod_cat", "glosa_cat", "capitulo")
           .agg(F.sum("dias_estada").alias("dias_cama"), F.count("*").alias("egresos"))
           .orderBy(F.desc("dias_cama")).limit(TOP_DIAG_COMPARACION).cache())
    guardar_csv(top, TABLAS_DIR / "top_diagnosticos_comparacion.csv")
    top_cods = [r["cod_cat"] for r in top.collect()]
    glosas = top.select("cod_cat", "glosa_cat")

    df_top = df.filter(F.col("cod_cat").isin(top_cods))
    con_region = df_top.filter(F.col("region_cod") != "99")
    con_sector = df_top.filter(F.col("sector") != "SIN_DATO")

    print("Comparación cruda")
    guardar_csv(agregar(con_region, ["anio", "region_cod", "region_nombre", "cod_cat", "glosa_cat"])
                .filter(F.col("egresos") >= MIN_EGRESOS_COMPARACION),
                TABLAS_DIR / "region_diagnostico_cruda.csv", orden=["anio", "cod_cat", "region_cod"])
    guardar_csv(agregar(con_sector, ["anio", "sector", "cod_cat", "glosa_cat"]),
                TABLAS_DIR / "sector_diagnostico_cruda.csv", orden=["anio", "cod_cat", "sector"])

    print("Comparación ajustada por edad y sexo (estandarización indirecta)")
    # Referencia nacional: todos los egresos con edad y sexo conocidos (incluye región 99)
    conocidos = df.filter(F.col("edad_grupo") != "SIN_DATO").filter(F.col("sexo").isin("H", "M"))
    conocidos_top = conocidos.filter(F.col("cod_cat").isin(top_cods))

    reg_diag = (estandarizar(conocidos_top, ["region_cod"], ["cod_cat"], ESTRATO_DIAG)
                .filter(F.col("region_cod") != "99")
                .withColumn("region_nombre", H.region_nombre(F.col("region_cod")))
                .join(glosas, "cod_cat"))
    guardar_csv(reg_diag, TABLAS_DIR / "region_diagnostico_ajustada.csv",
                orden=["anio", "cod_cat", "region_cod"])

    sec_diag = (estandarizar(conocidos_top, ["sector"], ["cod_cat"], ESTRATO_DIAG)
                .join(glosas, "cod_cat"))
    guardar_csv(sec_diag, TABLAS_DIR / "sector_diagnostico_ajustada.csv",
                orden=["anio", "cod_cat", "sector"])

    # Todas las causas: ajusta por diagnóstico (case-mix) + edad + sexo
    print("Todas las causas, ajustado por diagnóstico, edad y sexo")
    guardar_csv(estandarizar(conocidos, ["region_cod"], [], ESTRATO_DIAG)
                .filter(F.col("region_cod") != "99")
                .withColumn("region_nombre", H.region_nombre(F.col("region_cod"))),
                TABLAS_DIR / "region_todas_causas_ajustada.csv", orden=["anio", "region_cod"])
    guardar_csv(estandarizar(conocidos, ["sector"], [], ESTRATO_DIAG),
                TABLAS_DIR / "sector_todas_causas_ajustada.csv", orden=["anio", "sector"])
    guardar_csv(estandarizar(conocidos, ["region_cod", "sector"], [], ESTRATO_DIAG)
                .filter(F.col("region_cod") != "99")
                .withColumn("region_nombre", H.region_nombre(F.col("region_cod"))),
                TABLAS_DIR / "sector_region_todas_causas_ajustada.csv",
                orden=["anio", "region_cod", "sector"])

    spark.stop()


if __name__ == "__main__":
    main()
