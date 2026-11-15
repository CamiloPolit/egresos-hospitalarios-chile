"""Paso 6 / Objetivo 3: efecto del COVID-19 comparando 2019 (pre-pandemia), 2020-2021 (pandemia)
y 2022-2023 (post-pandemia).

Salidas (output/tablas/):
  covid_totales_anio.csv          egresos, días-cama, letalidad por año + variación vs 2019,
                                  con y sin COVID
  covid_peso_anio.csv             peso de COVID (U07.1/U07.2) y neumonías/IRA (J09-J22, J96)
  covid_capitulo_variacion.csv    variación por capítulo CIE-10 vs 2019 (excluye COVID):
                                  causas desplazadas por la pandemia
  covid_categoria_variacion.csv   top categorías con mayor caída/alza de egresos 2020 vs 2019
  covid_recuperacion_categoria.csv categorías más alejadas de 2019 en el último año
  covid_region_anio.csv           egresos y letalidad COVID por región de residencia
  covid_sector_anio.csv           COVID por sector SNSS / No SNSS
  covid_edad_sexo.csv             COVID por grupo de edad y sexo
  covid_estadia_mismo_diag.csv    cambio de estadía y letalidad de los top diagnósticos no
                                  COVID entre años
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import harmonize as H
from config import ANIOS, EGRESOS_PARQUET, MIN_EGRESOS_COMPARACION, TABLAS_DIR
from metricas import agregar, es_covid
from spark_session import get_spark, guardar_csv

ANIO_BASE = 2019


def variacion_vs_base(df: DataFrame, claves: list, metricas: list) -> DataFrame:
    """Pivotea por año y agrega variación % de cada métrica respecto a 2019."""
    w = Window.partitionBy(*claves) if claves else Window.partitionBy(F.lit(1))
    out = df
    for m in metricas:
        base = F.max(F.when(F.col("anio") == ANIO_BASE, F.col(m))).over(w)
        out = out.withColumn(f"{m}_var_pct_vs_{ANIO_BASE}",
                             F.round(100 * (F.col(m) - base) / base, 2))
    return out


def main() -> None:
    spark = get_spark("06_covid")
    df = (spark.read.parquet(str(EGRESOS_PARQUET))
          .withColumn("es_covid", es_covid(F.col("diag1")))
          .withColumn("grupo_causa",
                      F.when(F.col("es_covid"), "COVID-19 (U07)")
                      .when(F.col("cod_cat").between("J09", "J22") | (F.col("cod_cat") == "J96"),
                            "Neumonía / IRA / insuf. respiratoria (J09-J22, J96)")
                      .otherwise("Otras causas"))
          .cache())
    met = ["egresos", "dias_cama", "fallecidos", "estadia_media", "letalidad_pct"]

    print("Totales por año")
    tot = agregar(df, ["anio"]).withColumn("alcance", F.lit("Todas las causas"))
    sin = agregar(df.filter(~F.col("es_covid")), ["anio"]).withColumn("alcance", F.lit("Sin COVID"))
    tot = variacion_vs_base(tot.unionByName(sin), ["alcance"], met)
    guardar_csv(tot, TABLAS_DIR / "covid_totales_anio.csv", orden=["alcance", "anio"])

    print("Peso de COVID y causas respiratorias")
    w = Window.partitionBy("anio")
    peso = (agregar(df, ["anio", "grupo_causa"])
            .withColumn("pct_egresos", F.round(100 * F.col("egresos") / F.sum("egresos").over(w), 2))
            .withColumn("pct_dias_cama", F.round(100 * F.col("dias_cama") / F.sum("dias_cama").over(w), 2))
            .withColumn("pct_fallecidos", F.round(100 * F.col("fallecidos") / F.sum("fallecidos").over(w), 2)))
    guardar_csv(peso, TABLAS_DIR / "covid_peso_anio.csv", orden=["anio", "grupo_causa"])

    print("Variación por capítulo (sin COVID)")
    no_covid = df.filter(~F.col("es_covid"))
    cap = variacion_vs_base(agregar(no_covid, ["anio", "capitulo", "glosa_capitulo"]),
                            ["capitulo"], met)
    guardar_csv(cap, TABLAS_DIR / "covid_capitulo_variacion.csv", orden=["capitulo", "anio"])

    print("Variación por categoría (sin COVID)")
    cat = variacion_vs_base(agregar(no_covid, ["anio", "cod_cat", "glosa_cat", "capitulo"]),
                            ["cod_cat"], met)
    # Categorías con al menos 1.000 egresos en 2019: las que más cayeron y más subieron en 2020
    base_ok = (cat.filter((F.col("anio") == ANIO_BASE) & (F.col("egresos") >= 1000))
               .select("cod_cat"))
    cat20 = (cat.filter(F.col("anio") == 2020).join(base_ok, "cod_cat")
             .select("cod_cat", F.col("egresos_var_pct_vs_2019").alias("_v20")))
    caidas = cat20.orderBy("_v20").limit(25)
    alzas = cat20.orderBy(F.desc("_v20")).limit(15)
    sel = caidas.unionByName(alzas)
    guardar_csv(cat.join(sel, "cod_cat").drop("_v20"), TABLAS_DIR / "covid_categoria_variacion.csv",
                orden=["cod_cat", "anio"])

    # Recuperación post-pandemia: categorías más alejadas de 2019 en el último año
    ultimo = max(ANIOS)
    cat_ult = (cat.filter(F.col("anio") == ultimo).join(base_ok, "cod_cat")
               .select("cod_cat", F.col("egresos_var_pct_vs_2019").alias("_v")))
    sel_ult = (cat_ult.orderBy("_v").limit(20)
               .unionByName(cat_ult.orderBy(F.desc("_v")).limit(20)).select("cod_cat"))
    guardar_csv(cat.join(sel_ult, "cod_cat"), TABLAS_DIR / "covid_recuperacion_categoria.csv",
                orden=["cod_cat", "anio"])

    print("COVID por región, sector, edad y sexo")
    cov = df.filter(F.col("es_covid"))
    guardar_csv(agregar(cov.filter(F.col("region_cod") != "99"), ["anio", "region_cod"])
                .withColumn("region_nombre", H.region_nombre(F.col("region_cod"))),
                TABLAS_DIR / "covid_region_anio.csv", orden=["anio", "region_cod"])
    guardar_csv(agregar(cov.filter(F.col("sector") != "SIN_DATO"), ["anio", "sector"]),
                TABLAS_DIR / "covid_sector_anio.csv", orden=["anio", "sector"])
    guardar_csv(agregar(cov.filter(F.col("edad_grupo") != "SIN_DATO")
                        .filter(F.col("sexo").isin("H", "M")), ["anio", "edad_grupo", "sexo"]),
                TABLAS_DIR / "covid_edad_sexo.csv", orden=["anio", "edad_grupo", "sexo"])

    print("Estadía y letalidad del mismo diagnóstico entre años")
    top_no_covid = (no_covid.filter(F.col("anio") == ANIO_BASE)
                    .groupBy("cod_cat").agg(F.sum("dias_estada").alias("d"))
                    .orderBy(F.desc("d")).limit(30).select("cod_cat"))
    mismo = variacion_vs_base(
        agregar(no_covid.join(top_no_covid, "cod_cat"), ["anio", "cod_cat", "glosa_cat", "sector"])
        .filter(F.col("egresos") >= MIN_EGRESOS_COMPARACION),
        ["cod_cat", "sector"], ["egresos", "estadia_media", "estadia_mediana", "letalidad_pct"])
    guardar_csv(mismo, TABLAS_DIR / "covid_estadia_mismo_diag.csv", orden=["cod_cat", "sector", "anio"])

    spark.stop()


if __name__ == "__main__":
    main()
