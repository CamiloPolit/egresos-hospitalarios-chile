"""Paso 2: ingesta de los CSV de egresos, armonización a un esquema común y escritura en
Parquet particionado por año (data/clean/egresos.parquet).

Esquema de salida:
  anio, sector, sexo, edad_grupo, edad_orig, region_cod, region_nombre, comuna_cod,
  comuna_nombre, prevision, pais_origen, diag1, diag2, dias_estada, fallecido,
  anonimizado, estadia_extrema, glosa_sub, cod_cat, glosa_cat, cod_grupo, glosa_grupo,
  capitulo, glosa_capitulo
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import harmonize as H
from config import ANIOS, CIE10_PARQUET, EGRESOS_PARQUET, ENCODING, ESTADIA_EXTREMA_DIAS, SEPARADOR
from spark_session import get_spark


def leer_anio(spark, anio: int, cfg: dict) -> DataFrame:
    raw = (spark.read
           .option("header", True)
           .option("sep", SEPARADOR)
           .option("encoding", ENCODING)
           .option("quote", '"')
           .option("escape", '"')  # 2022: comillas dobladas ("") dentro de campos entre comillas
           .option("inferSchema", False)  # todo como string: preserva ceros a la izquierda
           .csv(str(cfg["archivo"])))
    cols = cfg["columnas"]
    faltantes = [v for v in cols.values() if v not in raw.columns]
    if faltantes:
        raise ValueError(f"{anio}: columnas no encontradas {faltantes}; hay {raw.columns}")

    c = {k: F.col(v) for k, v in cols.items()}
    region = H.region_cod(c["comuna_cod"])
    dias = F.trim(c["dias_estada"]).cast("int")

    return raw.select(
        F.lit(anio).alias("anio"),
        H.sector(c["sector_orig"]).alias("sector"),
        H.sexo(c["sexo_orig"], cfg["formato_sexo"]).alias("sexo"),
        H.edad_grupo(c["edad_orig"], cfg["formato_edad"]).alias("edad_grupo"),
        H.limpiar_texto(c["edad_orig"]).alias("edad_orig"),
        region.alias("region_cod"),
        H.region_nombre(region).alias("region_nombre"),
        H.limpiar_texto(c["comuna_cod"]).alias("comuna_cod"),
        H.limpiar_texto(c["comuna_nombre"]).alias("comuna_nombre"),
        H.limpiar_texto(c["prevision"]).alias("prevision"),
        H.limpiar_texto(c["pais_origen"]).alias("pais_origen"),
        F.upper(F.trim(c["diag1"])).alias("diag1"),
        F.upper(H.limpiar_texto(c["diag2"])).alias("diag2"),
        dias.alias("dias_estada"),
        (F.trim(c["condicion_egreso"]) == "2").cast("int").alias("fallecido"),
        H.es_anonimizado(c["region_orig"]).alias("anonimizado"),
        (dias > ESTADIA_EXTREMA_DIAS).alias("estadia_extrema"),
        F.trim(c["anio_orig"]).alias("_anio_orig"),
    )


def main() -> None:
    spark = get_spark("02_ingesta")

    partes = []
    for anio, cfg in ANIOS.items():
        df = leer_anio(spark, anio, cfg)
        # Validación: el año declarado en la fila coincide (o está anonimizado)
        malos = df.filter(~F.col("_anio_orig").isin(str(anio), "*")).count()
        if malos:
            print(f"  AVISO {anio}: {malos} filas con ANO_EGRESO distinto")
        partes.append(df.drop("_anio_orig"))

    egresos = reduce(DataFrame.unionByName, partes)

    cie = spark.read.parquet(str(CIE10_PARQUET))
    # Lookup por subcategoría; si el código no existe, fallback por categoría (3 caracteres)
    cie_sub = cie.select(F.col("cod_sub").alias("diag1"), "glosa_sub", "cod_cat", "glosa_cat",
                         "cod_grupo", "glosa_grupo", "capitulo", "glosa_capitulo")
    cie_cat = (cie.groupBy("cod_cat")
               .agg(F.first("glosa_cat").alias("glosa_cat_fb"),
                    F.first("cod_grupo").alias("cod_grupo_fb"),
                    F.first("glosa_grupo").alias("glosa_grupo_fb"),
                    F.first("capitulo").alias("capitulo_fb"),
                    F.first("glosa_capitulo").alias("glosa_capitulo_fb"))
               .withColumnRenamed("cod_cat", "_cat"))

    egresos = (egresos
               .join(F.broadcast(cie_sub), "diag1", "left")
               .withColumn("_cat", F.substring("diag1", 1, 3))
               .join(F.broadcast(cie_cat), "_cat", "left")
               .withColumn("cod_cat", F.coalesce("cod_cat", F.when(F.col("capitulo_fb").isNotNull(), F.col("_cat"))))
               .withColumn("glosa_cat", F.coalesce("glosa_cat", "glosa_cat_fb"))
               .withColumn("cod_grupo", F.coalesce("cod_grupo", "cod_grupo_fb"))
               .withColumn("glosa_grupo", F.coalesce("glosa_grupo", "glosa_grupo_fb"))
               .withColumn("capitulo", F.coalesce("capitulo", "capitulo_fb", F.lit("SIN_CIE")))
               .withColumn("glosa_capitulo", F.coalesce("glosa_capitulo", "glosa_capitulo_fb",
                                                        F.lit("Código no encontrado en CIE-10")))
               .drop("_cat", "glosa_cat_fb", "cod_grupo_fb", "glosa_grupo_fb",
                     "capitulo_fb", "glosa_capitulo_fb"))

    (egresos.repartition("anio")
     .write.mode("overwrite").partitionBy("anio")
     .parquet(str(EGRESOS_PARQUET)))

    out = spark.read.parquet(str(EGRESOS_PARQUET))
    print("Filas por año:")
    out.groupBy("anio").count().orderBy("anio").show()
    print(f"Escrito en {EGRESOS_PARQUET}")
    spark.stop()


if __name__ == "__main__":
    main()
