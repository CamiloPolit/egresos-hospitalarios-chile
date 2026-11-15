"""Métricas comunes de estadía y mortalidad hospitalaria."""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from config import ESTADIA_EXTREMA_DIAS, PERIODO

NIVELES_CIE = {
    "subcategoria": ["diag1", "glosa_sub", "cod_cat", "capitulo"],
    "categoria": ["cod_cat", "glosa_cat", "capitulo"],
    "grupo": ["cod_grupo", "glosa_grupo", "capitulo"],
    "capitulo": ["capitulo", "glosa_capitulo"],
}


def agregaciones() -> list:
    dias_sin_ext = F.when(F.col("dias_estada") <= ESTADIA_EXTREMA_DIAS, F.col("dias_estada"))
    return [
        F.count("*").alias("egresos"),
        F.sum("dias_estada").alias("dias_cama"),
        # Los días se imputan al año del alta: una estadía de décadas (p.ej. psiquiatría
        # de larga estadía) suma todos sus días al año de egreso. Esta versión los excluye.
        F.sum(dias_sin_ext).alias("dias_cama_sin_extremas"),
        F.round(F.avg("dias_estada"), 2).alias("estadia_media"),
        F.round(F.avg(dias_sin_ext), 2).alias("estadia_media_sin_extremas"),
        F.expr("percentile_approx(dias_estada, 0.5, 1000)").alias("estadia_mediana"),
        F.expr("percentile_approx(dias_estada, 0.9, 1000)").alias("estadia_p90"),
        F.sum("fallecido").alias("fallecidos"),
        F.round(100 * F.avg("fallecido"), 3).alias("letalidad_pct"),
    ]


def agregar(df: DataFrame, claves: list) -> DataFrame:
    return df.groupBy(*claves).agg(*agregaciones())


def ranking(df: DataFrame, particion: list, metrica: str = "dias_cama") -> DataFrame:
    """Agrega ranking, % del total y % acumulado (Pareto) dentro de cada partición."""
    w_tot = Window.partitionBy(*particion)
    w_rank = Window.partitionBy(*particion).orderBy(F.desc(metrica))
    w_acum = w_rank.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (df
            .withColumn("rank", F.row_number().over(w_rank))
            .withColumn(f"pct_{metrica}", F.round(100 * F.col(metrica) / F.sum(metrica).over(w_tot), 3))
            .withColumn(f"pct_{metrica}_acum",
                        F.round(100 * F.sum(metrica).over(w_acum) / F.sum(metrica).over(w_tot), 3)))


def con_total_periodo(df: DataFrame, etiqueta: str = PERIODO) -> DataFrame:
    """Duplica los datos con anio = etiqueta para obtener agregados del periodo completo."""
    base = df.withColumn("anio", F.col("anio").cast("string"))
    return base.unionByName(base.withColumn("anio", F.lit(etiqueta)))


def es_covid(diag: Column) -> Column:
    return diag.isin("U071", "U072")
