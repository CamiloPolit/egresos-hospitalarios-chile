"""Funciones de armonización de variables entre años (expresiones de columna Spark)."""
from pyspark.sql import Column
from pyspark.sql import functions as F

from config import REGIONES

ANON = "*"


def es_anonimizado(c: Column) -> Column:
    return c == F.lit(ANON)


def sector(c: Column) -> Column:
    return (
        F.when(c.startswith("Pertenecientes"), "SNSS")
        .when(c.startswith("No Pertenecientes"), "NO_SNSS")
        .otherwise("SIN_DATO")
    )


def sexo(c: Column, formato: str) -> Column:
    if formato == "texto":
        return (F.when(c == "HOMBRE", "H").when(c == "MUJER", "M")
                .when(c == ANON, "SIN_DATO").otherwise("OTRO"))
    # 2021: 1=Hombre, 2=Mujer, 3=Intersexual, 9=Desconocido (estándar DEIS;
    # consistente con la proporción M>H observada en 2019-2020)
    return (F.when(c == "1", "H").when(c == "2", "M")
            .when(c == "3", "OTRO").otherwise("SIN_DATO"))


def edad_grupo(c: Column, formato: str) -> Column:
    """Lleva el grupo de edad de origen al grupo común:
    <1, 1-9, 10-19, ..., 70-79, 80+ (80+ porque 2021 trae '85 A MAS')."""
    if formato == "decenal":
        mapa = {
            "menor de un año": "<1", "1 a 9": "1-9", "10 a 19": "10-19",
            "20 a 29": "20-29", "30 a 39": "30-39", "40 a 49": "40-49",
            "50 a 59": "50-59", "60 a 69": "60-69", "70 a 79": "70-79",
            "80 a 89": "80+", "90 y más": "80+",
        }
        expr = F.lit("SIN_DATO")
        for orig, dest in mapa.items():
            expr = F.when(c == orig, dest).otherwise(expr)
        return expr

    # Quinquenal (2021): "30 A 34 AÑOS", "85 A MAS", "menor a 7 días", "7 A 27 DIAS",
    # "28 DIAS A 2 MES", "2 MESES A MENOS DE 1 AÑO"
    u = F.upper(c)
    inicio = F.regexp_extract(u, r"^(\d+) A \d+ AÑOS", 1).cast("int")
    menor_1 = (u.startswith("MENOR") | u.contains("DIAS") | u.contains("DÍAS")
               | u.contains("MES"))
    decada = F.floor(inicio / 10) * 10
    return (
        F.when(menor_1, "<1")
        .when(u.startswith("85 A MAS") | (inicio >= 80), "80+")
        .when(inicio.isNotNull() & (inicio < 10), "1-9")
        .when(inicio.isNotNull(),
              F.concat(decada.cast("string"), F.lit("-"), (decada + 9).cast("string")))
        .otherwise("SIN_DATO")
    )


def region_cod(comuna: Column) -> Column:
    """Región derivada de los 2 primeros dígitos de la comuna (coincide en el 100% de
    los registros no anonimizados y permite recuperar la región de los anonimizados).
    88888=Extranjero, 99999=Ignorada y NULL se agrupan en '99'."""
    pref = F.substring(comuna, 1, 2)
    validas = [k for k in REGIONES if k != "99"]
    return F.when(pref.isin(validas), pref).otherwise(F.lit("99"))


def region_nombre(cod: Column) -> Column:
    expr = F.lit(None).cast("string")
    for k, v in REGIONES.items():
        expr = F.when(cod == k, v).otherwise(expr)
    return expr


def limpiar_texto(c: Column) -> Column:
    """Trim + vacío/'*'/'NULL' -> NULL."""
    t = F.trim(c)
    return F.when((t == "") | (t == ANON) | (F.upper(t) == "NULL"), None).otherwise(t)
