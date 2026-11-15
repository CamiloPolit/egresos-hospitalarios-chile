"""Configuración central del proyecto: rutas, archivos por año y mapeo de columnas.

Para agregar un año nuevo (p. ej. 2022) basta con añadir una entrada en ANIOS
indicando el archivo CSV y, si cambian, los nombres de columnas de origen.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT / "data"
REF_DIR = DATA_DIR / "ref"
CLEAN_DIR = DATA_DIR / "clean"
OUT_DIR = ROOT / "output"
TABLAS_DIR = OUT_DIR / "tablas"
GRAFICOS_DIR = OUT_DIR / "graficos"
CALIDAD_DIR = OUT_DIR / "calidad"

CIE10_PARQUET = REF_DIR / "cie10.parquet"
EGRESOS_PARQUET = CLEAN_DIR / "egresos.parquet"

# Diccionario (cualquiera de los años sirve; la hoja CIE-10 es idéntica)
DICCIONARIO_XLSX = ROOT / "EGRESOS_2019" / "Diccionario BD egresos hospitalario.xlsx"
DICCIONARIO_HOJA_CIE10 = "codigo CIE-10"

ENCODING = "ISO-8859-1"
SEPARADOR = ";"

# Nombres canónicos -> nombre de columna en el archivo de origen.
_COLS_BASE = {
    "sector_orig": "PERTENENCIA_ESTABLECIMIENTO_SALUD",
    "sexo_orig": "SEXO",
    "edad_orig": "GRUPO_EDAD",
    "pais_origen": "GLOSA_PAIS_ORIGEN",
    "comuna_cod": "COMUNA_RESIDENCIA",
    "comuna_nombre": "GLOSA_COMUNA_RESIDENCIA",
    "region_orig": "REGION_RESIDENCIA",
    "prevision": "GLOSA_PREVISION",
    "anio_orig": "ANO_EGRESO",
    "diag1": "DIAG1",
    "diag2": "DIAG2",
    "dias_estada": "DIAS_ESTADA",
    "condicion_egreso": "CONDICION_EGRESO",
}

ANIOS = {
    2019: {
        "archivo": ROOT / "EGRESOS_2019" / "EGRE_DATOS_ABIERTOS_2019.csv",
        # El header de 2019 viene truncado
        "columnas": {**_COLS_BASE, "sector_orig": "PERTENENCIA_ESTABLECIMIENTO_SALU"},
        "formato_sexo": "texto",
        "formato_edad": "decenal",
    },
    2020: {
        "archivo": ROOT / "EGRESOS_2020" / "EGRE_DATOS_ABIERTOS_2020.csv",
        "columnas": dict(_COLS_BASE),
        "formato_sexo": "texto",
        "formato_edad": "decenal",
    },
    2021: {
        "archivo": ROOT / "EGRESOS_2021" / "EGR_DATOS_ABIERTO_2021.csv",
        "columnas": dict(_COLS_BASE),
        "formato_sexo": "codigo",
        "formato_edad": "quinquenal",
    },
    2022: {
        # Trae además GLOSA_INTERV_Q_PPAL / GLOSA_PROCED_PPAL entre comillas con ';' internos
        "archivo": ROOT / "EGRESOS_2022" / "EGRE_DATOS_ABIERTOS_2022.csv",
        "columnas": dict(_COLS_BASE),
        "formato_sexo": "texto",
        "formato_edad": "decenal",
    },
    2023: {
        # Header truncado como 2019. Las filas anonimizadas (8%) tampoco traen comuna.
        "archivo": ROOT / "EGRESOS_2023" / "EGRESOS_2023.csv",
        "columnas": {**_COLS_BASE, "sector_orig": "PERTENENCIA_ESTABLECIMIENTO_SALU"},
        "formato_sexo": "texto",
        "formato_edad": "decenal",
    },
}

ANIO_MIN, ANIO_MAX = min(ANIOS), max(ANIOS)
PERIODO = f"{ANIO_MIN}-{ANIO_MAX}"  # etiqueta del agregado del periodo completo

# Totales esperados, medidos directamente sobre los CSV (control de calidad).
TOTALES_ESPERADOS = {
    2019: {"filas": 1_667_180, "fallecidos": 36_389, "dias": 9_982_117},
    2020: {"filas": 1_330_477, "fallecidos": 42_961, "dias": 8_694_958},
    2021: {"filas": 1_467_062, "fallecidos": 48_068, "dias": 9_624_299},
    2022: {"filas": 1_597_118, "fallecidos": 42_652, "dias": 9_818_737},
    2023: {"filas": 1_612_267, "fallecidos": 37_735, "dias": 10_032_975},
}

REGIONES = {
    "01": "Tarapacá",
    "02": "Antofagasta",
    "03": "Atacama",
    "04": "Coquimbo",
    "05": "Valparaíso",
    "06": "O'Higgins",
    "07": "Maule",
    "08": "Biobío",
    "09": "La Araucanía",
    "10": "Los Lagos",
    "11": "Aysén",
    "12": "Magallanes",
    "13": "Metropolitana",
    "14": "Los Ríos",
    "15": "Arica y Parinacota",
    "16": "Ñuble",
    "99": "Sin región (ignorada, extranjero o anonimizada)",
}
# Orden geográfico norte -> sur, útil para gráficos
ORDEN_REGIONES = ["15", "01", "02", "03", "04", "05", "13", "06", "07", "16",
                  "08", "09", "14", "10", "11", "12", "99"]

ORDEN_EDAD = ["<1", "1-9", "10-19", "20-29", "30-39", "40-49",
              "50-59", "60-69", "70-79", "80+"]

ESTADIA_EXTREMA_DIAS = 365
MIN_EGRESOS_COMPARACION = 30
TOP_N = 20
TOP_DIAG_COMPARACION = 30

CODIGOS_COVID = ["U071", "U072"]
