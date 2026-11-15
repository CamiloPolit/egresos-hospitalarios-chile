# Estadía hospitalaria y causas de hospitalización en Chile (2019-2023)

Análisis con Apache Spark (PySpark) de los egresos hospitalarios DEIS-Minsal: días-cama por
diagnóstico (7,68 M egresos), estadía y mortalidad por región de residencia y sector (SNSS / No SNSS) ajustadas
por edad y sexo, y efecto del COVID-19.

## Requisitos
- Java 17 (`/opt/homebrew/opt/openjdk@17`; se configura solo si `JAVA_HOME` no está definido)
- Python 3.9+ con el entorno `.venv`:
  ```bash
  python3 -m venv .venv
  .venv/bin/pip install -r requirements.txt
  ```

## Datos de entrada
Los CSV no están en el repositorio (~1,4 GB). Se descargan desde la sección de datos abiertos
del DEIS-Minsal (<https://deis.minsal.cl>, *Egresos hospitalarios*) y se dejan en la raíz del
proyecto, **una carpeta por año** con el nombre `EGRESOS_<año>`:

```
Proyecto PATOS/
├── EGRESOS_2019/
│   ├── EGRE_DATOS_ABIERTOS_2019.csv
│   └── Diccionario BD egresos hospitalario.xlsx
├── EGRESOS_2020/
│   └── EGRE_DATOS_ABIERTOS_2020.csv
├── EGRESOS_2021/
│   └── EGR_DATOS_ABIERTO_2021.csv
├── EGRESOS_2022/
│   └── EGRE_DATOS_ABIERTOS_2022.csv
├── EGRESOS_2023/
│   └── EGRESOS_2023.csv
├── src/
└── ...
```

- **Nombres exactos:** los nombres de las carpetas y de los archivos deben coincidir con los de
  arriba, porque son los que define `ANIOS` en `src/config.py`. Los CSV mantienen el nombre con
  que los entrega el DEIS (no es el mismo patrón todos los años). Si un archivo se llama
  distinto, renómbralo o cambia su ruta en `config.py`.
- **Sin modificar:** los CSV se usan tal como se descargan. Son texto **ISO-8859-1 (latin-1)**,
  separados por **`;`**, con fila de encabezado y fin de línea CRLF. Abrirlos y guardarlos con
  Excel puede cambiar la codificación o el separador y romper la lectura.
- **Diccionario:** sólo se necesita el de `EGRESOS_2019/` (de ahí se lee la hoja `codigo CIE-10`);
  el DEIS incluye el mismo archivo en cada descarga, así que se pueden dejar las copias de los
  demás años.
- **Validación:** el paso `03_calidad.py` compara filas, fallecidos y días de estada de cada año
  con `TOTALES_ESPERADOS` en `src/config.py`; si el archivo descargado no es el mismo, el
  pipeline se detiene con el detalle de la diferencia.

## Ejecución
```bash
.venv/bin/python src/run_all.py          # pipeline completo + informe (~4 min)
.venv/bin/jupyter lab notebooks/informe.ipynb
```

| Paso | Script | Salida |
|---|---|---|
| 1 | `src/01_cie10_lookup.py` | `data/ref/cie10.parquet` (jerarquía CIE-10 del diccionario DEIS) |
| 2 | `src/02_ingesta.py` | `data/clean/egresos.parquet` (años armonizados, partición por año) |
| 3 | `src/03_calidad.py` | `output/calidad/*.csv` + controles contra los totales de los CSV |
| 4 | `src/04_dias_cama.py` | `output/tablas/*dias_cama*.csv` (objetivo 1) |
| 5 | `src/05_comparacion_regional.py` | `output/tablas/region_*`, `sector_*` (objetivo 2) |
| 6 | `src/06_covid.py` | `output/tablas/covid_*` (objetivo 3) |
| 7 | `src/graficos.py` | `output/graficos/*.png` |
| 8 | `src/build_informe.py` | `notebooks/informe.ipynb` y `.html` ejecutados |

Configuración (rutas, años, mapeo de columnas, umbrales) en `src/config.py`. Para agregar un
año nuevo, añadir su entrada en `ANIOS` (y en `TOTALES_ESPERADOS` para el control de calidad).

## Limitación principal
Los archivos abiertos no traen establecimiento ni región del establecimiento, así que no se
pueden comparar hospitales ni medir flujos interregionales; el análisis usa la región de
**residencia** y el sector SNSS / No SNSS. En 2023 las filas anonimizadas (8%) no traen
comuna, por lo que quedan sin región.
