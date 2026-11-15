"""Genera notebooks/informe.ipynb (texto + celdas que leen output/tablas) y lo ejecuta,
exportando también a HTML.  Uso: .venv/bin/python src/build_informe.py
"""
import subprocess
import sys
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent
NB = ROOT / "notebooks" / "informe.ipynb"

C = []


def md(s):
    C.append(nbf.v4.new_markdown_cell(s))


def py(s):
    C.append(nbf.v4.new_code_cell(s))


md("""# Estadía hospitalaria y causas de hospitalización en Chile, 2019-2023

**Fuente:** bases de egresos hospitalarios DEIS-Minsal (datos abiertos), 2019 a 2023: **7.676.104 egresos**.
**Procesamiento:** Apache Spark (PySpark 3.5, modo local). El notebook sólo lee las tablas agregadas que produce el pipeline (`src/run_all.py`).

**Objetivos**
1. Diagnósticos que generan más días-cama.
2. Estadía y mortalidad hospitalaria del mismo diagnóstico entre regiones de residencia y entre sectores (SNSS vs No SNSS), ajustadas por edad y sexo.
3. Efecto del COVID-19: pre-pandemia (2019), pandemia (2020-2021) y post-pandemia (2022-2023).

> **Alcance:** los archivos abiertos no traen el establecimiento ni la región del establecimiento, por lo que la comparación entre hospitales y los flujos interregionales quedan fuera (ver *Limitaciones*). El eje "establecimiento" se reemplaza por el sector: **SNSS** (Sistema Nacional de Servicios de Salud, red pública) vs **No SNSS** (clínicas privadas, hospitales de FF.AA. y Carabineros, mutuales, etc.).""")

py("""import sys
from pathlib import Path
import pandas as pd
from IPython.display import display

SRC = Path.cwd().parent / "src" if Path.cwd().name == "notebooks" else Path.cwd() / "src"
sys.path.insert(0, str(SRC))
from config import TABLAS_DIR, CALIDAD_DIR, PERIODO
import graficos as G

pd.set_option("display.max_colwidth", 60)
pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
t = lambda n: pd.read_csv(TABLAS_DIR / n, dtype={"anio": str, "region_cod": str})""")

md("""## 0. Datos y calidad

| Año | Egresos | Fallecidos | Días de estada | Anonimizados |
|---|---|---|---|---|
| 2019 | 1.667.180 | 36.389 | 9.982.117 | 43.845 (2,6%) |
| 2020 | 1.330.477 | 42.961 | 8.694.958 | 37.542 (2,8%) |
| 2021 | 1.467.062 | 48.068 | 9.624.299 | 0 |
| 2022 | 1.597.118 | 42.652 | 9.818.737 | 39.569 (2,5%) |
| 2023 | 1.612.267 | 37.735 | 10.032.975 | 128.108 (7,9%) |

El pipeline verifica que estos totales coinciden exactamente con los CSV originales.

Armonización entre años (`src/harmonize.py`, `src/config.py`):
- **Formatos:** 2019, 2020, 2022 y 2023 tienen el mismo formato; 2021 trae la edad en quinquenios + tramos neonatales y el sexo codificado (1 = hombre, 2 = mujer). Grupo de edad común: `<1, 1-9, …, 70-79, 80+`.
- **2022** trae descripciones de procedimientos entre comillas con `;` internos; se leen como CSV con comillas escapadas.
- **Filas anonimizadas** (`*`): no tienen edad, sexo ni sector. En 2019, 2020 y 2022 sí traen comuna, y la **región se deriva de los 2 primeros dígitos de la comuna** (coincide en el 100% de las filas normales). En **2023 tampoco traen comuna**, así que ~128 mil egresos (8%) quedan sin región. Su mezcla de diagnósticos es parecida a la del total, salvo por más casos perinatales (9% vs 2%).
- **Estadías > 365 días** (~500 por año, máximo 61 años): se conservan, pero también se reportan métricas sin ellas, porque los días se imputan completos al año del alta.""")
py("""display(pd.read_csv(CALIDAD_DIR / "totales_por_anio.csv"))
display(pd.read_csv(CALIDAD_DIR / "percentiles_dias_estada.csv"))""")

md("## 1. ¿Qué diagnósticos generan más días-cama?")
py("""G.top_dias_cama();""")
py("""d = t("top_dias_cama_categoria.csv")
cols = ["anio","rank","cod_cat","glosa_cat","egresos","dias_cama","dias_cama_sin_extremas",
        "estadia_media","estadia_mediana","letalidad_pct","pct_dias_cama_acum"]
for a in sorted(set(d.anio)):
    print(a); display(d[d.anio == a][cols].head(10))""")
md("""**Lectura:**
- En el periodo 2019-2023, el **COVID-19 (U07)** es la primera causa de días-cama (2,4 millones, 5% del total), concentrados en 2020-2021. En 2023 ya no está en el top 10.
- La **esquizofrenia (F20)** es segunda, pero el 60% de sus días-cama corresponde a estadías de más de un año (barra clara = sin ellas). Refleja pacientes psiquiátricos de larga estadía, no hospitalizaciones agudas.
- **Por volumen:** partos (O80, O82) y colelitiasis (K80) tienen estadías de 2-3 días pero cientos de miles de egresos.
- **Por estadía larga:** neumonía (J18), fractura de fémur (S72), insuficiencia respiratoria (J96), insuficiencia cardíaca (I50) y septicemia (A41).
- Los 10 primeros diagnósticos explican sólo entre el 17% y el 27% de los días-cama de cada año: la carga está muy repartida.""")
py("""G.dias_cama_capitulo_anio();""")

md("""## 2. Mismo diagnóstico, distinta región o sector

**Método (estandarización indirecta):** para cada año y estrato *diagnóstico (categoría CIE-10) × grupo de edad × sexo* se calcula la letalidad y la estadía media nacional. Lo **esperado** para una región o sector es lo que tendría con su mezcla de pacientes si se comportara como el promedio nacional.
**Razón = observado / esperado:** valores > 1 indican más muertes (o estadía más larga) que lo esperado. El IC 95% de la mortalidad usa la aproximación de Byar. Se excluyen las celdas con < 30 egresos.

*Región = región de residencia del paciente, no del hospital.*""")
py("""G.forest_region();""")
py("""d = t("region_todas_causas_ajustada.csv")
display(d[d.anio==PERIODO][["region_nombre","egresos","fallecidos","fallecidos_esperados",
      "razon_letalidad","razon_letalidad_ic_inf","razon_letalidad_ic_sup","significativo_letalidad",
      "estadia_media","estadia_esperada","razon_estadia"]].sort_values("razon_letalidad", ascending=False))
print("Razón de mortalidad por año (estabilidad del ranking):")
display(d.pivot(index="region_nombre", columns="anio", values="razon_letalidad"))""")
md("""**Lectura:**
- A igual diagnóstico, edad y sexo, los residentes de **Tarapacá (+28%), Antofagasta (+18%) y Atacama (+18%)** mueren en el hospital más que lo esperado.
- **Aysén (−33%), Los Ríos (−18%) y Biobío/Los Lagos (−10%)** quedan bajo lo esperado.
- El patrón se repite **en cada uno de los 5 años**, por lo que no es ruido.
- Las diferencias de estadía son menores (±10%).

Esto no permite atribuir las diferencias a la calidad de los hospitales: también influyen la gravedad al ingreso (no observada), la oferta de camas y UCI, los traslados y las muertes fuera del hospital. Además, se compara por región de residencia y no de atención.""")
py("""G.heatmap_region_diag("razon_letalidad");""")
py("""G.heatmap_region_diag("razon_estadia");""")

md("### SNSS vs No SNSS")
py("""G.sector_diag();
display(t("sector_todas_causas_ajustada.csv")[["anio","sector","egresos","letalidad_pct",
        "fallecidos_esperados","razon_letalidad","razon_letalidad_ic_inf","razon_letalidad_ic_sup",
        "estadia_media","razon_estadia"]])""")
md("""**Lectura:** incluso ajustando por diagnóstico, edad y sexo, el SNSS tiene una mortalidad **~10% sobre lo esperado** y el sector No SNSS **~33% bajo lo esperado**, con estadías ~8% más largas en el SNSS y ~19% más cortas en el No SNSS. Las cifras son muy estables entre 2019 y 2023.

La explicación más probable es la **selección de pacientes**: el sistema público recibe casos más graves, con más comorbilidad, de menor nivel socioeconómico y derivados de otros centros. No debe leerse como una diferencia de calidad.""")

md("## 3. Efecto del COVID-19: antes, durante y después")
py("""G.covid_peso_anual();
display(t("covid_peso_anio.csv")[["anio","grupo_causa","egresos","pct_egresos","pct_dias_cama",
        "pct_fallecidos","estadia_media","letalidad_pct"]])""")
md("""**Lectura:**
- **2021 fue el peor año de la pandemia en los hospitales:** 89 mil egresos por COVID, 13% de los días-cama y 1 de cada 4 muertes hospitalarias.
- En **2022** el COVID cae a 25 mil egresos (10% de las muertes) y en **2023** a 5 mil (1,6% de las muertes).""")
py("""G.covid_indice();
display(t("covid_totales_anio.csv")[["alcance","anio","egresos","dias_cama","fallecidos","letalidad_pct",
        "egresos_var_pct_vs_2019","dias_cama_var_pct_vs_2019","fallecidos_var_pct_vs_2019"]])""")
md("""**Lectura:**
- Los egresos por otras causas cayeron 23% en 2020 y recién en **2023 están cerca de 2019 (−4%)**. Los días-cama sin COVID volvieron exactamente al nivel de 2019.
- Las muertes hospitalarias subieron 18% (2020), 32% (2021) y 17% (2022). En 2023 quedan sólo 4% sobre 2019.
- La letalidad general (2,3% en 2023) sigue algo sobre 2019 (2,2%).""")
py("""G.covid_edad();""")
md("""**Lectura (por qué cambió la letalidad del COVID):**
- **2021 vs 2020:** se hospitalizan más adultos jóvenes (20-49 años: de 24% a 34%) y menos mayores. Dentro de cada edad la letalidad es casi igual, así que la baja de la letalidad total (16,8% → 13,9%) se debe al **cambio en quiénes se hospitalizan**, coherente con la vacunación prioritaria de mayores desde febrero de 2021.
- **2022-2023:** el COVID hospitalizado pasa a ser una enfermedad de **mayores de 70 (50%) y de lactantes (<1 año: 11% en 2023)**. Aun así, la letalidad *dentro* de cada edad cae fuerte: en mayores de 80 baja de 43% (2021) a 31% (2022) y 18% (2023), en línea con la inmunidad acumulada y variantes menos graves (Ómicron). La letalidad total de 2022 (18%) es alta sólo porque los pacientes son mucho más viejos.""")
py("""G.covid_capitulos();""")
py("""d = t("covid_recuperacion_categoria.csv")
x = d[d.anio==str(max(int(a) for a in d.anio))].sort_values("egresos_var_pct_vs_2019")
print("Categorías más bajo 2019 en 2023:"); display(x[["cod_cat","glosa_cat","egresos","egresos_var_pct_vs_2019"]].head(12))
print("Categorías más sobre 2019 en 2023:"); display(x[["cod_cat","glosa_cat","egresos","egresos_var_pct_vs_2019"]].tail(12))""")
md("""**Lectura:**
- **2020-2021 (pandemia):**
  - Se desplomaron las enfermedades respiratorias no COVID (−50%; casi no circularon otros virus).
  - Cayeron las cirugías electivas: osteomuscular −36%, ojo −34%, oído −49%.
  - Los tumores bajaron 23%, lo que sugiere diagnósticos y tratamientos postergados.
- **2022-2023 (post-pandemia):**
  - Lo respiratorio vuelve a niveles de 2019, con un **rebote de neumonías e insuficiencia respiratoria en 2023** (J96 +117%, estado asmático +70%), coherente con la intensa temporada de virus respiratorios de 2023.
  - Tumores (+4%) y enfermedades circulatorias (+7%) superan 2019.
  - Crecen fuerte la **obesidad (E66: +164%**, probablemente por cirugía bariátrica) y los **trastornos mentales (+22%**; trastornos de personalidad +57%, reacción al estrés +57%).
- **Lo que no es efecto COVID:** embarazo, parto y perinatal siguen 13-17% bajo 2019 en 2023, lo que coincide con la **caída sostenida de la natalidad** en Chile.""")
py("""display(t("covid_region_anio.csv")[["anio","region_nombre","egresos","dias_cama","estadia_media","letalidad_pct"]])
d = t("covid_estadia_mismo_diag.csv")
display(d[d.anio=="2020"].sort_values("letalidad_pct_var_pct_vs_2019", ascending=False)
        [["cod_cat","glosa_cat","sector","egresos","estadia_media","estadia_media_var_pct_vs_2019",
          "letalidad_pct","letalidad_pct_var_pct_vs_2019"]].head(15))""")

md("""## Conclusiones

1. **Carga de días-cama:** el COVID fue la principal causa del periodo, pero transitoria (2020-2022). En régimen normal dominan la psiquiatría de larga estadía, las infecciones respiratorias, la colelitiasis, los partos, la fractura de fémur y la insuficiencia cardíaca.
2. **Desigualdad territorial persistente:** con igual diagnóstico, edad y sexo, el norte (Tarapacá, Antofagasta, Atacama) tiene una mortalidad hospitalaria 18-28% mayor que lo esperado todos los años, y el sur austral (Aysén, Los Ríos) una menor.
3. **Brecha público-privado estable** (~1,10 vs ~0,67 en la razón de mortalidad ajustada), explicada probablemente por la gravedad y el perfil social de los pacientes.
4. **Pandemia:**
   - Redujo la actividad no COVID hasta en un 23% y aumentó las muertes hospitalarias hasta en un 32% (2021).
   - La actividad se recuperó recién en 2023.
   - Quedan cambios estructurales: más salud mental, más obesidad, menos partos.

## Limitaciones y trabajo futuro

1. **Sin establecimiento ni región del establecimiento:** no se pueden comparar hospitales ni medir flujos interregionales. Para hacerlo se necesita la base DEIS con código de establecimiento (solicitud por Ley de Transparencia).
2. **Región de residencia, no de atención:** las diferencias regionales mezclan la red local con derivaciones a otras regiones. En 2023, el 8% de los egresos no tiene región.
3. **Datos por egreso, no por paciente:** hay reingresos, y los días se imputan completos al año del alta.
4. **Ajuste limitado:** sólo diagnóstico principal, edad (decenios) y sexo; no hay gravedad, comorbilidad ni nivel socioeconómico.
5. **Extensión:** calcular tasas por 100.000 habitantes con proyecciones INE (separaría el efecto de la natalidad y el envejecimiento) y agregar 2024 cuando se publique.""")


def main() -> int:
    nb = nbf.v4.new_notebook()
    nb["cells"] = C
    nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    NB.parent.mkdir(exist_ok=True)
    nbf.write(nb, NB)
    jup = str(Path(sys.executable).parent / "jupyter")
    for args in (["--to", "notebook", "--execute", "--inplace"], ["--to", "html"]):
        r = subprocess.run([jup, "nbconvert", *args, str(NB)], cwd=NB.parent)
        if r.returncode:
            return r.returncode
    print(f"Informe: {NB} y {NB.with_suffix('.html')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
