"""Gráficos del informe a partir de las tablas agregadas (output/tablas -> output/graficos).

Se puede ejecutar solo (`python src/graficos.py`) o importar desde el notebook.
Paleta: categórica fija en orden (azul, naranja, aqua, amarillo, magenta) para años en
gráficos de líneas y sectores; divergente azul<->rojo
con gris neutro en 1,0 para razones observado/esperado; un solo eje Y por gráfico.
"""
import sys
import textwrap

import matplotlib
import matplotlib.ticker

if "ipykernel" not in sys.modules:  # en Jupyter se deja el backend inline
    matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm  # noqa: E402

from config import (ANIOS, GRAFICOS_DIR, ORDEN_EDAD, ORDEN_REGIONES, PERIODO,  # noqa: E402
                    REGIONES, TABLAS_DIR)

# --- Paleta (instancia de referencia validada) ---
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
# Slots 1-5 en orden fijo. Los 3 primeros son válidos en todos los pares (CVD); los 5 son
# válidos para pares adyacentes (líneas), por eso los años sólo se colorean en líneas.
SERIE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
ANIOS_STR = [str(a) for a in ANIOS]
COLOR_ANIO = {a: SERIE[i % len(SERIE)] for i, a in enumerate(ANIOS_STR)}
COLOR_SECTOR = {"SNSS": SERIE[0], "NO_SNSS": SERIE[1]}
NOMBRE_SECTOR = {"SNSS": "SNSS (público)", "NO_SNSS": "No SNSS (privado y otros)"}
DIVERGENTE = LinearSegmentedColormap.from_list(
    "azul_rojo", ["#184f95", "#6da7ec", "#f0efec", "#ec8a8a", "#a32626"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK_2, "xtick.color": MUTED, "ytick.color": INK_2,
    "axes.edgecolor": AXIS, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold",
    "axes.titlesize": 12, "axes.titlelocation": "left", "legend.frameon": False,
})


def _t(nombre: str) -> pd.DataFrame:
    return pd.read_csv(TABLAS_DIR / nombre, dtype={"anio": str, "region_cod": str})


def _guardar(fig, nombre: str):
    GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)
    ruta = GRAFICOS_DIR / nombre
    fig.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  -> {ruta.name}")
    return fig


def _corto(s: str, n: int = 45) -> str:
    return textwrap.shorten(str(s), n, placeholder="…")


def _miles(x, _=None):
    return f"{x:,.0f}".replace(",", ".")


# ---------------------------------------------------------------- Objetivo 1
def top_dias_cama(n: int = 15):
    d = _t("top_dias_cama_categoria.csv")
    d = d[d.anio == PERIODO].nsmallest(n, "rank").iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6.5))
    y = np.arange(len(d))
    ax.barh(y, d.dias_cama, color=SERIE[0], height=0.7, label="Días-cama totales")
    ax.barh(y, d.dias_cama_sin_extremas, color="#9ec5f4", height=0.7,
            label="Sin estadías > 365 días")
    ax.set_yticks(y, [f"{c} {_corto(g, 40)}" for c, g in zip(d.cod_cat, d.glosa_cat)])
    ax.xaxis.set_major_formatter(_miles)
    ax.grid(axis="y", visible=False)
    for yi, v, e in zip(y, d.dias_cama, d.estadia_media):
        ax.text(v, yi, f"  {e:.1f} d/egreso", va="center", fontsize=8, color=INK_2)
    ax.set_title(f"Top {n} diagnósticos por días-cama, {PERIODO}")
    ax.set_xlabel("Días-cama (suma de días de estada de los egresos)")
    ax.legend(loc="lower right")
    return _guardar(fig, "01_top_dias_cama.png")


def _heatmap_variacion(d: pd.DataFrame, valor: str, titulo: str, nombre: str, lim: float = 50):
    """Heatmap capítulo x año de una variación % vs 2019 (divergente, gris = sin cambio)."""
    m = d.pivot(index="capitulo", columns="anio", values=valor)
    m = m[[a for a in ANIOS_STR if a in m.columns and a != ANIOS_STR[0]]]
    m = m.loc[m.iloc[:, 0].sort_values().index]
    glosa = d.drop_duplicates("capitulo").set_index("capitulo").glosa_capitulo
    fig, ax = plt.subplots(figsize=(8.5, 0.36 * len(m) + 1.8))
    im = ax.imshow(m.values.clip(-lim, lim), cmap=DIVERGENTE, norm=TwoSlopeNorm(0, -lim, lim),
                   aspect="auto")
    ax.set_xticks(range(m.shape[1]), m.columns)
    ax.xaxis.tick_top()
    ax.set_yticks(range(m.shape[0]), [f"{c} {_corto(glosa[c], 42)}" for c in m.index], fontsize=8.5)
    ax.grid(False)
    for (r, c), v in np.ndenumerate(m.values):
        if not np.isnan(v):
            ax.text(c, r, f"{v:+.0f}%", ha="center", va="center", fontsize=8,
                    color="#ffffff" if abs(v) > lim * 0.6 else INK)
    cb = fig.colorbar(im, ax=ax, shrink=0.5, pad=0.02)
    cb.outline.set_visible(False)
    cb.set_label("Variación vs 2019 (%)", color=INK_2)
    ax.set_title(titulo, pad=28)
    fig.tight_layout()
    return _guardar(fig, nombre)


def dias_cama_capitulo_anio():
    d = _t("dias_cama_capitulo.csv")
    d = d[d.anio.isin(ANIOS_STR) & (d.capitulo != "U00-U99")].copy()
    base = d[d.anio == ANIOS_STR[0]].set_index("capitulo").dias_cama
    d["var"] = 100 * (d.dias_cama / d.capitulo.map(base) - 1)
    return _heatmap_variacion(
        d, "var", "Días-cama por capítulo CIE-10 (sin COVID): variación respecto de 2019\n(azul = menos que en 2019, rojo = más)",
        "02_dias_cama_capitulo_anio.png")


# ---------------------------------------------------------------- Objetivo 2
def forest_region(anio: str = PERIODO):
    d = _t("region_todas_causas_ajustada.csv")
    d = d[d.anio == anio].set_index("region_cod").reindex(ORDEN_REGIONES[:-1][::-1]).reset_index()
    fig, axes = plt.subplots(1, 2, figsize=(11, 6), sharey=True)
    y = np.arange(len(d))
    ax = axes[0]
    ax.hlines(y, d.razon_letalidad_ic_inf, d.razon_letalidad_ic_sup, color=MUTED, lw=2)
    ax.scatter(d.razon_letalidad, y, s=55, color=SERIE[0], zorder=3, edgecolor=SURFACE, lw=1.5)
    ax.axvline(1, color=INK_2, lw=1, ls="--")
    ax.set_yticks(y, d.region_nombre)
    ax.set_title("Mortalidad: observada / esperada")
    ax.set_xlabel("Razón (IC 95%). >1 = más muertes que lo esperado")
    ax = axes[1]
    ax.scatter(d.razon_estadia, y, s=55, color=SERIE[1], zorder=3, edgecolor=SURFACE, lw=1.5)
    ax.axvline(1, color=INK_2, lw=1, ls="--")
    ax.set_title("Estadía: observada / esperada")
    ax.set_xlabel("Razón. >1 = estadía más larga que lo esperado")
    for a in axes:
        a.grid(axis="y", visible=False)
    fig.suptitle(f"Regiones de residencia (norte → sur), {anio}. Ajustado por diagnóstico, "
                 "edad y sexo", x=0.01, ha="left", fontsize=11, color=INK_2)
    fig.tight_layout()
    return _guardar(fig, "03_forest_regiones.png")


def heatmap_region_diag(metrica: str = "razon_letalidad", anio: str = PERIODO, n: int = 20):
    d = _t("region_diagnostico_ajustada.csv")
    top = _t("top_diagnosticos_comparacion.csv").head(n)
    d = d[(d.anio == anio) & d.cod_cat.isin(top.cod_cat)]
    if metrica == "razon_letalidad":
        # Diagnósticos con letalidad casi nula (partos, colelitiasis) dan razones inestables
        d = d[d.fallecidos_esperados >= 5]
    m = d.pivot(index="cod_cat", columns="region_cod", values=metrica)
    m = m.reindex(index=[c for c in top.cod_cat if c in m.index],
                  columns=[c for c in ORDEN_REGIONES if c in m.columns])
    # Sólo diagnósticos con dato en al menos la mitad de las regiones
    m = m[m.notna().sum(axis=1) >= m.shape[1] / 2]
    glosa = top.set_index("cod_cat").glosa_cat
    vals = np.log2(m.astype(float))
    lim = np.nanmax(np.abs(vals.values.clip(-1.5, 1.5)))
    fig, ax = plt.subplots(figsize=(12, 0.42 * len(m) + 2))
    im = ax.imshow(vals, cmap=DIVERGENTE, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
    ax.set_xticks(range(m.shape[1]), [REGIONES[c] for c in m.columns], rotation=45, ha="right")
    ax.set_yticks(range(m.shape[0]), [f"{c} {_corto(glosa[c], 36)}" for c in m.index])
    ax.grid(False)
    for (i, j), v in np.ndenumerate(m.values):
        if not np.isnan(v):
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=7,
                    color="#ffffff" if abs(np.log2(v)) > lim * 0.6 else INK)
    cb = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.01)
    ticks = [t for t in (-1.5, -1, -0.5, 0, 0.5, 1, 1.5) if abs(t) <= lim]
    cb.set_ticks(ticks, labels=[f"{2**t:.2f}" for t in ticks])
    cb.outline.set_visible(False)
    nombre = "mortalidad" if metrica == "razon_letalidad" else "estadía"
    ax.set_title(f"Razón observado/esperado de {nombre} por diagnóstico y región, {anio}\n"
                 "Ajustado por edad y sexo. Rojo = mayor que lo esperado, azul = menor; "
                 "celda vacía = < 30 egresos" + (" o < 5 muertes esperadas" if metrica == "razon_letalidad" else ""),
                 fontsize=11)
    fig.tight_layout()
    return _guardar(fig, f"04_heatmap_{metrica}.png")


def sector_diag(n: int = 20):
    d = _t("sector_diagnostico_ajustada.csv")
    top = _t("top_diagnosticos_comparacion.csv").head(n)
    d = d[(d.anio == PERIODO) & d.cod_cat.isin(top.cod_cat)]
    orden = [c for c in top.cod_cat][::-1]
    glosa = top.set_index("cod_cat").glosa_cat
    fig, axes = plt.subplots(1, 2, figsize=(12, 7), sharey=True)
    y = np.arange(len(orden))
    for sec, col in COLOR_SECTOR.items():
        s = d[d.sector == sec].set_index("cod_cat").reindex(orden)
        axes[0].scatter(s.estadia_media, y, color=col, s=45, label=NOMBRE_SECTOR[sec],
                        zorder=3, edgecolor=SURFACE, lw=1.5)
        axes[1].scatter(s.letalidad_pct, y, color=col, s=45, zorder=3, edgecolor=SURFACE, lw=1.5)
    for ax in axes:
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(y, [f"{c} {_corto(glosa[c], 36)}" for c in orden])
    axes[0].set_xscale("log")
    axes[0].set_xticks([2, 5, 10, 20, 50, 100], ["2", "5", "10", "20", "50", "100"])
    axes[0].xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    axes[0].set_xlabel("Estadía media en días (escala log, sin estadías > 365)")
    axes[0].set_title("Estadía media")
    axes[1].set_xlabel("Letalidad hospitalaria cruda (%)")
    axes[1].set_title("Letalidad")
    axes[0].legend(loc="lower left", bbox_to_anchor=(0, 1.04), ncol=2)
    fig.suptitle(f"Mismo diagnóstico, distinto sector, {PERIODO}", x=0.01, ha="left",
                 y=1.04, fontsize=11, color=INK_2)
    fig.tight_layout()
    return _guardar(fig, "05_sector_diagnostico.png")


# ---------------------------------------------------------------- Objetivo 3
def covid_indice():
    d = _t("covid_totales_anio.csv")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, alc in zip(axes, ["Todas las causas", "Sin COVID"]):
        s = d[d.alcance == alc].sort_values("anio")
        x = s.anio.astype(int)
        finales = []
        for (m, lab), col in zip([("egresos", "Egresos"), ("dias_cama", "Días-cama"),
                                  ("fallecidos", "Fallecidos")], SERIE):
            idx = 100 + s[f"{m}_var_pct_vs_2019"]
            ax.plot(x, idx, color=col, lw=2, marker="o", ms=7, mec=SURFACE, mew=1.5, label=lab)
            finales.append(idx.iloc[-1])
        # Etiquetas finales separadas al menos 3 puntos para que no se superpongan
        pos = sorted(range(len(finales)), key=lambda i: finales[i])
        y_lab = {}
        for k, i in enumerate(pos):
            y_lab[i] = finales[i] if k == 0 else max(finales[i], y_lab[pos[k - 1]] + 3)
        for i, v in enumerate(finales):
            ax.text(x.iloc[-1] + 0.06, y_lab[i], f"{v:.0f}", va="center", fontsize=9, color=INK_2)
        ax.axhline(100, color=AXIS, lw=1)
        ax.set_xticks([int(a) for a in ANIOS_STR])
        ax.set_xlim(int(ANIOS_STR[0]) - 0.2, int(ANIOS_STR[-1]) + 0.35)
        ax.set_title(alc)
    axes[0].set_ylabel("Índice (2019 = 100)")
    axes[0].legend(loc="upper left")
    fig.suptitle("Efecto de la pandemia sobre la actividad hospitalaria", x=0.01, ha="left",
                 y=1.02, fontsize=11, color=INK_2)
    fig.tight_layout()
    return _guardar(fig, "06_covid_indice.png")


def covid_capitulos():
    d = _t("covid_capitulo_variacion.csv")
    d = d[d.capitulo != "U00-U99"]
    return _heatmap_variacion(
        d, "egresos_var_pct_vs_2019",
        "Egresos sin COVID por capítulo: variación respecto de 2019\n(azul = menos que en 2019, rojo = más)",
        "07_covid_capitulos.png")


def covid_edad():
    d = _t("covid_edad_sexo.csv")
    g = (d.groupby(["anio", "edad_grupo"])[["egresos", "fallecidos"]].sum().reset_index())
    g["letalidad_pct"] = 100 * g.fallecidos / g.egresos
    g["pct_egresos"] = 100 * g.egresos / g.groupby("anio").egresos.transform("sum")
    anios = [a for a in ANIOS_STR if a in set(g.anio)]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    x = np.arange(len(ORDEN_EDAD))
    for a in anios:
        s = g[g.anio == a].set_index("edad_grupo").reindex(ORDEN_EDAD)
        for ax, col in zip(axes, ["pct_egresos", "letalidad_pct"]):
            ax.plot(x, s[col], color=COLOR_ANIO[a], lw=2, marker="o", ms=6,
                    mec=SURFACE, mew=1.5, label=a)
    for ax in axes:
        ax.set_xticks(x, ORDEN_EDAD)
        ax.set_xlabel("Grupo de edad")
    axes[0].set_ylabel("% de los egresos COVID del año")
    axes[0].set_title("¿Quiénes se hospitalizan por COVID?")
    axes[1].set_ylabel("Letalidad hospitalaria (%)")
    axes[1].set_title("Letalidad por COVID según edad")
    axes[1].legend(title="Año")
    fig.tight_layout()
    return _guardar(fig, "08_covid_letalidad_edad.png")


def covid_peso_anual():
    d = _t("covid_peso_anio.csv")
    d = d[d.grupo_causa.str.startswith("COVID")].set_index("anio").reindex(ANIOS_STR).fillna(0)
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))
    paneles = [("egresos", "Egresos por COVID", _miles),
               ("pct_dias_cama", "% de los días-cama del año", lambda v: f"{v:.1f}%"),
               ("pct_fallecidos", "% de las muertes hospitalarias", lambda v: f"{v:.1f}%")]
    for ax, (col, tit, fmt) in zip(axes, paneles):
        ax.bar(d.index, d[col], color=SERIE[0], width=0.6)
        for xi, v in enumerate(d[col]):
            ax.text(xi, v, fmt(v), ha="center", va="bottom", fontsize=8.5, color=INK_2)
        ax.set_title(tit, fontsize=11)
        ax.grid(axis="x", visible=False)
        ax.yaxis.set_major_formatter(_miles if col == "egresos" else (lambda v, _: f"{v:.0f}%"))
        ax.margins(y=0.15)
    fig.suptitle("Peso del COVID-19 (U07.1/U07.2) en la actividad hospitalaria", x=0.01,
                 ha="left", y=1.03, fontsize=11, color=INK_2)
    fig.tight_layout()
    return _guardar(fig, "09_covid_peso_anual.png")


TODOS = [top_dias_cama, dias_cama_capitulo_anio, forest_region,
         lambda: heatmap_region_diag("razon_letalidad"),
         lambda: heatmap_region_diag("razon_estadia"),
         sector_diag, covid_indice, covid_capitulos, covid_edad, covid_peso_anual]


def main() -> None:
    for f in TODOS:
        plt.close(f())


if __name__ == "__main__":
    main()
