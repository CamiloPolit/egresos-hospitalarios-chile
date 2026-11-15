"""Paso 1: tabla de referencia CIE-10 desde el diccionario DEIS (hoja 'codigo CIE-10').

Salida: data/ref/cie10.parquet con una fila por subcategoría (código de 4 caracteres).
"""
import pandas as pd

from config import CIE10_PARQUET, DICCIONARIO_HOJA_CIE10, DICCIONARIO_XLSX

COLUMNAS = {
    "CODIGO SUBCATEGORIA": "cod_sub",
    "GLOSA SUBCATEGORIA": "glosa_sub",
    "CAPITULO": "capitulo",
    "GLOSA CAPITULO": "glosa_capitulo",
    "CODIGO GRUPO": "cod_grupo",
    "GLOSA GRUPO": "glosa_grupo",
    "CODIGO CATEGORIA": "cod_cat",
    "GLOSA CATEGORIA": "glosa_cat",
}


def main() -> None:
    crudo = pd.read_excel(DICCIONARIO_XLSX, sheet_name=DICCIONARIO_HOJA_CIE10,
                          header=None, dtype=str)
    # La fila de encabezado no está al inicio: se busca la que dice CODIGO SUBCATEGORIA
    fila_hdr = crudo.index[crudo[0].str.strip() == "CODIGO SUBCATEGORIA"][0]
    df = crudo.iloc[fila_hdr + 1:].copy()
    df.columns = [str(c).strip() for c in crudo.iloc[fila_hdr]]
    df = df.rename(columns=COLUMNAS)[list(COLUMNAS.values())]

    for c in df.columns:
        df[c] = df[c].astype("string").str.strip()
    df = df[df["cod_sub"].str.fullmatch(r"[A-Z][0-9A-Z]{3}", na=False)]
    df = df.drop_duplicates(subset="cod_sub", keep="first")

    # Capítulo "corto" para gráficos: letra(s) del rango, p.ej. 'J00-J99'
    df["glosa_capitulo"] = df["glosa_capitulo"].str.replace(r"\s+", " ", regex=True)

    # Códigos CIE-10 incorporados por la OMS en 2020-2021 que no están en el diccionario
    # DEIS pero sí aparecen en los egresos 2021.
    u_cap = ("U00-U99", "Códigos para propósitos especiales")
    u_grp = ("U00-U49", "Asignación provisoria de nuevas afecciones de etiología incierta")
    extra = pd.DataFrame([
        ("U099", "CONDICIÓN POST COVID-19, NO ESPECIFICADA", *u_cap, *u_grp, "U09", "Condición post COVID-19"),
        ("U109", "SÍNDROME INFLAMATORIO MULTISISTÉMICO ASOCIADO A COVID-19, NO ESPECIFICADO", *u_cap, *u_grp,
         "U10", "Síndrome inflamatorio multisistémico asociado a COVID-19"),
        ("U129", "VACUNAS CONTRA COVID-19 QUE CAUSAN EFECTOS ADVERSOS, NO ESPECIFICADAS", *u_cap, *u_grp,
         "U12", "Vacunas contra COVID-19 que causan efectos adversos en su uso terapéutico"),
        ("A979", "DENGUE, NO ESPECIFICADO", "A00-B99", "Ciertas enfermedades infecciosas y parasitarias",
         "A92-A99", "Fiebres virales transmitidas por artrópodos y fiebres virales hemorrágicas",
         "A97", "Dengue"),
    ], columns=list(COLUMNAS.values()))
    extra = extra[~extra["cod_sub"].isin(df["cod_sub"])]
    df = pd.concat([df, extra.astype("string")], ignore_index=True)

    assert {"U071", "U072"} <= set(df["cod_sub"]), "Faltan códigos COVID en el diccionario"
    # Glosa legible para la categoría COVID (en el diccionario dice "Uso emergente de U07")
    df.loc[df["cod_cat"] == "U07", "glosa_cat"] = "COVID-19"

    CIE10_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index(drop=True).to_parquet(CIE10_PARQUET, index=False)
    print(f"CIE-10: {len(df)} subcategorías, {df['cod_cat'].nunique()} categorías, "
          f"{df['capitulo'].nunique()} capítulos -> {CIE10_PARQUET}")


if __name__ == "__main__":
    main()
