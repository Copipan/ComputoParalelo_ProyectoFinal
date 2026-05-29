"""
ETL: Lee CSVs del ATUS (1997-2024), limpia y guarda en Parquet.
Coloca tus archivos en data/raw/ con nombres como atus_anual_1997.csv
"""

import os
import time
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR    = "/app"
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "parquet")

COLUMNAS = [
    "COBERTURA", "ID_ENTIDAD", "ID_MUNICIPIO", "ANIO", "MES",
    "ID_HORA", "ID_MINUTO", "ID_DIA", "DIASEMANA",
    "URBANA", "SUBURBANA", "TIPACCID",
    "AUTOMOVIL", "CAMPASAJ", "MICROBUS", "PASCAMION", "OMNIBUS",
    "TRANVIA", "CAMIONETA", "CAMION", "TRACTOR", "FERROCARRI",
    "MOTOCICLET", "BICICLETA", "OTROVEHIC",
    "CAUSAACCI", "CAPAROD", "SEXO", "ALIENTO", "CINTURON", "ID_EDAD",
    "CONDMUERTO", "CONDHERIDO", "PASAMUERTO", "PASAHERIDO",
    "PEATMUERTO", "PEATHERIDO", "CICLMUERTO", "CICLHERIDO",
    "OTROMUERTO", "OTROHERIDO", "NEMUERTO", "NEHERIDO",
    "CLASACC", "ESTATUS",
]

COLUMNAS_NUMERICAS = [
    "ID_ENTIDAD", "ID_MUNICIPIO", "ANIO", "MES", "ID_HORA", "ID_MINUTO", "ID_DIA",
    "AUTOMOVIL", "CAMPASAJ", "MICROBUS", "PASCAMION", "OMNIBUS",
    "TRANVIA", "CAMIONETA", "CAMION", "TRACTOR", "FERROCARRI",
    "MOTOCICLET", "BICICLETA", "OTROVEHIC", "ID_EDAD",
    "CONDMUERTO", "CONDHERIDO", "PASAMUERTO", "PASAHERIDO",
    "PEATMUERTO", "PEATHERIDO", "CICLMUERTO", "CICLHERIDO",
    "OTROMUERTO", "OTROHERIDO", "NEMUERTO", "NEHERIDO",
]

ENTIDADES = {
    1: "Aguascalientes", 2: "Baja California", 3: "Baja California Sur",
    4: "Campeche", 5: "Coahuila", 6: "Colima", 7: "Chiapas",
    8: "Chihuahua", 9: "Ciudad de México", 10: "Durango",
    11: "Guanajuato", 12: "Guerrero", 13: "Hidalgo", 14: "Jalisco",
    15: "Estado de México", 16: "Michoacán", 17: "Morelos",
    18: "Nayarit", 19: "Nuevo León", 20: "Oaxaca", 21: "Puebla",
    22: "Querétaro", 23: "Quintana Roo", 24: "San Luis Potosí",
    25: "Sinaloa", 26: "Sonora", 27: "Tabasco", 28: "Tamaulipas",
    29: "Tlaxcala", 30: "Veracruz", 31: "Yucatán", 32: "Zacatecas",
}

CAUSAS = {
    1: "Conductor", 2: "Peatón", 3: "Pasajero", 4: "Ciclista",
    5: "Falla del vehículo", 6: "Mala condición del camino",
    7: "Otra causa",
}

TIPOS_ACCIDENTE = {
    1: "Colisión con vehículo automotor",
    2: "Colisión con peatón",
    3: "Colisión con animal",
    4: "Colisión con objeto fijo",
    5: "Volcadura",
    6: "Caída de pasajero",
    7: "Salida del camino",
    8: "Incendio",
    9: "Colisión con ferrocarril",
    10: "Colisión con ciclista",
    11: "Otro",
}


def _fix_encoding(s: str) -> str:
    """Corrige strings mal decodificados de latin-1 leído como utf-8."""
    import unicodedata
    try:
        # Intenta re-encodear: si vino mal, esto lo restaura
        fixed = s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        fixed = s
    return unicodedata.normalize("NFC", fixed).strip()


def _fix_str_col(series: pd.Series) -> pd.Series:
    """Aplica corrección de encoding a una columna de strings."""
    return series.astype(str).apply(
        lambda x: _fix_encoding(x) if x not in ("nan", "None", "") else pd.NA
    )


def limpiar_csv(path: str) -> pd.DataFrame:
    # latin-1 es el encoding oficial del INEGI — siempre primero
    try:
        df = pd.read_csv(path, encoding="latin-1", low_memory=False)
    except Exception:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)

    df.columns = [c.strip().upper() for c in df.columns]
    cols_presentes = [c for c in COLUMNAS if c in df.columns]
    df = df[cols_presentes]

    for col in COLUMNAS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["ANIO"].notna()]
    df = df[df["ID_ENTIDAD"].between(1, 32, inclusive="both")]

    df["NOM_ENTIDAD"] = df["ID_ENTIDAD"].map(ENTIDADES)

    # Columnas string: corregir encoding y limpiar
    if "CAUSAACCI" in df.columns:
        df["NOM_CAUSA"] = _fix_str_col(df["CAUSAACCI"])
    if "TIPACCID" in df.columns:
        df["NOM_TIPACCID"] = _fix_str_col(df["TIPACCID"])
    if "DIASEMANA" in df.columns:
        df["DIASEMANA"] = _fix_str_col(df["DIASEMANA"])

    muertos_cols = [c for c in ["CONDMUERTO","PASAMUERTO","PEATMUERTO","CICLMUERTO","OTROMUERTO","NEMUERTO"] if c in df.columns]
    heridos_cols = [c for c in ["CONDHERIDO","PASAHERIDO","PEATHERIDO","CICLHERIDO","OTROHERIDO","NEHERIDO"] if c in df.columns]

    df["TOTAL_MUERTOS"] = df[muertos_cols].fillna(0).sum(axis=1).astype(int)
    df["TOTAL_HERIDOS"] = df[heridos_cols].fillna(0).sum(axis=1).astype(int)
    df["GRAVEDAD"]      = df["TOTAL_MUERTOS"] * 3 + df["TOTAL_HERIDOS"]

    return df.reset_index(drop=True)



# Los CSVs solo están montados en el head. El procesamiento distribuido
# ocurre en analisis.py sobre los Parquet del volumen compartido.
def etl_secuencial(raw_dir: str, parquet_dir: str) -> list:
    archivos = sorted(glob.glob(os.path.join(raw_dir, "*.csv")))
    resultados = []
    for path in archivos:
        nombre = os.path.basename(path).replace(".csv", "")
        salida  = os.path.join(parquet_dir, f"{nombre}.parquet")
        df      = limpiar_csv(path)
        table   = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, salida, compression="snappy")
        resultados.append({"archivo": nombre, "registros": len(df)})
    return resultados



if __name__ == "__main__":
    import sys

    raw_dir     = RAW_DIR
    parquet_dir = PARQUET_DIR
    os.makedirs(parquet_dir, exist_ok=True)

    # ETL siempre corre secuencial en el head — los CSVs solo están montados ahí.
    # El procesamiento distribuido ocurre en analisis.py sobre los Parquet compartidos.
    print(f"\n{'='*50}")
    print(f"  ETL ATUS — modo: SECUENCIAL (head)")
    print(f"  raw_dir     : {raw_dir}")
    print(f"  parquet_dir : {parquet_dir}")
    print(f"{'='*50}\n")

    t0 = time.time()
    resultados = etl_secuencial(raw_dir, parquet_dir)
    t1 = time.time()

    total = sum(r["registros"] for r in resultados)
    print(f"\nArchivos procesados : {len(resultados)}")
    print(f"Registros totales   : {total:,}")
    print(f"Tiempo              : {t1 - t0:.2f}s")