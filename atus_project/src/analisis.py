"""
Análisis distribuido del ATUS con Ray.
Lee los Parquet generados por etl.py y calcula todos los indicadores.
"""

import os
import time
import glob
import json
import pandas as pd
import pyarrow.parquet as pq
import ray


# ── Helpers ─────────────────────────────────────────────────────────────────

def leer_parquet_todos(parquet_dir: str) -> pd.DataFrame:
    """Lee todos los Parquet y los consolida."""
    archivos = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not archivos:
        raise FileNotFoundError(f"No hay Parquet en {parquet_dir}. Ejecuta etl.py primero.")
    return pd.concat([pq.read_table(f).to_pandas() for f in archivos], ignore_index=True)


# ── Tareas Ray ───────────────────────────────────────────────────────────────

@ray.remote
def analisis_por_entidad(df) -> dict:
    resultado = (
        df.groupby(["ID_ENTIDAD", "NOM_ENTIDAD"])
        .agg(
            total_accidentes=("ANIO", "count"),
            total_muertos=("TOTAL_MUERTOS", "sum"),
            total_heridos=("TOTAL_HERIDOS", "sum"),
            gravedad_total=("GRAVEDAD", "sum"),
        )
        .reset_index()
        .sort_values("total_accidentes", ascending=False)
    )
    return resultado.to_dict(orient="records")


@ray.remote
def analisis_por_municipio(df, top_n: int = 50) -> dict:
    resultado = (
        df.groupby(["ID_ENTIDAD", "NOM_ENTIDAD", "ID_MUNICIPIO"])
        .agg(
            total_accidentes=("ANIO", "count"),
            total_muertos=("TOTAL_MUERTOS", "sum"),
            total_heridos=("TOTAL_HERIDOS", "sum"),
        )
        .reset_index()
        .sort_values("total_accidentes", ascending=False)
        .head(top_n)
    )
    return resultado.to_dict(orient="records")


@ray.remote
def analisis_temporal(df) -> dict:
    import pandas as pd

    # Forzar numérico en columnas que pueden venir como string
    df = df.copy()
    df["ID_HORA"] = pd.to_numeric(df["ID_HORA"], errors="coerce")
    df["MES"]     = pd.to_numeric(df["MES"],     errors="coerce")
    df["ANIO"]    = pd.to_numeric(df["ANIO"],    errors="coerce")

    por_hora = (
        df[df["ID_HORA"].between(0, 23)]
        .groupby("ID_HORA")
        .size()
        .reset_index(name="total")
        .sort_values("ID_HORA")
        .to_dict(orient="records")
    )

    por_mes = (
        df[df["MES"].between(1, 12)]
        .groupby("MES")
        .agg(total=("ANIO", "count"), muertos=("TOTAL_MUERTOS", "sum"))
        .reset_index()
        .to_dict(orient="records")
    )

    # DIASEMANA viene como string directo del INEGI
    DIAS_ORD = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
    por_dia_semana = (
        df[df["DIASEMANA"].isin(DIAS_ORD)]
        .groupby("DIASEMANA")
        .size()
        .reset_index(name="total")
        .to_dict(orient="records")
    )

    por_anio = (
        df.dropna(subset=["ANIO"])
        .groupby("ANIO")
        .agg(
            total=("MES", "count"),
            muertos=("TOTAL_MUERTOS", "sum"),
            heridos=("TOTAL_HERIDOS", "sum"),
        )
        .reset_index()
        .sort_values("ANIO")
        .to_dict(orient="records")
    )

    return {
        "por_hora": por_hora,
        "por_mes": por_mes,
        "por_dia_semana": por_dia_semana,
        "por_anio": por_anio,
    }


@ray.remote
def analisis_causas(df) -> dict:

    causas = (
        df.dropna(subset=["NOM_CAUSA"])
        .groupby("NOM_CAUSA")
        .agg(
            total=("ANIO", "count"),
            muertos=("TOTAL_MUERTOS", "sum"),
            heridos=("TOTAL_HERIDOS", "sum"),
        )
        .reset_index()
        .sort_values("total", ascending=False)
        .to_dict(orient="records")
    )

    tipos = (
        df.dropna(subset=["NOM_TIPACCID"])
        .groupby("NOM_TIPACCID")
        .agg(total=("ANIO", "count"), muertos=("TOTAL_MUERTOS", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
        .to_dict(orient="records")
    )

    return {"causas": causas, "tipos_accidente": tipos}


@ray.remote
def analisis_gravedad(df) -> dict:

    # Accidentes con fallecidos
    graves = df[df["TOTAL_MUERTOS"] > 0]
    por_entidad_graves = (
        graves.groupby(["ID_ENTIDAD", "NOM_ENTIDAD"])
        .agg(
            accidentes_con_muertos=("TOTAL_MUERTOS", "count"),
            total_muertos=("TOTAL_MUERTOS", "sum"),
        )
        .reset_index()
        .sort_values("total_muertos", ascending=False)
        .to_dict(orient="records")
    )

    resumen = {
        "total_accidentes": int(len(df)),
        "con_heridos": int((df["TOTAL_HERIDOS"] > 0).sum()),
        "con_muertos": int((df["TOTAL_MUERTOS"] > 0).sum()),
        "total_muertos": int(df["TOTAL_MUERTOS"].sum()),
        "total_heridos": int(df["TOTAL_HERIDOS"].sum()),
        "indice_gravedad_promedio": float(df["GRAVEDAD"].mean()),
    }

    return {"resumen": resumen, "por_entidad_graves": por_entidad_graves}


# ── Coordinador ──────────────────────────────────────────────────────────────

def ejecutar_analisis(parquet_dir: str, reports_dir: str) -> dict:
    if not ray.is_initialized():
        head = os.environ.get("RAY_ADDRESS", "ray-head:6379"); ray.init(address=head, ignore_reinit_error=True)

    os.makedirs(reports_dir, exist_ok=True)

    print("Cargando datos...")
    t0 = time.time()
    df = leer_parquet_todos(parquet_dir)
    print(f"  {len(df):,} registros cargados ({time.time()-t0:.1f}s)")

    # Subconjuntos de columnas por tarea — reduce memoria en workers
    ref_entidad   = ray.put(df[["ID_ENTIDAD","NOM_ENTIDAD","ANIO","TOTAL_MUERTOS","TOTAL_HERIDOS","GRAVEDAD"]])
    ref_municipio = ray.put(df[["ID_ENTIDAD","NOM_ENTIDAD","ID_MUNICIPIO","ANIO","TOTAL_MUERTOS","TOTAL_HERIDOS"]])
    ref_temporal  = ray.put(df[["ID_HORA","MES","DIASEMANA","ANIO","TOTAL_MUERTOS","TOTAL_HERIDOS"]])
    ref_causas    = ray.put(df[["NOM_CAUSA","NOM_TIPACCID","ANIO","TOTAL_MUERTOS","TOTAL_HERIDOS"]])
    ref_gravedad  = ray.put(df[["ID_ENTIDAD","NOM_ENTIDAD","TOTAL_MUERTOS","TOTAL_HERIDOS","GRAVEDAD"]])

    print("Lanzando análisis distribuido...")
    t1 = time.time()
    fut_entidad   = analisis_por_entidad.remote(ref_entidad)
    fut_municipio = analisis_por_municipio.remote(ref_municipio)
    fut_temporal  = analisis_temporal.remote(ref_temporal)
    fut_causas    = analisis_causas.remote(ref_causas)
    fut_gravedad  = analisis_gravedad.remote(ref_gravedad)

    resultados = {
        "entidades":   ray.get(fut_entidad),
        "municipios":  ray.get(fut_municipio),
        "temporal":    ray.get(fut_temporal),
        "causas":      ray.get(fut_causas),
        "gravedad":    ray.get(fut_gravedad),
    }
    t2 = time.time()
    print(f"  Análisis completado en {t2-t1:.2f}s")

    # Guardar JSON para el dashboard
    salida = os.path.join(reports_dir, "resultados.json")
    with open(salida, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2, default=str)

    print(f"\nResultados guardados en: {salida}")
    return resultados




if __name__ == "__main__":
    parquet_dir = "/app/data/parquet"
    reports_dir = "/app/data/reports"
    os.makedirs(reports_dir, exist_ok=True)
    ejecutar_analisis(parquet_dir, reports_dir)