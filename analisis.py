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
def analisis_por_entidad(df_ref) -> dict:
    df = ray.get(df_ref)
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
def analisis_por_municipio(df_ref, top_n: int = 50) -> dict:
    df = ray.get(df_ref)
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
def analisis_temporal(df_ref) -> dict:
    df = ray.get(df_ref)

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

    por_dia_semana = (
        df[df["DIASEMANA"].between(1, 7)]
        .groupby("DIASEMANA")
        .size()
        .reset_index(name="total")
        .to_dict(orient="records")
    )

    por_anio = (
        df.groupby("ANIO")
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
def analisis_causas(df_ref) -> dict:
    df = ray.get(df_ref)

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
def analisis_gravedad(df_ref) -> dict:
    df = ray.get(df_ref)

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
        ray.init(address="auto", ignore_reinit_error=True)

    os.makedirs(reports_dir, exist_ok=True)

    print("Cargando datos...")
    t0 = time.time()
    df = leer_parquet_todos(parquet_dir)
    print(f"  {len(df):,} registros cargados ({time.time()-t0:.1f}s)")

    # Poner el DataFrame en el object store de Ray una sola vez
    df_ref = ray.put(df)

    print("Lanzando análisis distribuido...")
    t1 = time.time()
    fut_entidad   = analisis_por_entidad.remote(df_ref)
    fut_municipio = analisis_por_municipio.remote(df_ref)
    fut_temporal  = analisis_temporal.remote(df_ref)
    fut_causas    = analisis_causas.remote(df_ref)
    fut_gravedad  = analisis_gravedad.remote(df_ref)

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


# ── Benchmark ────────────────────────────────────────────────────────────────

def benchmark(parquet_dir: str) -> dict:
    """Compara tiempo Ray vs Pandas secuencial."""
    df = leer_parquet_todos(parquet_dir)

    # Secuencial
    t0 = time.time()
    _ = df.groupby("NOM_ENTIDAD").agg(total=("ANIO", "count"), muertos=("TOTAL_MUERTOS", "sum"))
    _ = df.groupby("ID_HORA").size()
    _ = df.groupby("MES").size()
    _ = df.groupby("NOM_CAUSA").size()
    _ = df[df["TOTAL_MUERTOS"] > 0].groupby("NOM_ENTIDAD").size()
    tiempo_seq = time.time() - t0

    # Ray
    if not ray.is_initialized():
        ray.init(address="auto", ignore_reinit_error=True)

    t0 = time.time()
    df_ref = ray.put(df)
    fs = [
        analisis_por_entidad.remote(df_ref),
        analisis_temporal.remote(df_ref),
        analisis_causas.remote(df_ref),
        analisis_gravedad.remote(df_ref),
    ]
    ray.get(fs)
    tiempo_ray = time.time() - t0

    speedup = tiempo_seq / tiempo_ray if tiempo_ray > 0 else 0

    resultado = {
        "registros": len(df),
        "tiempo_secuencial_s": round(tiempo_seq, 3),
        "tiempo_ray_s": round(tiempo_ray, 3),
        "speedup": round(speedup, 2),
    }

    print(f"\n{'='*40}")
    print(f"  BENCHMARK")
    print(f"{'='*40}")
    print(f"  Registros        : {len(df):,}")
    print(f"  Secuencial       : {tiempo_seq:.3f}s")
    print(f"  Ray distribuido  : {tiempo_ray:.3f}s")
    print(f"  Speedup          : {speedup:.2f}x")
    print(f"{'='*40}\n")

    return resultado


if __name__ == "__main__":
    import sys
    parquet_dir = "data/parquet"
    reports_dir = "data/reports"

    if len(sys.argv) > 1 and sys.argv[1] == "benchmark":
        benchmark(parquet_dir)
    else:
        ejecutar_analisis(parquet_dir, reports_dir)
