"""
Dashboard ATUS — Streamlit
Ejecutar: streamlit run dashboard/app.py
"""

import json
import os
import sys

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ───────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ATUS México — Accidentes Viales",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

RESULTS_PATH = os.environ.get("RESULTS_PATH", "data/reports/resultados.json")

MESES = {1:"Ene",2:"Feb",3:"Mar",4:"Abr",5:"May",6:"Jun",
          7:"Jul",8:"Ago",9:"Sep",10:"Oct",11:"Nov",12:"Dic"}

DIAS = {1:"Lunes",2:"Martes",3:"Miércoles",4:"Jueves",
        5:"Viernes",6:"Sábado",7:"Domingo"}

PALETTE = px.colors.sequential.Reds_r


# ── Carga de datos ────────────────────────────────────────────────────────────
@st.cache_data
def cargar_resultados():
    if not os.path.exists(RESULTS_PATH):
        return None
    with open(RESULTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def df_from(lista):
    return pd.DataFrame(lista)


# ── Estilos ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;600;700&family=Syne:wght@700;800&display=swap');

  html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }

  .titulo-hero {
    font-family: 'Syne', sans-serif;
    font-size: 2.6rem;
    font-weight: 800;
    line-height: 1.1;
    color: #fff;
    background: linear-gradient(135deg, #c0392b 0%, #922b21 60%, #641e16 100%);
    padding: 2rem 2.5rem;
    border-radius: 16px;
    margin-bottom: 1.5rem;
    letter-spacing: -0.5px;
  }

  .kpi-card {
    background: #1a1a2e;
    border: 1px solid #c0392b33;
    border-left: 4px solid #c0392b;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    text-align: center;
  }
  .kpi-num {
    font-family: 'Syne', sans-serif;
    font-size: 2rem;
    font-weight: 800;
    color: #e74c3c;
  }
  .kpi-label {
    font-size: 0.78rem;
    color: #aaa;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.2rem;
  }

  .section-title {
    font-family: 'Syne', sans-serif;
    font-size: 1.3rem;
    font-weight: 700;
    color: #e74c3c;
    border-bottom: 2px solid #c0392b22;
    padding-bottom: 0.4rem;
    margin: 2rem 0 1rem 0;
  }

  div[data-testid="stMetric"] { display: none; }

  .stTabs [data-baseweb="tab"] {
    font-family: 'Space Grotesk', sans-serif;
    font-weight: 600;
  }
</style>
""", unsafe_allow_html=True)

PLOTLY_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="#ddd",
    font_family="Space Grotesk",
    colorway=["#e74c3c","#c0392b","#922b21","#f1948a","#ec7063"],
)


def fig_theme(fig):
    fig.update_layout(**PLOTLY_THEME)
    fig.update_xaxes(gridcolor="#333", zeroline=False)
    fig.update_yaxes(gridcolor="#333", zeroline=False)
    return fig


# ── Main ──────────────────────────────────────────────────────────────────────
datos = cargar_resultados()

st.markdown("""
<div class="titulo-hero">
  🚦 ATUS México<br>
  <span style="font-size:1.1rem;font-weight:400;opacity:0.85">
  Accidentes de Tránsito Terrestre · 1997–2024
  </span>
</div>
""", unsafe_allow_html=True)

if datos is None:
    st.error(f"No se encontró `{RESULTS_PATH}`.")
    st.info("Ejecuta primero:\n```bash\npython src/etl.py\npython src/analisis.py\n```")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────────────────
g = datos["gravedad"]["resumen"]

kpis = [
    (f"{g['total_accidentes']:,}", "Total de accidentes"),
    (f"{g['total_muertos']:,}", "Fallecidos"),
    (f"{g['total_heridos']:,}", "Heridos"),
    (f"{g['con_muertos']:,}", "Accidentes con fallecidos"),
    (f"{g['indice_gravedad_promedio']:.2f}", "Índice de gravedad promedio"),
]

cols = st.columns(len(kpis))
for col, (num, label) in zip(cols, kpis):
    with col:
        st.markdown(f"""
        <div class="kpi-card">
          <div class="kpi-num">{num}</div>
          <div class="kpi-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)

# ── Pestañas ──────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🗺️ Por Estado", "🏙️ Municipios", "🕐 Temporal", "⚠️ Causas", "📊 Benchmark"]
)

# ── Tab 1: Entidades ──────────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-title">Accidentes por Estado</div>', unsafe_allow_html=True)

    df_ent = df_from(datos["entidades"])

    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(
            df_ent.head(20),
            x="total_accidentes", y="NOM_ENTIDAD",
            orientation="h",
            color="total_accidentes",
            color_continuous_scale="Reds",
            labels={"total_accidentes": "Accidentes", "NOM_ENTIDAD": "Estado"},
            title="Top 20 estados — total de accidentes",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
        st.plotly_chart(fig_theme(fig), use_container_width=True)

    with col2:
        fig2 = px.bar(
            df_ent.head(15),
            x="total_muertos", y="NOM_ENTIDAD",
            orientation="h",
            color="total_muertos",
            color_continuous_scale="Reds",
            labels={"total_muertos": "Fallecidos", "NOM_ENTIDAD": "Estado"},
            title="Fallecidos por estado",
        )
        fig2.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
        st.plotly_chart(fig_theme(fig2), use_container_width=True)

    # Tabla
    st.dataframe(
        df_ent.rename(columns={
            "NOM_ENTIDAD": "Estado",
            "total_accidentes": "Accidentes",
            "total_muertos": "Fallecidos",
            "total_heridos": "Heridos",
            "gravedad_total": "Gravedad",
        }).set_index("Estado").drop(columns=["ID_ENTIDAD"], errors="ignore"),
        use_container_width=True,
    )

# ── Tab 2: Municipios ─────────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-title">Top 50 Municipios con Mayor Siniestralidad</div>', unsafe_allow_html=True)

    df_mun = df_from(datos["municipios"])
    df_mun["etiqueta"] = (
        df_mun["NOM_ENTIDAD"].fillna("?") + " · " + df_mun["ID_MUNICIPIO"].astype(str)
    )

    fig = px.bar(
        df_mun.head(30),
        x="total_accidentes", y="etiqueta",
        orientation="h",
        color="total_muertos",
        color_continuous_scale="OrRd",
        labels={"total_accidentes": "Accidentes", "etiqueta": "Municipio"},
        title="Top 30 municipios — accidentes vs fallecidos",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=700)
    st.plotly_chart(fig_theme(fig), use_container_width=True)

# ── Tab 3: Temporal ───────────────────────────────────────────────────────────
with tab3:
    temp = datos["temporal"]

    st.markdown('<div class="section-title">Tendencia Anual</div>', unsafe_allow_html=True)
    df_anio = df_from(temp["por_anio"])
    if not df_anio.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_anio["ANIO"], y=df_anio["total"],
            name="Accidentes", line=dict(color="#e74c3c", width=2.5), fill="tozeroy",
            fillcolor="rgba(231,76,60,0.15)",
        ))
        fig.add_trace(go.Scatter(
            x=df_anio["ANIO"], y=df_anio["muertos"],
            name="Fallecidos", line=dict(color="#f39c12", width=2),
        ))
        fig.update_layout(title="Accidentes y fallecidos anuales (1997–2024)")
        st.plotly_chart(fig_theme(fig), use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-title">Accidentes por Hora</div>', unsafe_allow_html=True)
        df_hora = df_from(temp["por_hora"])
        if not df_hora.empty:
            fig = px.bar(
                df_hora, x="ID_HORA", y="total",
                color="total", color_continuous_scale="Reds",
                labels={"ID_HORA": "Hora del día", "total": "Accidentes"},
            )
            st.plotly_chart(fig_theme(fig), use_container_width=True)

    with col2:
        st.markdown('<div class="section-title">Accidentes por Mes</div>', unsafe_allow_html=True)
        df_mes = df_from(temp["por_mes"])
        if not df_mes.empty:
            df_mes["mes_nombre"] = df_mes["MES"].map(MESES)
            fig = px.bar(
                df_mes, x="mes_nombre", y="total",
                color="muertos", color_continuous_scale="Reds",
                labels={"mes_nombre": "Mes", "total": "Accidentes"},
                category_orders={"mes_nombre": list(MESES.values())},
            )
            st.plotly_chart(fig_theme(fig), use_container_width=True)

    st.markdown('<div class="section-title">Accidentes por Día de Semana</div>', unsafe_allow_html=True)
    df_dia = df_from(temp["por_dia_semana"])
    if not df_dia.empty:
        df_dia["dia_nombre"] = df_dia["DIASEMANA"].map(DIAS)
        fig = px.bar(
            df_dia, x="dia_nombre", y="total",
            color="total", color_continuous_scale="Reds",
            labels={"dia_nombre": "Día", "total": "Accidentes"},
            category_orders={"dia_nombre": list(DIAS.values())},
        )
        st.plotly_chart(fig_theme(fig), use_container_width=True)

# ── Tab 4: Causas ─────────────────────────────────────────────────────────────
with tab4:
    causas_data = datos["causas"]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-title">Causas Frecuentes</div>', unsafe_allow_html=True)
        df_cau = df_from(causas_data["causas"])
        if not df_cau.empty:
            fig = px.pie(
                df_cau, names="NOM_CAUSA", values="total",
                color_discrete_sequence=px.colors.sequential.Reds_r,
                hole=0.45,
            )
            fig.update_traces(textposition="outside", textinfo="percent+label")
            st.plotly_chart(fig_theme(fig), use_container_width=True)

    with col2:
        st.markdown('<div class="section-title">Tipos de Accidente</div>', unsafe_allow_html=True)
        df_tip = df_from(causas_data["tipos_accidente"])
        if not df_tip.empty:
            fig = px.bar(
                df_tip, x="total", y="NOM_TIPACCID",
                orientation="h", color="muertos",
                color_continuous_scale="Reds",
                labels={"total": "Total", "NOM_TIPACCID": "Tipo"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_theme(fig), use_container_width=True)

    # Mortalidad por causa
    st.markdown('<div class="section-title">Fallecidos por Causa</div>', unsafe_allow_html=True)
    if not df_cau.empty:
        df_cau["tasa_mortalidad"] = df_cau["muertos"] / df_cau["total"]
        fig = px.bar(
            df_cau.sort_values("muertos", ascending=False),
            x="NOM_CAUSA", y="muertos",
            color="tasa_mortalidad", color_continuous_scale="Reds",
            labels={"NOM_CAUSA": "Causa", "muertos": "Fallecidos", "tasa_mortalidad": "Tasa"},
        )
        st.plotly_chart(fig_theme(fig), use_container_width=True)

# ── Tab 5: Benchmark ──────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-title">Rendimiento: Ray vs Pandas Secuencial</div>', unsafe_allow_html=True)

    benchmark_path = "data/reports/benchmark.json"
    if os.path.exists(benchmark_path):
        with open(benchmark_path) as f:
            bm = json.load(f)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
              <div class="kpi-num">{bm['tiempo_secuencial_s']}s</div>
              <div class="kpi-label">Pandas Secuencial</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
              <div class="kpi-num">{bm['tiempo_ray_s']}s</div>
              <div class="kpi-label">Ray Distribuido</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
              <div class="kpi-num">{bm['speedup']}x</div>
              <div class="kpi-label">Speedup</div>
            </div>""", unsafe_allow_html=True)

        fig = go.Figure(go.Bar(
            x=["Pandas Secuencial", "Ray Distribuido"],
            y=[bm["tiempo_secuencial_s"], bm["tiempo_ray_s"]],
            marker_color=["#555", "#e74c3c"],
            text=[f"{bm['tiempo_secuencial_s']}s", f"{bm['tiempo_ray_s']}s"],
            textposition="auto",
        ))
        fig.update_layout(title=f"Tiempo de análisis — {bm['registros']:,} registros", yaxis_title="Segundos")
        st.plotly_chart(fig_theme(fig), use_container_width=True)

    else:
        st.info("Ejecuta el benchmark para ver resultados:\n```bash\npython src/analisis.py benchmark\n```")
        st.markdown("""
        **¿Qué mide el benchmark?**

        Compara el tiempo de ejecutar los mismos 5 análisis (por entidad, temporal, causas, gravedad, municipios)
        de forma secuencial con Pandas vs distribuida con Ray en paralelo.

        El speedup depende del número de núcleos disponibles en el nodo.
        """)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
---
<div style="text-align:center;color:#555;font-size:0.75rem">
Fuente: INEGI — ATUS · Accidentes de Tránsito Terrestre en Zonas Urbanas y Suburbanas
</div>
""", unsafe_allow_html=True)
