# ATUS México — Análisis Distribuido de Accidentes Viales

Sistema distribuido con **Python + Ray + Docker + Streamlit** para procesar y visualizar los datos ATUS del INEGI (1997–2024).

---

## Estructura del proyecto

```
atus_project/
├── src/
│   ├── etl.py          # Limpieza CSV → Parquet
│   └── analisis.py     # Cálculo de indicadores (distribuido con Ray)
├── dashboard/
│   └── app.py          # Dashboard Streamlit
├── docker/
│   ├── Dockerfile          # Head node + dashboard
│   ├── Dockerfile.worker   # Worker nodes
│   └── docker-compose.yml  # Orquestación completa
├── data/
│   ├── raw/            # ← Coloca aquí tus CSVs del ATUS
│   ├── parquet/        # Generado por ETL
│   └── reports/        # JSON con resultados del análisis
└── requirements.txt
```

---

## Flujo de trabajo

```
CSVs raw  →  ETL (Ray)  →  Parquet  →  Análisis (Ray)  →  JSON  →  Dashboard
```

---

## Inicio rápido

### 1. Coloca tus archivos CSV

```bash
# Nombra tus archivos así (como los tienes):
data/raw/atus_anual_1997.csv
data/raw/atus_anual_1998.csv
...
data/raw/atus_anual_2024.csv
```

### 2. Levanta el clúster

```bash
cd docker
docker compose up -d ray-head ray-worker-1 ray-worker-2
```

Espera ~30 segundos a que Ray esté listo.

### 3. Ejecuta el ETL

```bash
docker compose --profile etl up etl
```

Esto convierte todos los CSVs a Parquet optimizados en `data/parquet/`.

### 4. Ejecuta el análisis

```bash
docker compose --profile analisis up analisis
```

Genera `data/reports/resultados.json` con todos los indicadores.

### 5. Abre el dashboard

```
http://localhost:8501
```


## Acceso a los paneles

| Servicio | URL |
|---|---|
| Dashboard ATUS | http://localhost:8501 |
| Ray Dashboard | http://localhost:8265 |

---

## Escalar workers

Para agregar más workers, duplica el bloque `ray-worker-2` en `docker-compose.yml` con un nombre diferente (`ray-worker-3`, etc.) o usa:

```bash
docker compose up -d --scale ray-worker-1=4
```

---

## Variables de columnas usadas

| Columna | Descripción |
|---|---|
| `ID_ENTIDAD` | Clave de estado (1–32) |
| `ID_MUNICIPIO` | Clave de municipio |
| `ANIO`, `MES`, `ID_HORA` | Temporalidad |
| `DIASEMANA` | Día de la semana (1=Lunes) |
| `CAUSAACCI` | Causa del accidente |
| `TIPACCID` | Tipo de accidente |
| `CONDMUERTO/HERIDO` | Víctimas conductores |
| `PASAMUERTO/HERIDO` | Víctimas pasajeros |
| `PEATMUERTO/HERIDO` | Víctimas peatones |
| `CICLMUERTO/HERIDO` | Víctimas ciclistas |
| `GRAVEDAD` | Índice calculado: muertos×3 + heridos |

---
...
## Tecnologías

- **Python 3.11**
- **Ray 2.10** — procesamiento distribuido
- **Pandas 2.2** — ETL y análisis
- **PyArrow** — formato Parquet
- **Streamlit** — dashboard
- **Plotly** — visualizaciones
- **Docker Compose** — orquestación
