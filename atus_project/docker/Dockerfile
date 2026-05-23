FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código
COPY src/ ./src/
COPY dashboard/ ./dashboard/

# Directorios de datos (montados como volumen en producción)
RUN mkdir -p data/raw data/parquet data/reports

ENV PYTHONPATH=/app/src
ENV RESULTS_PATH=/app/data/reports/resultados.json

EXPOSE 8265 8000 8501

# Por defecto arranca Streamlit
# Para ETL: docker exec <container> python src/etl.py
# Para análisis: docker exec <container> python src/analisis.py
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true"]
