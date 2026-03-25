FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_load.py api.py data_profiler.py statistical_analysis.py ./
COPY dashboard/ dashboard/
