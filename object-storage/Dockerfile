FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY upload_spark_features.py .

ENTRYPOINT ["python", "/app/upload_spark_features.py"]
