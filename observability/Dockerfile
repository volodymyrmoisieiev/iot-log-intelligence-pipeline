FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY write_pipeline_observability.py /app/write_pipeline_observability.py

ENTRYPOINT ["python", "/app/write_pipeline_observability.py"]
