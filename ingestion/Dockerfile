FROM python:3.13-slim
WORKDIR /app
COPY ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestion/ ./ingestion/
WORKDIR /app/ingestion
CMD ["python", "-u", "consumer.py"]