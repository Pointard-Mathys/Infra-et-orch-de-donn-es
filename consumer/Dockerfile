FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
COPY consumer_postgres.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "consumer_postgres.py"]
