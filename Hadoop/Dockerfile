FROM python:3.11-slim

WORKDIR /Hadoop

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY export_postgres.py .

CMD ["python", "export_postgres.py"]
