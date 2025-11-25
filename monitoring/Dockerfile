FROM python:3.11-slim

WORKDIR /app

# Installer dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script Python
COPY fake_pipeline.py .

# Exposer le port du serveur Flask
EXPOSE 5000

# Lancer le script Python
CMD ["python3", "fake_pipeline.py"]

# FROM python:3.11-slim

# WORKDIR /app
# COPY fake_exporter.py .
# RUN pip install flask
# CMD ["python", "fake_exporter.py"]
