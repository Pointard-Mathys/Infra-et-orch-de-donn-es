# FROM python:3.11-slim

# WORKDIR /Hadoop

# RUN apt-get update && apt-get install -y cron && \
#     rm -rf /var/lib/apt/lists/*


# COPY requirements.txt .
# RUN pip install --upgrade pip
# RUN pip install --no-cache-dir -r requirements.txt

# COPY export_postgres.py .

# COPY cronjob /etc/cron.d/data-export-cron
# RUN chmod 0644 /etc/cron.d/data-export-cron && crontab /etc/cron.d/data-export-cron

# CMD ["cron", "-f"]

# CMD ["python", "export_postgres.py"]






FROM python:3.11-slim

WORKDIR /Hadoop

RUN apt-get update && apt-get install -y cron wget curl unzip vim && \
    rm -rf /var/lib/apt/lists/*

# Installer dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copier scripts et cron
COPY export_postgres.py .
COPY run_hadoop_pipeline.py .
COPY insert_to_hbase.py .
COPY processing/ processing/
COPY cronjob /etc/cron.d/data-export-cron

RUN chmod 0644 /etc/cron.d/data-export-cron && crontab /etc/cron.d/data-export-cron
RUN touch /var/log/cron.log

CMD cron && tail -f /var/log/cron.log


