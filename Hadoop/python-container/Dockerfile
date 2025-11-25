# FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# ENV HBASE_VERSION 2.5.7
# ENV HBASE_HOME /opt/hbase
# ENV PATH $HBASE_HOME/bin:$PATH

# # Installer HBase
# RUN curl -fsSL -o /tmp/hbase.tar.gz https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
#     && tar -xzf /tmp/hbase.tar.gz -C /opt/ \
#     && mv /opt/hbase-${HBASE_VERSION} /opt/hbase \
#     && rm /tmp/hbase.tar.gz

# # Configurer HBase pour utiliser HDFS
# COPY hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

# # Créer dossier pour Zookeeper et logs
# RUN mkdir -p /tmp/zookeeper /hbase/logs && chmod -R 777 /tmp/zookeeper /hbase/logs

# # Entrypoint
# COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh
# ENTRYPOINT ["/entrypoint.sh"]

FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

ENV HBASE_VERSION 2.5.7
ENV HBASE_HOME /opt/hbase
ENV PATH $HBASE_HOME/bin:$PATH

# Installer HBase
RUN curl -fsSL -o /tmp/hbase.tar.gz https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    && tar -xzf /tmp/hbase.tar.gz -C /opt/ \
    && mv /opt/hbase-${HBASE_VERSION} /opt/hbase \
    && rm /tmp/hbase.tar.gz

# Copier hbase-site.xml si tu en as un personnalisé
# COPY hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

# Copier l'entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]






FROM python:3.11-slim

WORKDIR /Hadoop

RUN apt-get update && apt-get install -y cron wget curl unzip vim && \
    rm -rf /var/lib/apt/lists/*

# Installer dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Définir JAVA_HOME pour tout le conteneur
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copier hadoop-env.sh dans le conteneur
COPY hadoop-env.sh /opt/hadoop/etc/hadoop/hadoop-env.sh
RUN chmod +x /opt/hadoop/etc/hadoop/hadoop-env.sh


# Copier scripts et cron
COPY export_postgres.py .
COPY run_hadoop_pipeline.py .
COPY insert_to_hive.py .
COPY processing/ processing/
COPY cronjob /etc/cron.d/data-export-cron

RUN chmod 0644 /etc/cron.d/data-export-cron && crontab /etc/cron.d/data-export-cron
RUN touch /var/log/cron.log

CMD cron && tail -f /var/log/cron.log


