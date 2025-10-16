FROM openjdk:17-jdk-slim

ENV KAFKA_VERSION=3.7.0
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka
ENV KAFKA_CLUSTER_ID=""

RUN apt-get update && apt-get install -y wget tar bash

RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
    && mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} $KAFKA_HOME \
    && rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz


WORKDIR $KAFKA_HOME

EXPOSE 9092 9093

COPY start-kraft.sh /start-kraft.sh
RUN chmod +x /start-kraft.sh

CMD ["/start-kraft.sh"]
