FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -o /opt/bitnami/spark/jars/postgresql-42.2.23.jar

RUN pip install --upgrade pip && \
    pip install \
    geocoder \
    && rm -rf /root/.cache/pip