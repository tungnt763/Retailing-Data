FROM --platform=linux/amd64 apache/airflow:2.8.0

USER root

# Cài đặt hệ thống và Java
RUN apt-get update --allow-releaseinfo-change && \
    apt-get install -y openjdk-17-jdk wget curl tzdata python3 python3-distutils python3-venv python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường cho Spark và Python
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Cài Spark
ENV SPARK_VERSION=3.5.5
ENV SPARK_HOME=/opt/spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Thêm thư viện S3 và JDBC driver cho Spark
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -P /opt/spark/jars/ && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /opt/spark/jars/


# Cập nhật biến môi trường
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER airflow

ENV MINIO_ACCESS_KEY='minio'
ENV MINIO_SECRET_KEY='minio123'
ENV MINIO_ENDPOINT='minio:9000'
ENV MINIO_BUCKET_RAW='raw'

COPY ./.env /opt/airflow/.env
COPY ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt