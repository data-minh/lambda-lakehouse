FROM apache/airflow:2.11.0-python3.11

# Chạy với quyền root để cài đặt hệ thống package
USER root

# Cập nhật hệ thống và cài đặt các package cần thiết
RUN apt-get update && apt-get install -y wget gnupg openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME cho OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Chuyển về user airflow để cài đặt Python packages
USER airflow

# Cài đặt các package Apache Spark cho Airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark==3.5.1 findspark

# Nhắc nhở build image
# Build lệnh: docker build -t airflow-spark .