FROM quay.io/debezium/connect:3.3

USER root

ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx-exporter/jmx_prometheus_javaagent.jar
COPY kafka-connect.yml /opt/jmx-exporter/kafka-connect.yml
RUN chmod 0444 /opt/jmx-exporter/jmx_prometheus_javaagent.jar \
    /opt/jmx-exporter/kafka-connect.yml

USER kafka
