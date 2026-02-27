FROM confluentinc/cp-kafka:7.7.1

USER root

# Download JMX Prometheus exporter
RUN mkdir -p /opt/jmx-exporter && \
    curl -fsSL -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar

USER appuser
