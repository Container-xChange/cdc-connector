FROM debezium/connect:3.0.0.Final

# Switch to root for system setup
USER root

# Install OpenVPN and netcat for VPN connection
# Debezium image is based on Red Hat UBI, use microdnf
RUN microdnf install -y openvpn nmap-ncat iproute ca-certificates && microdnf clean all

# JMX Prometheus exporter agent (loaded when ENABLE_JMX_EXPORTER=true)
ARG JMX_EXPORTER_VERSION=0.20.0
RUN curl -sSL -o /kafka/libs/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar
COPY metrics.yaml /kafka/config/metrics.yaml

# Remove unnecessary connectors - keep only MySQL/MariaDB and JDBC (for Postgres sink)
RUN rm -rf /kafka/connect/debezium-connector-mongodb \
    /kafka/connect/debezium-connector-oracle \
    /kafka/connect/debezium-connector-db2 \
    /kafka/connect/debezium-connector-sqlserver \
    /kafka/connect/debezium-connector-postgres \
    /kafka/connect/debezium-connector-informix \
    /kafka/connect/debezium-connector-ibmi

# Copy VPN configuration and entrypoint
COPY profile-286.ovpn /kafka/profile-286.ovpn
COPY pass-prod.txt /pass-prod.txt
COPY entrypoint-prod.sh /entrypoint-prod.sh

# Copy connector configurations
COPY connectors/sources/mariadb/*.json /kafka/connectors/sources/mariadb/
COPY connectors/sinks/postgres/*.json /kafka/connectors/sinks/postgres/

# Update CA certificates to fix SSL verification
RUN update-ca-trust

# Set permissions
RUN chmod 600 /pass-prod.txt && \
    chmod 644 /kafka/profile-286.ovpn && \
    chmod +x /entrypoint-prod.sh

# Stay as root (OpenVPN requires root privileges)
# The original Debezium entrypoint will handle user switching if needed

ENTRYPOINT ["/entrypoint-prod.sh"]
