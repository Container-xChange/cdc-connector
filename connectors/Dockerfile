FROM debezium/connect:3.0.0.Final

# Configure Kafka Connect REST API to listen on all interfaces (required for Fly.io)
# Must use CONNECT_ prefix for the Debezium Connect image
ENV CONNECT_REST_HOST_NAME=0.0.0.0
ENV CONNECT_REST_PORT=8083
ENV CONNECT_REST_ADVERTISED_HOST_NAME=cdc-connector.fly.dev
ENV CONNECT_REST_ADVERTISED_PORT=8083

# Switch to root for system setup
USER root

# Install OpenVPN and netcat for connectivity testing
# Debezium image is based on Red Hat UBI, use microdnf
RUN microdnf install -y openvpn nmap-ncat iproute && microdnf clean all

# Remove unnecessary connectors - keep only MySQL/MariaDB and JDBC (for Postgres sink)
RUN rm -rf /kafka/connect/debezium-connector-mongodb \
    /kafka/connect/debezium-connector-oracle \
    /kafka/connect/debezium-connector-db2 \
    /kafka/connect/debezium-connector-sqlserver \
    /kafka/connect/debezium-connector-postgres \
    /kafka/connect/debezium-connector-informix \
    /kafka/connect/debezium-connector-ibmi

# Copy VPN configuration and entrypoint
COPY connectors/profile-286.ovpn /kafka/profile-286.ovpn
COPY connectors/pass-prod.txt /pass-prod.txt
COPY connectors/entrypoint-prod.sh /entrypoint-prod.sh

# Set permissions
RUN chmod 600 /pass-prod.txt && \
    chmod 644 /kafka/profile-286.ovpn && \
    chmod +x /entrypoint-prod.sh

# Stay as root (OpenVPN requires root privileges)
# The original Debezium entrypoint will handle user switching if needed

ENTRYPOINT ["/entrypoint-prod.sh"]
