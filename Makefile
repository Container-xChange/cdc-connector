.PHONY: help connectors connector-status unregister-all \
	register-trading-source register-trading-sink \
	register-finance-source register-finance-sink \
	register-live-source register-live-sink \
	register-chat-source register-chat-sink \
	register-performance-source register-performance-sink \
	register-concontrol-source register-concontrol-sink \
	register-claim-source register-claim-sink \
	register-payment-source register-payment-sink \
	restart-trading-source restart-trading-sink \
	restart-finance-source restart-finance-sink \
	restart-live-source restart-live-sink \
	restart-chat-source restart-chat-sink \
	restart-performance-source restart-performance-sink \
	restart-concontrol-source restart-concontrol-sink \
	restart-claim-source restart-claim-sink \
	restart-payment-source restart-payment-sink \
	unregister-trading-source unregister-trading-sink \
	unregister-finance-source unregister-finance-sink \
	unregister-live-source unregister-live-sink \
	unregister-chat-source unregister-chat-sink \
	unregister-performance-source unregister-performance-sink \
	unregister-concontrol-source unregister-concontrol-sink \
	unregister-claim-source unregister-claim-sink \
	unregister-payment-source unregister-payment-sink

SHELL := /bin/bash

.ONESHELL:
.EXPORT_ALL_VARIABLES:

# Source .env if it exists, otherwise rely on existing environment
ENV_FILE := .env
ifneq (,$(wildcard $(ENV_FILE)))
    include $(ENV_FILE)
endif

help:
	@echo "CDC Pipeline - Debezium Connector Management"
	@echo ""
	@echo "Register Connectors:"
	@echo "  make register-<database>-source   - Register source connector"
	@echo "  make register-<database>-sink     - Register sink connector"
	@echo ""
	@echo "Restart Connectors:"
	@echo "  make restart-<database>-source    - Restart source connector (preserves connection)"
	@echo "  make restart-<database>-sink      - Restart sink connector (preserves connection)"
	@echo ""
	@echo "Unregister Connectors:"
	@echo "  make unregister-<database>-source - Delete source connector"
	@echo "  make unregister-<database>-sink   - Delete sink connector"
	@echo "  make unregister-all               - Delete ALL source and sink connectors"
	@echo ""
	@echo "Available databases: trading, finance, live, chat, performance, concontrol, claim, payment"
	@echo ""
	@echo "Status:"
	@echo "  make connectors                   - List all connectors and their status"
	@echo "  make connector-status C=<name>    - Get specific connector status"

# Trading
register-trading-source:
	@echo "Registering Trading source connector..."
	@envsubst < connectors/sources/mariadb/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Trading source registered"

register-trading-sink:
	@echo "Registering Trading sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Trading sink registered"

restart-trading-source:
	@echo "Restarting Trading source connector..."
	@envsubst < connectors/sources/mariadb/trading.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-trading-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Trading source config updated"

restart-trading-sink:
	@echo "Restarting Trading sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/trading.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-trading/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Trading sink config updated"

unregister-trading-source:
	@echo "Deleting Trading source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-trading-connector 2>/dev/null || true
	@echo "✓ Trading source deleted"

unregister-trading-sink:
	@echo "Deleting Trading sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@echo "✓ Trading sink deleted"

# Finance
register-finance-source:
	@echo "Registering Finance source connector..."
	@envsubst < connectors/sources/mariadb/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Finance source registered"

register-finance-sink:
	@echo "Registering Finance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Finance sink registered"

restart-finance-source:
	@echo "Restarting Finance source connector..."
	@envsubst < connectors/sources/mariadb/finance.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-finance-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Finance source config updated"

restart-finance-sink:
	@echo "Restarting Finance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/finance.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-finance/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Finance sink config updated"

unregister-finance-source:
	@echo "Deleting Finance source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-finance-connector 2>/dev/null || true
	@echo "✓ Finance source deleted"

unregister-finance-sink:
	@echo "Deleting Finance sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-finance 2>/dev/null || true
	@echo "✓ Finance sink deleted"

# Live
register-live-source:
	@echo "Registering Live source connector..."
	@envsubst '$${LIVE_HOST} $${LIVE_PORT} $${LIVE_USER} $${LIVE_PASS} $${LIVE_SERVER_ID} $${LIVE_TABLE_ALLOWLIST} $${KAFKA_BOOTSTRAP_SERVERS} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${KAFKA_LOGIN_MODULE} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sources/mariadb/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Live source registered"

register-live-sink:
	@echo "Registering Live sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Live sink registered"

restart-live-source:
	@echo "Restarting Live source connector..."
	@envsubst '$${LIVE_HOST} $${LIVE_PORT} $${LIVE_USER} $${LIVE_PASS} $${LIVE_SERVER_ID} $${LIVE_TABLE_ALLOWLIST} $${KAFKA_BOOTSTRAP_SERVERS} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${KAFKA_LOGIN_MODULE} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sources/mariadb/live.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-live-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Live source config updated"

restart-live-sink:
	@echo "Restarting Live sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/live.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-live/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Live sink config updated"

unregister-live-source:
	@echo "Deleting Live source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-live-connector 2>/dev/null || true
	@echo "✓ Live source deleted"

unregister-live-sink:
	@echo "Deleting Live sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-live 2>/dev/null || true
	@echo "✓ Live sink deleted"

# Chat
register-chat-source:
	@echo "Registering Chat source connector..."
	@envsubst < connectors/sources/mariadb/chat.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Chat source registered"

register-chat-sink:
	@echo "Registering Chat sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/chat.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Chat sink registered"

restart-chat-source:
	@echo "Restarting Chat source connector..."
	@envsubst < connectors/sources/mariadb/chat.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-chat-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Chat source config updated"

restart-chat-sink:
	@echo "Restarting Chat sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/chat.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-chat/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Chat sink config updated"

unregister-chat-source:
	@echo "Deleting Chat source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-chat-connector 2>/dev/null || true
	@echo "✓ Chat source deleted"

unregister-chat-sink:
	@echo "Deleting Chat sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-chat 2>/dev/null || true
	@echo "✓ Chat sink deleted"

# Performance
register-performance-source:
	@echo "Registering Performance source connector..."
	@envsubst < connectors/sources/mariadb/performance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Performance source registered"

register-performance-sink:
	@echo "Registering Performance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/performance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Performance sink registered"

restart-performance-source:
	@echo "Restarting Performance source connector..."
	@envsubst < connectors/sources/mariadb/performance.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-performance-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Performance source config updated"

restart-performance-sink:
	@echo "Restarting Performance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/performance.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-performance/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Performance sink config updated"

unregister-performance-source:
	@echo "Deleting Performance source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-performance-connector 2>/dev/null || true
	@echo "✓ Performance source deleted"

unregister-performance-sink:
	@echo "Deleting Performance sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-performance 2>/dev/null || true
	@echo "✓ Performance sink deleted"

# Concontrol
register-concontrol-source:
	@echo "Registering Concontrol source connector..."
	@envsubst < connectors/sources/mariadb/concontrol.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Concontrol source registered"

register-concontrol-sink:
	@echo "Registering Concontrol sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/concontrol.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Concontrol sink registered"

restart-concontrol-source:
	@echo "Restarting Concontrol source connector..."
	@envsubst < connectors/sources/mariadb/concontrol.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-concontrol-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Concontrol source config updated"

restart-concontrol-sink:
	@echo "Restarting Concontrol sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/concontrol.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-concontrol/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Concontrol sink config updated"

unregister-concontrol-source:
	@echo "Deleting Concontrol source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-concontrol-connector 2>/dev/null || true
	@echo "✓ Concontrol source deleted"

unregister-concontrol-sink:
	@echo "Deleting Concontrol sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-concontrol 2>/dev/null || true
	@echo "✓ Concontrol sink deleted"

# Claim
register-claim-source:
	@echo "Registering Claim source connector..."
	@envsubst < connectors/sources/mariadb/claim.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Claim source registered"

register-claim-sink:
	@echo "Registering Claim sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/claim.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Claim sink registered"

restart-claim-source:
	@echo "Restarting Claim source connector..."
	@envsubst < connectors/sources/mariadb/claim.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-claim-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Claim source config updated"

restart-claim-sink:
	@echo "Restarting Claim sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/claim.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-claim/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Claim sink config updated"

unregister-claim-source:
	@echo "Deleting Claim source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-claim-connector 2>/dev/null || true
	@echo "✓ Claim source deleted"

unregister-claim-sink:
	@echo "Deleting Claim sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-claim 2>/dev/null || true
	@echo "✓ Claim sink deleted"

# Payment
register-payment-source:
	@echo "Registering Payment source connector..."
	@envsubst < connectors/sources/mariadb/payment.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Payment source registered"

register-payment-sink:
	@echo "Registering Payment sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/payment.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Payment sink registered"

restart-payment-source:
	@echo "Restarting Payment source connector..."
	@envsubst < connectors/sources/mariadb/payment.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/mariadb-payment-connector/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Payment source config updated"

restart-payment-sink:
	@echo "Restarting Payment sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/payment.json | jq '.config' | \
	curl -X PUT $$DEBEZIUM_URL/connectors/postgres-sink-payment/config \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Payment sink config updated"

unregister-payment-source:
	@echo "Deleting Payment source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-payment-connector 2>/dev/null || true
	@echo "✓ Payment source deleted"

unregister-payment-sink:
	@echo "Deleting Payment sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-payment 2>/dev/null || true
	@echo "✓ Payment sink deleted"

# Status
connectors:
	@echo "Registered connectors:"
	@curl -s $$DEBEZIUM_URL/connectors | jq -r '.[]' | while read c; do \
		echo ""; \
		echo "$$c:"; \
		curl -s $$DEBEZIUM_URL/connectors/$$c/status | jq '{state: .connector.state, tasks: [.tasks[].state]}'; \
	done

connector-status:
	@if [ -z "$(C)" ]; then \
		echo "Usage: make connector-status C=<connector-name>"; \
		exit 1; \
	fi
	@curl -s $$DEBEZIUM_URL/connectors/$(C)/status | jq .

# Unregister all connectors
unregister-all:
	@echo "⚠️  WARNING: This will delete ALL connectors (sources and sinks)"
	@echo "Deleting all source connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-trading-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-finance-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-live-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-chat-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-performance-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-concontrol-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-claim-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-payment-connector 2>/dev/null || true
	@echo "✓ All source connectors deleted"
	@echo ""
	@echo "Deleting all sink connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-finance 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-live 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-chat 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-performance 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-concontrol 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-claim 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-payment 2>/dev/null || true
	@echo "✓ All sink connectors deleted"
	@echo ""
	@echo "✅ All connectors have been unregistered"

register-dev-source:
	@echo "Registering Dev source connector..."
	@envsubst < connectors/sources/mariadb/test.json | \
	curl -X POST $$DEV_DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Dev source registered"

register-dev-sink:
	@echo "Registering Dev sink connector..."
	@envsubst '$${DEV_DB_URL} $${DEV_DB_USER} $${DEV_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/test.json | \
	curl -X POST $$DEV_DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo "✓ Dev sink registered"