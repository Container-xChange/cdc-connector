.PHONY: help register-trading register-finance register-live register-sink unregister-sinks restart-sinks unregister-all connectors check-health deploy connector-status

SHELL := /bin/bash

# Use .ONESHELL to ensure all commands in a recipe run in the same shell
.ONESHELL:
.EXPORT_ALL_VARIABLES:

# Source .env if it exists, otherwise rely on existing environment
ENV_FILE := .env
ifneq (,$(wildcard $(ENV_FILE)))
    include $(ENV_FILE)
endif

help:
	@echo "CDC Pipeline - Debezium + Confluent Cloud (3 MariaDB sources)"
	@echo ""
	@echo "Migration:"
	@echo "  make migrate            - Run pgloader snapshot dump (all databases)"
	@echo "  make load-trading       - Run pgloader for Trading database only"
	@echo "  make add-indexes        - Add indexes and foreign keys after migration"
	@echo ""
	@echo "Connectors:"
	@echo "  make register-trading       - Register Trading source + sink"
	@echo "  make register-finance       - Register Finance source + sink"
	@echo "  make register-live          - Register Live source + sink"
	@echo "  make register-chat          - Register Chat source + sink"
	@echo "  make register-performance   - Register Performance source + sink"
	@echo "  make register-concontrol    - Register Concontrol source + sink"
	@echo "  make register-claim         - Register Claim source + sink"
	@echo "  make register-payment       - Register Payment source + sink"
	@echo "  make register-sources       - Register all source connectors only"
	@echo "  make register-sink          - Register all sink connectors only"
	@echo "  make register-all           - Register all connectors (uses deploy-connectors.sh)"
	@echo "  make restart-sinks          - Restart all sink connectors (keeps sources running)"
	@echo "  make unregister-trading     - Delete Trading source + sink"
	@echo "  make unregister-finance     - Delete Finance source + sink"
	@echo "  make unregister-live        - Delete Live source + sink"
	@echo "  make unregister-chat        - Delete Chat source + sink"
	@echo "  make unregister-performance - Delete Performance source + sink"
	@echo "  make unregister-concontrol  - Delete Concontrol source + sink"
	@echo "  make unregister-claim       - Delete Claim source + sink"
	@echo "  make unregister-payment     - Delete Payment source + sink"
	@echo "  make unregister-sinks       - Delete all sink connectors only"
	@echo "  make unregister-all         - Delete all connectors"
	@echo "  make connectors             - List all connectors and their status"
	@echo "  make check-health           - Check Debezium health"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy             - Register all CDC connectors"
	@echo ""
	@echo "Debugging:"
	@echo "  make connector-status C=<connector-name> - Get connector status"

migrate:
	@echo "🗄️  Running Python migration (MariaDB -> Postgres)..."
	@echo ""
	@which python3 > /dev/null || (echo "❌ python3 not found. Install python3" && exit 1)
	@python3 -c "import mysql.connector" 2>/dev/null || (echo "Installing mysql-connector-python..." && pip3 install mysql-connector-python)
	@python3 -c "import psycopg2" 2>/dev/null || (echo "Installing psycopg2-binary..." && pip3 install psycopg2-binary)
	python3 bootstrap/migrate.py
	@echo ""
	@echo "📊 Adding indexes and foreign keys..."
	@echo ""
	@echo "  → Trading indexes/FKs..."
	@PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/trading-indexes-fks.sql > /dev/null 2>&1
	@echo "  → Finance indexes/FKs..."
	@PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/finance-indexes-fks.sql > /dev/null 2>&1
	@echo "  → Live indexes/FKs..."
	@PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/live-indexes-fks.sql > /dev/null 2>&1
	@echo ""
	@echo "✅ Migration completed with indexes!"

load-trading:
	@echo "🗄️  Installing pgloader via Homebrew if not present..."
	@which pgloader > /dev/null || brew install pgloader
	@echo ""
	@echo "🗄️  Running pgloader migration (Trading database only)..."
	cd bootstrap/pgloader && \
	export PGSSLMODE=prefer && \
	PROCESSED_FILE=trading.processed.load && \
	sed -e "s|\$${TRADING_USER}|$$TRADING_USER|g" \
	    -e "s|\$${TRADING_PASS}|$$TRADING_PASS|g" \
	    -e "s|\$${TRADING_HOST}|$$TRADING_HOST|g" \
	    -e "s|\$${TRADING_PORT}|$$TRADING_PORT|g" \
	    -e "s|\$${TRADING_DB}|$$TRADING_DB|g" \
	    -e "s|\$${PG_USER}|$$PG_USER|g" \
	    -e "s|\$${PG_PASS}|$$PG_PASS|g" \
	    -e "s|\$${PG_HOST}|$$PG_HOST|g" \
	    -e "s|\$${PG_PORT}|$$PG_PORT|g" \
	    -e "s|\$${PG_DB}|$$PG_DB|g" \
	    trading.load > $$PROCESSED_FILE && \
	pgloader --no-ssl-cert-verification --verbose $$PROCESSED_FILE && \
	rm $$PROCESSED_FILE
	@echo ""
	@echo "📊 Adding Trading indexes and foreign keys..."
	@PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/trading-indexes-fks.sql
	@echo ""
	@echo "✅ Trading migration completed with indexes!"

add-indexes:
	@echo ""
	@echo "📊 Adding indexes and foreign keys..."
	@echo ""
	@echo "Adding Trading indexes/FKs..."
	PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/trading-indexes-fks.sql
	@echo ""
	@echo "Adding Finance indexes/FKs..."
	PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/finance-indexes-fks.sql
	@echo ""
	@echo "Adding Live indexes/FKs..."
	PGPASSWORD=$$PG_PASS psql -h $$PG_HOST -p $$PG_PORT -U $$PG_USER -d $$PG_DB -f bootstrap/sql/live-indexes-fks.sql
	@echo ""
	@echo "✅ All indexes and foreign keys created successfully!"

check-health:
	@echo "Checking Debezium health..."
	@curl -sf $$DEBEZIUM_URL/ > /dev/null && echo "✓ Debezium healthy" || echo "✗ Debezium unhealthy"

register-trading-source:
	@echo "Registering xchange_trading source connector..."
	@envsubst < connectors/sources/mariadb/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Trading source registered"

register-finance-source:
	@echo "Registering xchange_finance source connector..."
	@envsubst < connectors/sources/mariadb/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Finance source registered"

register-live-source:
	@echo "Registering xchangelive source connector..."
	@envsubst '$${LIVE_HOST} $${LIVE_PORT} $${LIVE_USER} $${LIVE_PASS} $${LIVE_SERVER_ID} $${LIVE_TABLE_ALLOWLIST} $${KAFKA_BOOTSTRAP_SERVERS} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${KAFKA_LOGIN_MODULE} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sources/mariadb/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Live source registered"

register-sink-trading:
	@echo "Registering Postgres Trading sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Trading sink registered"

register-sink-finance:
	@echo "Registering Postgres Finance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Finance sink registered"

register-sink-live:
	@echo "Registering Postgres Live sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Live sink registered"

register-trading: register-trading-source register-sink-trading
	@echo ""
	@echo "✓ Trading source and sink registered"

register-finance: register-finance-source register-sink-finance
	@echo ""
	@echo "✓ Finance source and sink registered"

register-live: register-live-source register-sink-live
	@echo ""
	@echo "✓ Live source and sink registered"

register-chat-source:
	@echo "Registering xchange_chat source connector..."
	@envsubst < connectors/sources/mariadb/chat.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Chat source registered"

register-sink-chat:
	@echo "Registering Postgres Chat sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/chat.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Chat sink registered"

register-chat: register-chat-source register-sink-chat
	@echo ""
	@echo "✓ Chat source and sink registered"

register-performance-source:
	@echo "Registering xchange_performance source connector..."
	@envsubst < connectors/sources/mariadb/performance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Performance source registered"

register-sink-performance:
	@echo "Registering Postgres Performance sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/performance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Performance sink registered"

register-performance: register-performance-source register-sink-performance
	@echo ""
	@echo "✓ Performance source and sink registered"

register-concontrol-source:
	@echo "Registering xchange_concontrol source connector..."
	@envsubst < connectors/sources/mariadb/concontrol.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Concontrol source registered"

register-sink-concontrol:
	@echo "Registering Postgres Concontrol sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/concontrol.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Concontrol sink registered"

register-concontrol: register-concontrol-source register-sink-concontrol
	@echo ""
	@echo "✓ Concontrol source and sink registered"

register-claim-source:
	@echo "Registering xchange_claim source connector..."
	@envsubst < connectors/sources/mariadb/claim.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Claim source registered"

register-sink-claim:
	@echo "Registering Postgres Claim sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/claim.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Claim sink registered"

register-claim: register-claim-source register-sink-claim
	@echo ""
	@echo "✓ Claim source and sink registered"

register-payment-source:
	@echo "Registering xchange_payment source connector..."
	@envsubst < connectors/sources/mariadb/payment.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Payment source registered"

register-sink-payment:
	@echo "Registering Postgres Payment sink connector..."
	@envsubst '$${SINK_DB_URL} $${SINK_DB_USER} $${SINK_DB_PASSWORD} $${KAFKA_SECURITY_PROTOCOL} $${KAFKA_SASL_MECHANISM} $${CONFLUENT_API_KEY} $${CONFLUENT_API_SECRET}' < connectors/sinks/postgres/payment.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Payment sink registered"

register-payment: register-payment-source register-sink-payment
	@echo ""
	@echo "✓ Payment source and sink registered"

register-sink: register-sink-trading register-sink-live register-sink-chat register-sink-performance register-sink-concontrol register-sink-claim register-sink-payment
	@echo ""
	@echo "✓ All sink connectors registered"

register-sources: register-trading-source register-finance-source register-live-source register-chat-source register-performance-source register-concontrol-source register-claim-source register-payment-source
	@echo ""
	@echo "✓ All source connectors registered"

register-all:
	@echo "🔌 Deploying all connectors..."
	./scripts/deploy/deploy-connectors.sh

deploy:
	@make register-all
	@echo ""
	@echo "======================================"
	@echo "✅ CDC Pipeline connectors deployed!"
	@echo "======================================"
	@echo ""
	@echo "📊 Debezium Connect: $$DEBEZIUM_URL"
	@echo ""
	@echo "🔍 Check status with: make connectors"
	@echo ""

unregister-trading:
	@echo "Deleting Trading source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-trading-connector 2>/dev/null || true
	@echo "✓ Trading source deleted"
	@echo "Deleting Trading sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@echo "✓ Trading sink deleted"

unregister-finance:
	@echo "Deleting Finance source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-finance-connector 2>/dev/null || true
	@echo "✓ Finance source deleted"
	@echo "Deleting Finance sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-finance 2>/dev/null || true
	@echo "✓ Finance sink deleted"

unregister-live:
	@echo "Deleting Live source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-live-connector 2>/dev/null || true
	@echo "✓ Live source deleted"
	@echo "Deleting Live sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-live 2>/dev/null || true
	@echo "✓ Live sink deleted"

unregister-sinks:
	@echo "Deleting all sink connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@echo "✓ Trading sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-finance 2>/dev/null || true
	@echo "✓ Finance sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-live 2>/dev/null || true
	@echo "✓ Live sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-chat 2>/dev/null || true
	@echo "✓ Chat sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-performance 2>/dev/null || true
	@echo "✓ Performance sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-concontrol 2>/dev/null || true
	@echo "✓ Concontrol sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-claim 2>/dev/null || true
	@echo "✓ Claim sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-payment 2>/dev/null || true
	@echo "✓ Payment sink deleted"
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-connector 2>/dev/null || true
	@echo "✓ Generic sink deleted"
	@echo ""
	@echo "✅ All sink connectors deleted"

restart-sinks: unregister-sinks register-sink
	@echo ""
	@echo "✅ All sink connectors restarted"

unregister-chat:
	@echo "Deleting Chat source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-chat-connector 2>/dev/null || true
	@echo "✓ Chat source deleted"
	@echo "Deleting Chat sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-chat 2>/dev/null || true
	@echo "✓ Chat sink deleted"

unregister-performance:
	@echo "Deleting Performance source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-performance-connector 2>/dev/null || true
	@echo "✓ Performance source deleted"
	@echo "Deleting Performance sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-performance 2>/dev/null || true
	@echo "✓ Performance sink deleted"

unregister-concontrol:
	@echo "Deleting Concontrol source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-concontrol-connector 2>/dev/null || true
	@echo "✓ Concontrol source deleted"
	@echo "Deleting Concontrol sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-concontrol 2>/dev/null || true
	@echo "✓ Concontrol sink deleted"

unregister-claim:
	@echo "Deleting Claim source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-claim-connector 2>/dev/null || true
	@echo "✓ Claim source deleted"
	@echo "Deleting Claim sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-claim 2>/dev/null || true
	@echo "✓ Claim sink deleted"

unregister-payment:
	@echo "Deleting Payment source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-payment-connector 2>/dev/null || true
	@echo "✓ Payment source deleted"
	@echo "Deleting Payment sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-payment 2>/dev/null || true
	@echo "✓ Payment sink deleted"

unregister-all: unregister-trading unregister-finance unregister-live unregister-chat unregister-performance unregister-concontrol unregister-claim unregister-payment unregister-sinks
	@echo ""
	@echo "Deleting any remaining connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-global 2>/dev/null || true
	@echo "✓ All remaining connectors deleted"
	@echo ""
	@echo "✅ All connectors deleted"

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

restart-connector:
	@if [ -z "$(C)" ]; then \
		echo "Usage: make restart-connector C=<connector-name>"; \
		exit 1; \
	fi
	@echo "Restarting connector $(C)..."
	@curl -s -X POST $$DEBEZIUM_URL/connectors/$(C)/restart
	@echo ""
	@echo "✓ Connector restarted"

# Incremental snapshot testing (trading only - uses SAME Kafka topics as regular connector)
test-incremental-source:
	@echo "🧪 Registering Trading source with INCREMENTAL snapshot..."
	@echo "   Uses same Kafka topics: xchange_trading.xchange_trading.*"
	@echo "   Chunk size: 8192 rows"
	@echo ""
	@envsubst < connectors/sources/mariadb/trading-incremental.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Incremental source registered"
	@echo "📊 Monitor with: flyctl logs --app cdc-connector | grep snapshot"

test-incremental-full:
	@echo "🧪 Testing incremental snapshot: source → Kafka → sink"
	@echo ""
	@$(MAKE) test-incremental-source
	@echo ""
	@echo "⏳ Wait for snapshot to complete, then run:"
	@echo "   make register-sink-trading"
	@echo ""
	@echo "Monitor progress:"
	@echo "   flyctl logs --app cdc-connector | grep -E '(snapshot|rows)'"
	@echo "   make connector-status C=mariadb-trading-connector"

test-incremental-cleanup:
	@echo "🧹 Cleaning up incremental test..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-trading-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@echo "✓ Test connectors deleted"

register-one-schema-exp:
	@echo "🧪 One-schema experiment: 3 sources → 1 schema (xchange_finance)"
	@echo "   Tables will be renamed: database__table_name"
	@echo ""
	@echo "Registering Trading source..."
	@envsubst < connectors/sources/mariadb/trading.json | \
	curl -s -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "Registering Finance source..."
	@envsubst < connectors/sources/mariadb/finance.json | \
	curl -s -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "Registering Live source..."
	@envsubst < connectors/sources/mariadb/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "Registering global sink (3 sources → xchange_finance schema)..."
	@envsubst < connectors/sinks/postgres/global.json | \
	curl -s -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✅ 3 sources + 1 global sink registered!"
	@echo ""
	@echo "📊 Tables in xchange_finance schema:"
	@echo "   trading__*      (from xchange_trading)"
	@echo "   finance__*      (from xchange_finance)"
	@echo "   live__*         (from xchangelive)"
	@echo ""
	@echo "🔍 Check status: make connectors"

