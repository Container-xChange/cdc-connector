.PHONY: help register-trading register-finance register-live register-sink unregister-all connectors check-health deploy connector-status

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
	@echo "  make generate-connectors    - Generate connector configs from templates"
	@echo "  make register-trading       - Register Trading source + sink"
	@echo "  make register-finance       - Register Finance source + sink"
	@echo "  make register-live          - Register Live source + sink"
	@echo "  make register-sources       - Register all source connectors only"
	@echo "  make register-sink          - Register all sink connectors only"
	@echo "  make register-all           - Register all connectors (uses deploy-connectors.sh)"
	@echo "  make unregister-trading     - Delete Trading source + sink"
	@echo "  make unregister-finance     - Delete Finance source + sink"
	@echo "  make unregister-live        - Delete Live source + sink"
	@echo "  make unregister-all         - Delete all connectors"
	@echo "  make connectors             - List all connectors and their status"
	@echo "  make check-health           - Check Debezium health"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy             - Register all CDC connectors"
	@echo ""
	@echo "Debugging:"
	@echo "  make connector-status C=<connector-name> - Get connector status"

generate-connectors:
	@echo "🔧 Generating connector configurations from templates..."
	@bash scripts/generate-connectors.sh

migrate:
	@echo "🗄️  Installing pgloader via Homebrew if not present..."
	@which pgloader > /dev/null || brew install pgloader
	@echo ""
	@echo "🗄️  Running pgloader migration (all databases)..."
	cd bootstrap/pgloader && bash load-all.sh
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
	@envsubst < connectors/sources/mariadb/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Live source registered"

register-sink-trading:
	@echo "Registering Postgres Trading sink connector..."
	@envsubst < connectors/sinks/postgres/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Trading sink registered"

register-sink-finance:
	@echo "Registering Postgres Finance sink connector..."
	@envsubst < connectors/sinks/postgres/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Finance sink registered"

register-sink-live:
	@echo "Registering Postgres Live sink connector..."
	@envsubst < connectors/sinks/postgres/live.json | \
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

register-sink: register-sink-trading register-sink-live
	@echo ""
	@echo "✓ All sink connectors registered"

register-sources: register-trading-source register-finance-source register-live-source
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

unregister-finance:
	@echo "Deleting Finance source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-finance-connector 2>/dev/null || true
	@echo "✓ Finance source deleted"

unregister-live:
	@echo "Deleting Live source connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-live-connector 2>/dev/null || true
	@echo "✓ Live source deleted"

unregister-sink:
	@echo "Deleting sink connector..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-connector 2>/dev/null || true
	@echo "✓ Sink connector deleted"

unregister-all: unregister-trading unregister-finance unregister-live unregister-sink
	@echo ""
	@echo "Deleting any remaining connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-trading 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-finance 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-live 2>/dev/null || true
	@echo ""
	@echo "✓ All connectors deleted"

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

