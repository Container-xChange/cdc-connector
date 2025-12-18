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
	@echo ""
	@echo "Connectors:"
	@echo "  make register-trading   - Register xchange_trading source"
	@echo "  make register-finance   - Register xchange_finance source"
	@echo "  make register-live      - Register xchangelive source"
	@echo "  make register-sink      - Register Postgres sink"
	@echo "  make register-all       - Register all connectors (uses deploy-connectors.sh)"
	@echo "  make unregister-all     - Delete all connectors"
	@echo "  make connectors         - List all connectors and their status"
	@echo "  make check-health       - Check Debezium health"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy             - Register all CDC connectors"
	@echo ""
	@echo "Debugging:"
	@echo "  make connector-status C=<connector-name> - Get connector status"

migrate:
	@echo "🔄 Building pgloader image..."
	docker build -t pgloader-cdc -f bootstrap/Dockerfile bootstrap/
	@echo ""
	@echo "🗄️  Running pgloader migration (load-all.sh)..."
	docker run --rm --env-file .env pgloader-cdc
	@echo ""
	@echo "✅ Migration completed!"

check-health:
	@echo "Checking Debezium health..."
	@curl -sf $$DEBEZIUM_URL/ > /dev/null && echo "✓ Debezium healthy" || echo "✗ Debezium unhealthy"

register-trading:
	@echo "Registering xchange_trading source connector..."
	@envsubst < connectors/sources/mariadb/trading.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Trading source registered"

register-finance:
	@echo "Registering xchange_finance source connector..."
	@envsubst < connecto/sources/mariadb/finance.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Finance source registered"

register-live:
	@echo "Registering xchangelive source connector..."
	@envsubst < connectors/sources/mariadb/live.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Live source registered"

register-sink:
	@echo "Registering Postgres sink connector..."
	@envsubst < connectors/sinks/postgres/sink.json | \
	curl -X POST $$DEBEZIUM_URL/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Sink connector registered"

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

unregister-all:
	@echo "Deleting all connectors..."
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-trading-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-finance-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/mariadb-live-connector 2>/dev/null || true
	@curl -s -X DELETE $$DEBEZIUM_URL/connectors/postgres-sink-connector 2>/dev/null || true
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

