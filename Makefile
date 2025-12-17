.PHONY: help start stop restart status logs register-trading register-finance register-live register-sink unregister-all connectors check-health clean

SHELL := /bin/bash
ENV_FILE := env/local.env

help:
	@echo "CDC Pipeline - Debezium + Redpanda (3 MariaDB sources)"
	@echo ""
	@echo "Setup:"
	@echo "  make start              - Start CDC stack (Redpanda + Debezium + Postgres)"
	@echo "  make stop               - Stop CDC stack"
	@echo "  make restart            - Restart CDC stack"
	@echo "  make clean              - Stop and remove all containers/volumes"
	@echo ""
	@echo "Connectors:"
	@echo "  make register-trading   - Register xchange_trading source"
	@echo "  make register-finance   - Register xchange_finance source"
	@echo "  make register-live      - Register xchangelive source"
	@echo "  make register-sink      - Register Postgres sink"
	@echo "  make register-all       - Register all 4 connectors"
	@echo "  make unregister-all     - Delete all connectors"
	@echo "  make connectors         - List all connectors and their status"
	@echo "  make check-health       - Check health of all services"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs               - Tail all container logs"
	@echo "  make logs-debezium      - Tail Debezium logs"
	@echo "  make logs-redpanda      - Tail Redpanda logs"
	@echo "  make console            - Open Redpanda Console (http://localhost:8080)"
	@echo ""
	@echo "Debugging:"
	@echo "  make topics             - List all Kafka topics"
	@echo "  make consume-topic T=<topic> - Consume messages from topic"
	@echo "  make connector-status C=<connector-name> - Get connector status"

start:
	@echo "Starting CDC stack..."
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "Error: $(ENV_FILE) not found."; \
		exit 1; \
	fi
	docker compose -f docker/docker-compose.yml up -d redpanda console debezium postgres
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@make check-health

stop:
	docker compose -f docker/docker-compose.yml down

restart: stop start

clean:
	docker compose -f docker/docker-compose.yml down -v

status:
	@docker compose -f docker/docker-compose.yml ps

logs:
	docker compose -f docker/docker-compose.yml logs -f

logs-debezium:
	docker compose -f docker/docker-compose.yml logs -f debezium

logs-redpanda:
	docker compose -f docker/docker-compose.yml logs -f redpanda

check-health:
	@echo "Checking service health..."
	@echo -n "Redpanda: "
	@curl -sf http://localhost:9644/v1/cluster/health_overview > /dev/null && echo "✓ healthy" || echo "✗ unhealthy"
	@echo -n "Debezium: "
	@curl -sf http://localhost:8083/ > /dev/null && echo "✓ healthy" || echo "✗ unhealthy"
	@echo -n "Postgres: "
	@docker compose -f docker/docker-compose.yml exec -T postgres pg_isready -U postgres > /dev/null 2>&1 && echo "✓ healthy" || echo "✗ unhealthy"

register-trading:
	@echo "Registering xchange_trading source connector..."
	@set -a; source $(ENV_FILE); set +a; \
	envsubst < connectors/sources/mariadb/trading.json | \
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Trading source registered"

register-finance:
	@echo "Registering xchange_finance source connector..."
	@set -a; source $(ENV_FILE); set +a; \
	envsubst < connectors/sources/mariadb/finance.json | \
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Finance source registered"

register-live:
	@echo "Registering xchangelive source connector..."
	@set -a; source $(ENV_FILE); set +a; \
	envsubst < connectors/sources/mariadb/live.json | \
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Live source registered"

register-sink:
	@echo "Registering Postgres sink connector..."
	@set -a; source $(ENV_FILE); set +a; \
	envsubst < connectors/sinks/postgres/sink.json | \
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @- | jq .
	@echo ""
	@echo "✓ Sink connector registered"

register-all:
	@echo "🔌 Deploying all connectors..."
	./scripts/deploy/deploy-connectors.sh

migrate:
	@echo "📦 Running pgloader migration..."
	./bootstrap/pgloader/load-all.sh
	@echo "✅ Migration completed"

add-pks:
	@echo "🔑 Adding primary keys to all tables..."
	docker compose -f docker/docker-compose.yml exec -T postgres psql -U postgres -d cdc_pipeline -f - < bootstrap/sql/add-primary-keys.sql
	@echo "✅ Primary keys added"

deploy: start
	@echo ""
	@echo "⏳ Waiting for Postgres to be ready..."
	@sleep 15
	@echo ""
	@make migrate
	@echo ""
	@make add-pks
	@echo ""
	@make register-all
	@echo ""
	@echo "======================================"
	@echo "✅ Full CDC Pipeline deployment complete!"
	@echo "======================================"
	@echo ""
	@echo "📊 Service URLs:"
	@echo "   Redpanda Console:  http://localhost:8080"
	@echo "   Debezium Connect:  http://localhost:8083"
	@echo "   Postgres:          localhost:5432"
	@echo ""
	@echo "🔍 Check status with: make connectors"
	@echo ""

unregister-all:
	@echo "Deleting all connectors..."
	@curl -s -X DELETE http://localhost:8083/connectors/mariadb-trading-connector 2>/dev/null || true
	@curl -s -X DELETE http://localhost:8083/connectors/mariadb-finance-connector 2>/dev/null || true
	@curl -s -X DELETE http://localhost:8083/connectors/mariadb-live-connector 2>/dev/null || true
	@curl -s -X DELETE http://localhost:8083/connectors/postgres-sink-xchangelive 2>/dev/null || true
	@curl -s -X DELETE http://localhost:8083/connectors/postgres-sink-trading 2>/dev/null || true
	@curl -s -X DELETE http://localhost:8083/connectors/postgres-sink-finance 2>/dev/null || true
	@echo "✓ All connectors deleted"

connectors:
	@echo "Registered connectors:"
	@curl -s http://localhost:8083/connectors | jq -r '.[]' | while read c; do \
		echo ""; \
		echo "$$c:"; \
		curl -s http://localhost:8083/connectors/$$c/status | jq '{state: .connector.state, tasks: [.tasks[].state]}'; \
	done

connector-status:
	@if [ -z "$(C)" ]; then \
		echo "Usage: make connector-status C=<connector-name>"; \
		exit 1; \
	fi
	@curl -s http://localhost:8083/connectors/$(C)/status | jq .

topics:
	@docker compose -f docker/docker-compose.yml exec -T redpanda rpk topic list

consume-topic:
	@if [ -z "$(T)" ]; then \
		echo "Usage: make consume-topic T=<topic-name>"; \
		exit 1; \
	fi
	@docker compose -f docker/docker-compose.yml exec redpanda rpk topic consume $(T) --format json | jq .

console:
	@echo "Opening Redpanda Console at http://localhost:8080"
	@open http://localhost:8080 || xdg-open http://localhost:8080 || echo "Visit: http://localhost:8080"
