# CDC Pipeline Project

This directory contains a Change Data Capture (CDC) pipeline implementation.

## Project Overview

The CDC pipeline captures and processes data changes from source systems.

## Key Components

- Docker Compose setup for containerized services
- Finance data loading scripts (`load-finance.sh`)
- PostgreSQL database integration with pgloader
- MySQL database support

## Getting Started

### Prerequisites

- Docker and Docker Compose
- PostgreSQL
- MySQL

### Running the Pipeline

```bash
# Load finance data
./load-finance.sh

# Or using the bootstrap script
./bootstrap/pgloader/load-finance.sh
```

### Docker Services

Start services using Docker Compose:

```bash
docker compose up -d
```

View logs:

```bash
docker logs <container-name>
```

Execute commands in containers:

```bash
docker compose exec <service-name> <command>
docker exec <container-name> <command>
```

## Development

Environment variables can be configured in `.env` file.

```bash
source .env
```

## Build

```bash
make
```
