#!/bin/bash
set -e

# Detect if running in Docker or locally
if [ -d "/kafka/connectors" ]; then
  BASE_DIR="/kafka"
else
  BASE_DIR="."
fi

CONFIG_FILE="$BASE_DIR/connectors/config.json"
SOURCE_TEMPLATE="$BASE_DIR/connectors/sources/mariadb/source.template.json"
SINK_TEMPLATE="$BASE_DIR/connectors/sinks/postgres/sink.template.json"

echo "🔧 Generating connector configurations from templates..."
echo "📁 Base directory: $BASE_DIR"

# Create output directories if they don't exist
mkdir -p "$BASE_DIR/connectors/sources/mariadb"
mkdir -p "$BASE_DIR/connectors/sinks/postgres"

# Generate source connectors
echo "📥 Generating source connectors..."
jq -r '.sources[] | @json' "$CONFIG_FILE" | while read -r source; do
  name=$(echo "$source" | jq -r '.name')
  database=$(echo "$source" | jq -r '.database')
  env_prefix=$(echo "$source" | jq -r '.env_prefix')

  output_file="$BASE_DIR/connectors/sources/mariadb/${name}.json"

  echo "  - Creating $output_file"

  sed -e "s/{{NAME}}/$name/g" \
      -e "s/{{DATABASE}}/$database/g" \
      -e "s/{{ENV_PREFIX}}/$env_prefix/g" \
      "$SOURCE_TEMPLATE" > "$output_file"
done

# Generate sink connectors
echo "📤 Generating sink connectors..."
jq -r '.sinks[] | @json' "$CONFIG_FILE" | while read -r sink; do
  name=$(echo "$sink" | jq -r '.name')
  schema=$(echo "$sink" | jq -r '.schema')
  topic_pattern=$(echo "$sink" | jq -r '.topic_pattern')

  output_file="$BASE_DIR/connectors/sinks/postgres/${name}.json"

  echo "  - Creating $output_file"

  sed -e "s/{{NAME}}/$name/g" \
      -e "s/{{SCHEMA}}/$schema/g" \
      -e "s/{{TOPIC_PATTERN}}/$topic_pattern/g" \
      "$SINK_TEMPLATE" > "$output_file"
done

echo "✅ Connector configurations generated successfully!"
