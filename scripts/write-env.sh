#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../terraform"
umask 077
temp_env=$(mktemp ../.env.XXXXXX)
trap 'rm -f "$temp_env"' EXIT

for name in producer_kafka_bootstrap_servers producer_kafka_api_key producer_kafka_api_secret producer_schema_registry_url producer_schema_registry_api_key producer_schema_registry_api_secret consumer_kafka_bootstrap_servers consumer_kafka_api_key consumer_kafka_api_secret consumer_schema_registry_url consumer_schema_registry_api_key consumer_schema_registry_api_secret; do
  value=$(terraform output -raw "$name")
  if [[ -z "$value" ]]; then
    printf 'Terraform output %s is empty. Run terraform apply successfully first.\n' "$name" >&2
    exit 1
  fi
  env_name=$(printf '%s' "$name" | tr '[:lower:]' '[:upper:]')
  printf '%s=%s\n' "$env_name" "${value#SASL_SSL://}" >> "$temp_env"
done
chmod 600 "$temp_env"
mv "$temp_env" ../.env