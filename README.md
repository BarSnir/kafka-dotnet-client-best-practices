# Confluent Cloud .NET best practices

Production-minded .NET 10 Kafka producer and consumer samples using Avro, Confluent Cloud, and librdkafka statistics in Grafana.

## Run

Prerequisites: .NET 10 SDK, Terraform, Docker Compose, and a Confluent Cloud API key/secret with provisioning privileges. Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and set those two values. Runtime service accounts are created separately and receive only topic, group, and schema-subject permissions.

```bash
cd terraform
terraform init
terraform plan
terraform apply
cd ..
./scripts/write-env.sh
docker compose up --build -d
```

Open Grafana at http://localhost:3000 (admin/admin) and Prometheus at http://localhost:9090. The provisioned `Kafka .NET Client Metrics` dashboard has Producer and Consumer rows. Cloud credentials exist only in `.env`; Compose never passes them to Prometheus or Grafana.

Scale consumers with `docker compose up -d --scale consumer=2`. Prometheus discovers replicas through Docker DNS. With three default partitions, the two `Partitions assigned per consumer` series should sum to three after rebalance.

Smoke test one `orderId`: find its producer `delivered` log line, then find the consumer line with the same ID. Partition and offset must match. In Prometheus, both producer and consumer targets should be UP and Grafana should show traffic.

The consumer stores each offset only after printing the record. A crash after processing but before the auto-commit can replay that record, which is at-least-once delivery. On a decode or processing error it stops rather than advancing past the failed offset.

Cleanup: `docker compose down`, then `cd terraform && terraform destroy`. A Standard Confluent Cloud cluster incurs charges until it is destroyed.

## Validation

Run `dotnet build`, `terraform fmt -check`, `terraform init -backend=false`, `terraform validate`, `docker compose --env-file .env.example config --quiet`, and `docker compose build`. `terraform apply` and end-to-end Cloud traffic require your Confluent credentials.