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
docker compose --profile standard up --build -d
```

Open Grafana at http://localhost:3000 (admin/admin) and Prometheus at http://localhost:9090. The provisioned `Kafka .NET Client Metrics` dashboard has Producer and Consumer rows. Cloud credentials exist only in `.env`; Compose never passes them to Prometheus or Grafana.

Scale consumers with `docker compose --profile standard up -d --scale consumer-standard=2`. Prometheus discovers replicas through Docker DNS. With three default partitions, the two `Partitions assigned per consumer` series should sum to three after rebalance.

## Throughput demonstration

Run one profile at a time. Switch with `docker compose --profile standard down` (or the profile currently running), then `docker compose --profile high-throughput up --build -d` or `docker compose --profile throttle up --build -d`. For a fresh start use `docker compose --profile standard up --build -d`. The high-throughput profile requests 100 records/s of 128 KiB each, and throttle requests 200 records/s. Both use one customer key, sending traffic to one partition. The consumer reads the same Avro payload and logs every 100th record, preserving processing and offset storage for every record. The original three business fields remain; the optional `payload` bytes field carries random, poorly compressible demo data. Apply the updated Terraform Avro schema before starting either load profile.

Watch produced payload bytes/s, producer queue, consumer lag, and producer/consumer broker throttle time in Grafana. Confluent's Standard partition ingress guidance is approximately 5 MB/s, but actual throttling depends on cluster conditions and is not guaranteed at a specific rate. The throttle panels show the `brokers[*].throttle.avg` rolling-window value in milliseconds; zero means no throttle samples in that window. Generated traffic can incur Confluent Cloud charges. Stop the load profile after the demonstration.

Smoke test one `orderId`: find its producer `delivered` log line, then find the consumer line with the same ID. Partition and offset must match. In Prometheus, both producer and consumer targets should be UP and Grafana should show traffic.

The consumer stores each offset only after printing the record. A crash after processing but before the auto-commit can replay that record, which is at-least-once delivery. On a decode or processing error it stops rather than advancing past the failed offset.

Cleanup: `docker compose --profile <active-profile> down`, then `cd terraform && terraform destroy`. A Standard Confluent Cloud cluster incurs charges until it is destroyed.

## Validation

Run `dotnet build`, `terraform fmt -check`, `terraform init -backend=false`, `terraform validate`, `docker compose --env-file .env.example config --quiet`, and `docker compose build`. `terraform apply` and end-to-end Cloud traffic require your Confluent credentials.
