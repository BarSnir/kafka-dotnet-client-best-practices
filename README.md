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

## Customer demo stages

From the repository root with the runtime .env configured, run:

```sh
sh scripts/01-monitoring.sh
sh scripts/02-standard.sh
# Ctrl+C, then
sh scripts/03-high-throughput.sh
# Ctrl+C, then
sh scripts/04-throttle.sh
```

Monitoring runs in the background. Each workload script prints stage status and producer throughput, forwards client stderr, and hides per-record stdout and successful Docker build output. Ctrl+C stops and removes that stage's clients. The next stage reuses monitoring and its history. Each producer reports payload_MB/s once after startup, using the next nonempty sampling window after its first deliveries. Prometheus continues scraping throughout the run. Grafana legends use the client ID, composed of service name and container hostname.

## Throughput demonstration

Start standard and high-throughput together:

```bash
docker compose --profile standard --profile high-throughput up --build -d
```

Each Kafka client ID includes its Compose service name and container hostname. All producers write to orders. Consumers share the orders-demo group and divide its partitions. The high-throughput producer targets 5 MB/s of payload (100 records/s × 50,000 bytes). The high-throughput producer uses one customer key. The throttle producer discovers the orders partitions at startup and sends directly to each in rotation, targeting 7 Mbps (0.875 MB/s) of generated payload per partition. Override the target with `THROTTLE_PARTITION_MBPS`, in decimal megabits/s.

On startup each producer reads the latest orders-value writer schema from the configured registry. With the four-field schema it puts the random bytes in payload. With the original three-field schema it carries the same bytes in a demo-payload Kafka header. Terraform provisions the subject before startup.

Each service declares its workload in compose.yaml. In the throttle profile, compare **Produced MB/s by partition** with **Producer recent broker throttle (ms)**. A positive broker throttle time confirms throttling during that sample window.

Watch produced payload bytes/s, producer queue, consumer lag, and producer/consumer broker throttle time in Grafana. Confluent's Standard partition ingress guidance is approximately 5 MB/s, but actual throttling depends on cluster conditions and is not guaranteed at a specific rate. The throttle panels retain the peak delay from the most recent positive broker statistics interval for 60 seconds so short events survive Prometheus scrapes. A delivery timeout alone does not confirm throttling. Compare **Producer network send rate (MB/s)** for the throttle client with aggregate traffic across partitions. Use **Acknowledged generated payload by partition (Mbps)** to check delivery against the per-partition target, including payload carried in headers. This rate includes headers, retries and protocol overhead. The startup payload reading is a single message-byte sample. Generated traffic can incur Confluent Cloud charges. Stop the load profile after the demonstration.

Smoke test one `orderId`: find its producer `delivered` log line, then find the consumer line with the same ID. Partition and offset must match. In Prometheus, both producer and consumer targets should be UP and Grafana should show traffic.

The consumer stores each offset only after printing the record. A crash after processing but before the auto-commit can replay that record, which is at-least-once delivery. On a decode or processing error it stops rather than advancing past the failed offset.

Cleanup: `docker compose --profile <active-profile> down`, then `cd terraform && terraform destroy`. A Standard Confluent Cloud cluster incurs charges until it is destroyed.

## Validation

Run `dotnet build`, `terraform fmt -check`, `terraform init -backend=false`, `terraform validate`, `docker compose --env-file .env.example config --quiet`, and `docker compose build`. `terraform apply` and end-to-end Cloud traffic require your Confluent credentials.
