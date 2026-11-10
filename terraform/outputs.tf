output "producer_kafka_bootstrap_servers" {
  value     = confluent_kafka_cluster.standard.bootstrap_endpoint
  sensitive = true
}
output "producer_kafka_api_key" {
  value     = confluent_api_key.producer_kafka.id
  sensitive = true
}
output "producer_kafka_api_secret" {
  value     = confluent_api_key.producer_kafka.secret
  sensitive = true
}
output "producer_schema_registry_url" {
  value     = data.confluent_schema_registry_cluster.bsnir-main.rest_endpoint
  sensitive = true
}
output "producer_schema_registry_api_key" {
  value     = confluent_api_key.producer_schema.id
  sensitive = true
}
output "producer_schema_registry_api_secret" {
  value     = confluent_api_key.producer_schema.secret
  sensitive = true
}
output "consumer_kafka_bootstrap_servers" {
  value     = confluent_kafka_cluster.standard.bootstrap_endpoint
  sensitive = true
}
output "consumer_kafka_api_key" {
  value     = confluent_api_key.consumer_kafka.id
  sensitive = true
}
output "consumer_kafka_api_secret" {
  value     = confluent_api_key.consumer_kafka.secret
  sensitive = true
}
output "consumer_schema_registry_url" {
  value     = data.confluent_schema_registry_cluster.bsnir-main.rest_endpoint
  sensitive = true
}
output "consumer_schema_registry_api_key" {
  value     = confluent_api_key.consumer_schema.id
  sensitive = true
}
output "consumer_schema_registry_api_secret" {
  value     = confluent_api_key.consumer_schema.secret
  sensitive = true
}
output "topic_provisioner_kafka_api_key" {
  value     = confluent_api_key.topic_provisioner_kafka.id
  sensitive = true
}
output "topic_provisioner_kafka_api_secret" {
  value     = confluent_api_key.topic_provisioner_kafka.secret
  sensitive = true
}
output "schema_provisioner_api_key" {
  value     = confluent_api_key.schema_provisioner.id
  sensitive = true
}
output "schema_provisioner_api_secret" {
  value     = confluent_api_key.schema_provisioner.secret
  sensitive = true
}
