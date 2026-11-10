terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.61.0"
    }
  }
}

provider "confluent" {}

resource "confluent_environment" "bsnir-main-dot-net-demo" {
  display_name = "bsnir-main-dot-net-demo"
  stream_governance { package = "ESSENTIALS" }
}

resource "confluent_kafka_cluster" "standard" {
  display_name = "orders-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.region
  standard {}
  environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
}

data "confluent_schema_registry_cluster" "bsnir-main" {
  environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
}

resource "confluent_kafka_topic" "orders" {
  kafka_cluster { id = confluent_kafka_cluster.standard.id }
  topic_name       = "orders"
  partitions_count = var.partitions
  rest_endpoint    = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.topic_provisioner_kafka.id
    secret = confluent_api_key.topic_provisioner_kafka.secret
  }
  depends_on = [confluent_role_binding.topic_provisioner_environment_admin]
}

resource "confluent_service_account" "producer" { display_name = "orders-producer" }
resource "confluent_service_account" "consumer" { display_name = "orders-consumer" }
resource "confluent_service_account" "schema_provisioner" { display_name = "orders-schema-provisioner" }
resource "confluent_service_account" "topic_provisioner" { display_name = "orders-topic-provisioner" }

resource "confluent_role_binding" "producer_developer_write" {
  principal   = "User:${confluent_service_account.producer.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/topic=orders"
}

resource "confluent_role_binding" "producer_idempotent_write" {
  principal   = "User:${confluent_service_account.producer.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/transactional-id=*"
}

resource "confluent_role_binding" "consumer_read" {
  principal   = "User:${confluent_service_account.consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/topic=orders"
}

resource "confluent_role_binding" "consumer_group" {
  principal   = "User:${confluent_service_account.consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.standard.rbac_crn}/kafka=${confluent_kafka_cluster.standard.id}/group=orders-demo"
}

resource "confluent_role_binding" "producer_schema" {
  principal   = "User:${confluent_service_account.producer.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.bsnir-main.resource_name}/subject=orders-value"
}

resource "confluent_role_binding" "consumer_schema" {
  principal   = "User:${confluent_service_account.consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.bsnir-main.resource_name}/subject=orders-value"
}

resource "confluent_role_binding" "schema_provisioner" {
  principal   = "User:${confluent_service_account.schema_provisioner.id}"
  role_name   = "DataSteward"
  crn_pattern = confluent_environment.bsnir-main-dot-net-demo.resource_name
}

resource "confluent_role_binding" "topic_provisioner_environment_admin" {
  principal   = "User:${confluent_service_account.topic_provisioner.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.bsnir-main-dot-net-demo.resource_name
}

resource "confluent_api_key" "producer_kafka" {
  display_name = "producer-kafka"
  owner {
    id          = confluent_service_account.producer.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = "cmk/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_api_key" "consumer_kafka" {
  display_name = "consumer-kafka"
  owner {
    id          = confluent_service_account.consumer.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = "cmk/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_api_key" "topic_provisioner_kafka" {
  display_name = "topic-provisioner-kafka"
  owner {
    id          = confluent_service_account.topic_provisioner.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = "cmk/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_api_key" "producer_schema" {
  display_name = "producer-schema"
  owner {
    id          = confluent_service_account.producer.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.bsnir-main.id
    api_version = "srcm/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_api_key" "consumer_schema" {
  display_name = "consumer-schema"
  owner {
    id          = confluent_service_account.consumer.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.bsnir-main.id
    api_version = "srcm/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_api_key" "schema_provisioner" {
  display_name = "schema-provisioner"
  owner {
    id          = confluent_service_account.schema_provisioner.id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.bsnir-main.id
    api_version = "srcm/v2"
    kind        = "Cluster"
    environment { id = confluent_environment.bsnir-main-dot-net-demo.id }
  }
}

resource "confluent_schema" "order" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.bsnir-main.id }
  rest_endpoint = data.confluent_schema_registry_cluster.bsnir-main.rest_endpoint
  subject_name  = "orders-value"
  format        = "AVRO"
  schema        = file("${path.module}/../schemas/order.avsc")
  credentials {
    key    = confluent_api_key.schema_provisioner.id
    secret = confluent_api_key.schema_provisioner.secret
  }
  depends_on = [confluent_role_binding.schema_provisioner]
}
