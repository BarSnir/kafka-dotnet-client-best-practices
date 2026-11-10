variable "region" {
  type    = string
  default = "us-east-2"
}

variable "partitions" {
  type    = number
  default = 3
}

variable "cloud_api_key" {
  type      = string
  sensitive = true
}

variable "cloud_api_secret" {
  type      = string
  sensitive = true
}
