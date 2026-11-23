variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-central-1"
}

variable "environment_name" {
  description = "Short environment name used in resource names, e.g. dev or prod"
  type        = string
  default     = "dev"
}

variable "vpc_cidr_block" {
  description = "IP range of the VPC"
  type        = string
  default     = "10.40.0.0/16"
}

variable "worker_instance_type" {
  description = "EC2 type for the Kubernetes worker nodes"
  type        = string
  default     = "t3.large"
}

variable "worker_node_count" {
  description = "Desired number of worker nodes; the node group scales between 1 and 2x this"
  type        = number
  default     = 2
}

variable "database_instance_class" {
  description = "RDS instance size for the state database"
  type        = string
  default     = "db.t4g.micro"
}

