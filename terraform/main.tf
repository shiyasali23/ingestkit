terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project     = "ingestkit"
      environment = var.environment_name
      managed_by  = "terraform"
    }
  }
}

data "aws_availability_zones" "available_zones" {
  state = "available"
}

locals {
  name_prefix        = "ingestkit-${var.environment_name}"
  availability_zones = slice(data.aws_availability_zones.available_zones.names, 0, 2)
}

module "network_vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name               = "${local.name_prefix}-vpc"
  cidr               = var.vpc_cidr_block
  azs                = local.availability_zones
  private_subnets    = [for zone_index in range(2) : cidrsubnet(var.vpc_cidr_block, 4, zone_index)]
  public_subnets     = [for zone_index in range(2) : cidrsubnet(var.vpc_cidr_block, 4, zone_index + 8)]
  enable_nat_gateway = true
  single_nat_gateway = true
}
