terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

locals {
  resource_prefix = "${var.project}-${var.environment}"
}

module "tf_state" {
  source = "./state_setup"

  aws_region = var.aws_region
  project    = var.project
}
