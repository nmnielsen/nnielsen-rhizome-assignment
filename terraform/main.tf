terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
  backend "s3" {
    bucket = "rhizome-terraform-state"
    key    = "terraform.tfstate"
    region = "us-east-1"
    dynamodb_table = "rhizome-terraform-state-lock"
  }
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

module "ingest_observations" {
  source = "./ingest_observations"

  aws_region = var.aws_region
  project    = var.project
}

module "model_runner" {
  source = "./model_runner"

  aws_region = var.aws_region
  project    = var.project
}
