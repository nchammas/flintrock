terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 2"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 1"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}
