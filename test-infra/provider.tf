terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}
