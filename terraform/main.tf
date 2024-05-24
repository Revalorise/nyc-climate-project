provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = "bon-raw-data-17835146"
  tags = {
    Name        = "NYC DE Project"
    Environment = "development"
  }
}

resource "aws_s3_bucket" "transformed_data_bucket" {
  bucket = "bon-transformed-data-17835146"
  tags = {
    Name        = "NYC DE Project"
    Environment = "development"
  }
}

resource "aws_s3_bucket" "serving_data_bucket" {
  bucket = "bon-serving-data-17835146"
  tags = {
    Name        = "NYC DE Project"
    Environment = "development"
  }
}
