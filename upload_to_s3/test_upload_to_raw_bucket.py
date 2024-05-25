import os
import boto3
import pytest

from moto import mock_aws
from main import upload_to_raw_bucket


@mock_aws
def test_upload_to_raw_bucket_success():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bon-raw-data-test-17835146")

    data_directory_path = "../data"
    data_directory = os.listdir(data_directory_path)
    bucket_name = "bon-raw-data-test-17835146"

    upload_to_raw_bucket(data_directory)

    for data in data_directory:
        assert s3.get_object(Bucket=bucket_name, Key=data)


@mock_aws
def test_upload_to_raw_bucket_file_not_found():
    s3 = boto3.client('s3', region_name="us-east-1")
    s3.create_bucket(Bucket="bon-raw-data-test-17835146")

    data_directory_path = "tests/data"
    data_directory = os.listdir(data_directory_path)

    with pytest.raises(FileNotFoundError):
        upload_to_raw_bucket(data_directory)
