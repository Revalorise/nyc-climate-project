import boto3
import os

s3 = boto3.resource('s3')

data_directory_path = '../data'
data_directory = os.listdir(data_directory_path)


def upload_to_raw_bucket(data_directory):
    for data in data_directory:
        try:
            with open(data_directory_path + '/' + data, 'rb') as file:
                print('Uploading...')
                s3.Bucket('bon-raw-data-17835146').put_object(Key=data, Body=data)
        except FileNotFoundError:
            print(f"Error: File '{data}' not found. Please check the file path and try again.")
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Error uploading '{data}' to S3: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
        finally:
            print(f"'{data}' uploaded to S3.")


if __name__ == '__main__':
    upload_to_raw_bucket(data_directory)
