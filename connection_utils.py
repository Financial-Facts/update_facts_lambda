import serviceConstants as const
import os
import boto3


def initialize_S3():
    return boto3.resource(
        service_name='s3',
        region_name=const.REGION,
        aws_access_key_id=os.environ[const.S3_KEY],
        aws_secret_access_key=os.environ[const.S3_SECRET_KEY])