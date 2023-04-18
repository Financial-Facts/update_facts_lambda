import serviceConstants as const
import os
import psycopg2
import boto3


def get_db_auth() -> list[str]:
    try:
        db_username: str = os.environ[const.DB_USERNAME_KEY]
        db_password: str = os.environ[const.DB_PASSWORD_KEY]
        db_endpoint: str = os.environ[const.DB_ENDPOINT_KEY]
    except KeyError:
        raise Exception("Database credentials not provided")
    return [db_username, db_password, db_endpoint]

def get_db_connection():
    creds: list[str] = get_db_auth()
    conn = psycopg2.connect(
        host=creds[2], 
        port=const.PORT,
        database=const.DBNAME,
        user=creds[0],
        password=creds[1],
        sslrootcert='SSLCERTIFICATE',
        options="-c search_path=financial_facts",)
    return conn

def initialize_S3():
    return boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.environ[const.S3_KEY],
        aws_secret_access_key=os.environ[const.S3_SECRET_KEY])