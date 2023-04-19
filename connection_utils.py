import serviceConstants as const
import os
import psycopg2
import boto3
from base64 import b64decode

def decrypt_env_variable(encrypted: str):
    return boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')

def get_db_auth() -> list[str]:
    try:
        db_username: str = decrypt_env_variable(os.environ[const.DB_USERNAME_KEY])
        db_password: str = decrypt_env_variable(os.environ[const.DB_PASSWORD_KEY])
        db_endpoint: str = decrypt_env_variable(os.environ[const.DB_ENDPOINT_KEY])
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
        aws_access_key_id=decrypt_env_variable(os.environ[const.S3_KEY]),
        aws_secret_access_key=decrypt_env_variable(os.environ[const.S3_SECRET_KEY]))