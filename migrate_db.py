import os
import sys
import psycopg2
import time
import json
import serviceConstants as const
from os import listdir
from io import BytesIO
from urllib.request import Request, urlopen
from zipfile import ZipFile
import shutil
import boto3
from botocore.exceptions import ClientError
import threading


MAX_CONNECTION_REESTABLISH_ATTEMPTS = 5
BUCKET_NAME = 'public-financial-facts-bucket-dev-1'

def __initialize_S3() -> None:
    return boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.environ[const.S3_KEY],
        aws_secret_access_key=os.environ[const.S3_SECRET_KEY])

def __get_db_auth() -> list[str]:
    try:
        db_username: str = os.environ[const.DB_USERNAME_KEY],
        db_password: str = os.environ[const.DB_PASSWORD_KEY]
        db_endpoint: str = os.environ[const.DB_ENDPOINT_KEY]
    except KeyError:
        raise Exception("Database credentials not provided")
    return [db_username[0], db_password, db_endpoint]


def __get_db_connection():
    creds: list[str] = __get_db_auth()
    conn = psycopg2.connect(
        host=creds[2], 
        port=const.PORT,
        database=const.DBNAME,
        user=creds[0],
        password=creds[1],
        sslrootcert='SSLCERTIFICATE',
        options="-c search_path=financial_facts",)
    return conn


def __initialize_db() -> None:
    print("Initializing database connection..")
    connection = __get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(const.CREATE_FACTS_TABLE_QUERY)
    except (psycopg2.errors.InvalidSchemaName):
        cursor.execute(const.CREATE_SCHEMA_QUERY)
        cursor.execute(const.CREATE_FACTS_TABLE_QUERY)
    connection.commit()
    cursor.close()
    connection.close()


def __drop_facts() -> None:
    print("Dropping facts table from database...")
    connection = __get_db_connection()
    cursor = connection.cursor()
    cursor.execute(const.DROP_FACTS_TABLE_QUERY)
    connection.commit()
    cursor.close()
    connection.close()


def __delete_data() -> None:
    print("Deleting current facts data...")
    shutil.rmtree('Data')
    shutil.rmtree('Temp')


def __download_data() -> None:
    print("Downloading facts data from EDGAR...")
    try:
        # os.mkdir('Temp')
        pass
    except FileExistsError:
        # Process already started but not finished...
        print("Continuing with current temp folder...")
        return
    req = Request(
        url=const.EDGAR_URL + const.DATA_ZIP_PATH,
        headers={'User-Agent': const.USER_AGENT_VALUE}
    )
    with urlopen(req) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            __process_data(zfile)


def __process_data(zfile: ZipFile) -> list:
    print("Processing data updates..")
    s3 = __initialize_S3()
    fileAdded = False
    for file in zfile.filelist:
        cik = file.filename[:-5]
        try:
            temp = json.loads(zfile.read(file))
            try:
                data = json.loads(s3.Bucket(BUCKET_NAME).Object(file.filename).get()['Body'].read().decode())
                if (
                    temp != data
                ):
                    print("Updating %s..." % cik)
                    __attempt_update(cik, temp)
                    fileAdded = True
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    print("Adding %s..." % cik)
                    __attempt_insert(cik, temp)
                    fileAdded = True
                else:
                    print(ex)
        except json.decoder.JSONDecodeError:
            print("Cannot process %s" % cik)
        if (fileAdded):
            fileAdded = False
            object = s3.Object(BUCKET_NAME, file.filename)
            object.put(Body=json.dumps(temp))

def __attempt_insert(cik, data) -> None:
    connection = __get_db_connection()
    insertComplete = False
    retryAttempts = MAX_CONNECTION_REESTABLISH_ATTEMPTS
    with connection.cursor() as cursor:
        while (not insertComplete and retryAttempts >= 0):
            try:
                __insert_database(data, cik, cursor)
                insertComplete = True
            except psycopg2.errors.InFailedSqlTransaction:
                print("%s already exists within database..." % cik)
                insertComplete = True
            except psycopg2.OperationalError:
                connection = __get_db_connection()
                cursor = connection.cursor()
    cursor.close()
    connection.close()


def __attempt_update(cik, data) -> None:
    connection = __get_db_connection()
    updateComplete = False
    retryAttempts = MAX_CONNECTION_REESTABLISH_ATTEMPTS
    with connection.cursor() as cursor:
        while (not updateComplete and retryAttempts >= 5):
            try:
                __update_database(data, cik, cursor)
                updateComplete = True
            except psycopg2.OperationalError:
                connection = __get_db_connection()
                cursor = connection.cursor()
    cursor.close()
    connection.close()


def __update_database(data, cik, cursor) -> None:
    try:
        text = json.dumps(data)
        text = text.replace('\'', const.EMPTY)
        try:
            cursor.execute(const.UPDATE_DATA_QUERY % (
                text,
                cik
            ))
        except psycopg2.errors.UniqueViolation:
            pass
    except OSError:
        pass


def __insert_database(data, cik, cursor) -> None:
    try:
        text = json.dumps(data)
        text = text.replace('\'', const.EMPTY)
        try:
            cursor.execute(const.INSERT_DATA_QUERY % (
                cik,
                text
            ))
        except psycopg2.errors.UniqueViolation:
            pass
    except OSError:
        pass


if __name__ == '__main__':

    startTime = time.time()

    print("Checking repopulation flag status...")
    try:
        repopulateFlag = sys.argv[1]
        if (repopulateFlag == const.TRUE):
            repopulateFlag = True
        else:
            repopulateFlag = False
    except IndexError:
        repopulateFlag = False

    if (repopulateFlag):
        __drop_facts()
        __delete_data()

    __initialize_db()
    __download_data()

    end = time.time()

    print("""Sticker Price Database Population Complete\n
            Elapsed time: %2d minutes, %2d seconds\n"""
          % ((end - startTime)/60, (end - startTime) % 60))
