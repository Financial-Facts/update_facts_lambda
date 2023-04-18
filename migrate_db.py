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
from zipfile import ZipInfo
import shutil
from botocore.exceptions import ClientError
import file_processing_worker as fpw
import connection_utils as utils


MAX_NUMBER_OF_THREADS = 10


def __initialize_db() -> None:
    print("Initializing database connection..")
    connection = utils.get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(const.CREATE_FACTS_TABLE_QUERY)
    except (psycopg2.errors.InvalidSchemaName):
        cursor.execute(const.CREATE_SCHEMA_QUERY)
        cursor.execute(const.CREATE_FACTS_TABLE_QUERY)
    connection.commit()
    cursor.close()
    connection.close()


def __download_data() -> None:
    print("Downloading facts data from EDGAR...")
    req = Request(
        url=const.EDGAR_URL + const.DATA_ZIP_PATH,
        headers={'User-Agent': const.USER_AGENT_VALUE}
    )
    with urlopen(req) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            __divide_processing_workload(zfile)

def __divide_processing_workload(zfile: ZipFile):
    print("Dividing processing workload to workers...")
    files: list[ZipInfo] = zfile.filelist
    if (len(files) % MAX_NUMBER_OF_THREADS == 0):
        step = int(len(files)/MAX_NUMBER_OF_THREADS)
    else:
        step = int(len(files)/MAX_NUMBER_OF_THREADS) + 1
    threads = []
    file_batches = []
    for i in range(0, len(files), step):
        file_batches.append(files[i:i+step])
    for i in range(len(file_batches)):
        threads.append(fpw.file_processing_worker(
            threadID=i,
            name='file_processing_worker_%s' % i,
            counter=len(file_batches[i]),
            zip=zfile,
            files=files
        ))
        threads[int(i)].start()
    for t in threads:
        t.join()
    del (threads)


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


if __name__ == '__main__':

    startTime = time.time()

    __initialize_db()
    __download_data()

    end = time.time()

    print("""Sticker Price Database Population Complete\n
            Elapsed time: %2d minutes, %2d seconds\n"""
          % ((end - startTime)/60, (end - startTime) % 60))
