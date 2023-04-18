import psycopg2
import time
import json
import serviceConstants as const
from io import BytesIO
from urllib.request import Request, urlopen
from zipfile import ZipFile
from zipfile import ZipInfo
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


def __download_and_process_data() -> None:
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
    s3Client = utils.initialize_S3()
    for i in range(0, len(files), step):
        file_batches.append(files[i:i+step])
    for i in range(len(file_batches)):
        threads.append(fpw.file_processing_worker(
            threadID=i,
            name='file_processing_worker_%s' % i,
            counter=len(file_batches[i]),
            zip=zfile,
            files=file_batches[i],
            s3=s3Client
        ))
        print("Starting worker %s..." % i)
        threads[int(i)].start()
    for t in threads:
        t.join()
    del (threads)


if __name__ == '__main__':

    startTime = time.time()

    __initialize_db()
    __download_and_process_data()

    end = time.time()

    print("""Sticker Price Database Population Complete\n
            Elapsed time: %2d minutes, %2d seconds\n"""
          % ((end - startTime)/60, (end - startTime) % 60))
