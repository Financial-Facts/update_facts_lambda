import psycopg2
import time
import os
import serviceConstants as const
from io import BytesIO
from urllib.request import Request, urlopen
from zipfile import ZipFile
from zipfile import ZipInfo
import file_processing_worker as fpw
import connection_utils as utils
from queue import Queue


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


def __download_and_process_data(number_of_chunks: int, chunk_to_process: int) -> None:
    print("Downloading facts data from EDGAR...")
    req = Request(
        url=const.EDGAR_URL + const.DATA_ZIP_PATH,
        headers={
            'User-Agent': os.environ[const.USER_AGENT_VALUE_KEY],
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
    )
    with urlopen(req) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            __divide_processing_workload(number_of_chunks, chunk_to_process, zfile)


def __divide_processing_workload(number_of_chunks: int, chunk_to_process: int, zfile: ZipFile):
    print("Dividing processing workload to workers...")
    files: list[ZipInfo] = zfile.filelist
    queue = Queue()
    chunks = __separate_into_chunks(files, number_of_chunks)
    for file in chunks[chunk_to_process]:
        queue.put(file)
    threads = []
    s3Client = utils.initialize_S3()
    for i in range(MAX_NUMBER_OF_THREADS):
        threads.append(fpw.file_processing_worker(
            threadID=i,
            name='file_processing_worker_%s' % i,
            queue=queue,
            zip=zfile,
            s3=s3Client
        ))
        print("Starting worker %s..." % i)
        threads[int(i)].start()
    for t in threads:
        t.join()
    del (threads)

def __separate_into_chunks(files, num):
    avg = len(files) / float(num)
    out = []
    last = 0.0

    while last < len(files):
        out.append(files[int(last):int(last + avg)])
        last += avg

    return out

def start(event, context):
    print("Starting facts update...")
    startTime = time.time()
    
    number_of_chunks = int(event['NUMBER_OF_CHUNKS_KEY'])
    chunk_to_process = int(event['CHUNK_TO_PROCESS_KEY'])

    __initialize_db()
    __download_and_process_data(number_of_chunks, chunk_to_process)

    end = time.time()

    print("""Sticker Price Database Population Complete\n
            Elapsed time: %2d minutes, %2d seconds\n"""
          % ((end - startTime)/60, (end - startTime) % 60))
