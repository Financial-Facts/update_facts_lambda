import threading
import asyncio
from zipfile import ZipFile
from zipfile import ZipInfo
import json
import serviceConstants as const
import psycopg2
import connection_utils as utils
from botocore.exceptions import ClientError
import os
lock: threading.Lock = threading.Lock()

MAX_CONNECTION_REESTABLISH_ATTEMPTS = 5
BUCKET_NAME = os.environ[const.BUCKET_NAME_KEY]
MAX_PROCESSING_BATCH_SIZE = 5

class file_processing_worker (threading.Thread):

    def __init__(
        self,
        threadID,
        name,
        counter,
        zip: ZipFile,
        files: list[ZipInfo]
    ):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.zip = zip
        self.files = files

    def run(self):
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.waitForProcessingCompletion(loop))
        loop.close()

    async def waitForProcessingCompletion(
            self,
            loop: asyncio.AbstractEventLoop) -> None:
        i = 0
        while i in range(len(self.files)):
            tasks: list[asyncio.Task] = []
            for j in range(MAX_PROCESSING_BATCH_SIZE):
                tasks.append(loop.create_task(self.processFactsFile(
                        self.files[i]
                    )))
                i += 1
            await asyncio.wait(tasks)

    async def processFactsFile(
            self,
            file: ZipInfo) -> None:
        fileAdded = False
        cik = file.filename[:-5]
        try:
            s3 = utils.initialize_S3()
            temp = json.loads(self.zip.read(file))
            try:
                data = json.loads(s3.Bucket(BUCKET_NAME).Object(file.filename).get()['Body'].read().decode())
                if (
                    temp != data
                ):
                    print("Updating %s..." % cik)
                    self.__attempt_update(cik, temp)
                    fileAdded = True
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    print("Adding %s..." % cik)
                    self.__attempt_insert(cik, temp)
                    fileAdded = True
                else:
                    print(ex)
        except json.decoder.JSONDecodeError:
            print("Cannot process %s" % cik)
        if (fileAdded):
            fileAdded = False
            object = s3.Object(BUCKET_NAME, file.filename)
            object.put(Body=json.dumps(temp))

    
    def __attempt_update(self, cik, data) -> None:
        connection = utils.get_db_connection()
        updateComplete = False
        retryAttempts = MAX_CONNECTION_REESTABLISH_ATTEMPTS
        with connection.cursor() as cursor:
            while (not updateComplete and retryAttempts >= 5):
                try:
                    self.__update_database(data, cik, cursor)
                    updateComplete = True
                except psycopg2.OperationalError:
                    connection = utils.get_db_connection()
                    cursor = connection.cursor()
        cursor.close()
        connection.close()

    def __update_database(self, data, cik, cursor) -> None:
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

    def __attempt_insert(self, cik, data) -> None:
        connection = utils.get_db_connection()
        insertComplete = False
        retryAttempts = MAX_CONNECTION_REESTABLISH_ATTEMPTS
        with connection.cursor() as cursor:
            while (not insertComplete and retryAttempts >= 0):
                try:
                    self.__insert_database(data, cik, cursor)
                    insertComplete = True
                except psycopg2.errors.InFailedSqlTransaction:
                    print("%s already exists within database..." % cik)
                    insertComplete = True
                except psycopg2.OperationalError:
                    connection = utils.get_db_connection()
                    cursor = connection.cursor()
        cursor.close()
        connection.close()

    def __insert_database(self, data, cik, cursor) -> None:
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
