import threading
import asyncio
from zipfile import ZipFile
from zipfile import ZipInfo
import json
import serviceConstants as const
from botocore.exceptions import ClientError
import os
from queue import Queue

BUCKET_NAME = os.environ[const.BUCKET_NAME_KEY]
MAX_PROCESSING_BATCH_SIZE = 10

class file_processing_worker (threading.Thread):

    def __init__(
        self,
        threadID,
        name,
        queue: Queue,
        zip: ZipFile,
        s3
    ):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.zip = zip
        self.s3 = s3
        self.queue = queue

    def run(self):
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.waitForProcessingCompletion(loop))
        loop.close()
        print("%s is finished processing!" % self.name)

    async def waitForProcessingCompletion(
            self,
            loop: asyncio.AbstractEventLoop) -> None:
        i = 0
        while not self.queue.empty():
            tasks: list[asyncio.Task] = []
            j = 0
            while j in range(MAX_PROCESSING_BATCH_SIZE) and not self.queue.empty():
                tasks.append(loop.create_task(self.processFactsFile(
                        self.queue.get()
                    )))
                i += 1
                j += 1
            await asyncio.wait(tasks)

    async def processFactsFile(
            self,
            file: ZipInfo) -> None:
        fileAdded = False
        cik = file.filename[:-5]
        try:
            temp = json.loads(self.zip.read(file))
            try:
                data = json.loads(self.s3.Bucket(BUCKET_NAME).Object(file.filename).get()['Body'].read().decode())
                if (
                    temp != data
                ):
                    print("%s: Updating %s..." % (self.name, cik))
                    fileAdded = True
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    print("%s: Adding %s..." % (self.name, cik))
                    fileAdded = True
                else:
                    print(ex)
        except json.decoder.JSONDecodeError:
            print("%s: Cannot process %s" % (self.name, cik))
        if (fileAdded):
            fileAdded = False
            object = self.s3.Object(BUCKET_NAME, file.filename)
            object.put(Body=json.dumps(temp))
        else:
            print("%s: %s is up to date!" % (self.name, cik))
