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
import filecmp


def __get_db_auth() -> list[str]:
    try:
        db_username: str = os.environ[const.DB_USERNAME_KEY],
        db_password: str = os.environ[const.DB_PASSWORD_KEY]
    except KeyError:
        raise Exception("Database credentials not provided")
    return [db_username, db_password]


def __get_db_connection():
    print("Establishing database connection...")
    creds: list[str] = __get_db_auth()
    conn = psycopg2.connect(
        host=const.HOST,
        database=const.DATABASE_NAME,
        user=creds[0][0],
        password=creds[1])
    return conn


def __initialize_db(connection, cursor) -> None:
    print("Initializing database connection..")
    cursor.execute(const.CREATE_FACTS_TABLE_QUERY)
    connection.commit()


def __drop_facts(connection, cursor) -> None:
    print("Dropping facts table from database...")
    cursor.execute(const.DROP_FACTS_TABLE_QUERY)
    connection.commit()


def __delete_data() -> None:
    print("Deleting current facts data...")
    shutil.rmtree('Data')


def __download_data() -> None:
    print("Downloading facts data from EDGAR...")
    req = Request(
        url=const.EDGAR_URL + const.DATA_ZIP_PATH,
        headers={'User-Agent': const.USER_AGENT_VALUE}
    )
    with urlopen(req) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            os.mkdir('Temp')
            zfile.extractall(const.TEMP_DIRECTORY)


def __process_data() -> list:
    print("Processing data updates..")
    tempFiles = [
            os.path.relpath(file) for file in os.listdir(const.TEMP_DIRECTORY)
        ]
    fileAdded = False
    for i in range(len(tempFiles)):
        with open('Temp\\' + tempFiles[i]) as openTempFile:
            try:
                with open('Data\\' + tempFiles[i]) as openDataFile:
                    if (
                        not filecmp.cmp(
                            openTempFile.name,
                            openDataFile.name,
                            shallow=True
                        )
                    ):
                        cik = openTempFile.name[5:-5]
                        __update_database(openTempFile.name, cik)
                        fileAdded = True
            except FileNotFoundError:
                cik = openTempFile.name[5:-5]
                __update_database(openTempFile.name, cik)
                fileAdded = True
        if (fileAdded):
            os.replace('Temp\\' + tempFiles[i], 'Data\\' + tempFiles[i])
            fileAdded = False
    shutil.rmtree('Temp')


def __update_database(fileToProcess, cik) -> None:
    with open(fileToProcess) as file:
        try:
            data = json.load(file)
            text = json.dumps(data)
            text = text.replace('\'', const.EMPTY)
            try:
                cursor.execute(const.UPDATE_DATA_QUERY % (
                    cik,
                    text,
                    text,
                    cik
                ))
            except psycopg2.errors.UniqueViolation:
                pass
        except OSError:
            pass
    connection.commit()


if __name__ == '__main__':

    startTime = time.time()
    connection = __get_db_connection()
    cursor = connection.cursor()

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
        __drop_facts(connection, cursor)
        __delete_data()
        connection.commit()

    __initialize_db(connection, cursor)
    __download_data()
    __process_data()

    cursor.close()
    connection.close()

    end = time.time()

    print("""Sticker Price Database Population Complete\n
            Elapsed time: %2d minutes, %2d seconds\n"""
          % ((end - startTime)/60, (end - startTime) % 60))
