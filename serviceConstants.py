#DB
ENDPOINT='dev-financial-facts-db-1-public-instance-1.cilvae7glcw6.us-east-1.rds.amazonaws.com'
PORT="5432"
REGION="us-east-1"
DBNAME="financial_facts_db"
DB_USERNAME_KEY="USERNAME_KEY"
DB_PASSWORD_KEY="PASSWORD_KEY"
DB_ENDPOINT_KEY="ENDPOINT_KEY"
S3_KEY="S3_KEY"
S3_SECRET_KEY="S3_SECRET_KEY"

# Misc
HOST = 'dev-financial-facts-db-1-public-instance-1.cilvae7glcw6.us-east-1.rds.amazonaws.com'
DATABASE_NAME = 'postgres'
USER_AGENT_VALUE = 'XYZ/3.0'
DATA_DIRECTORY = 'Data'
TEMP_DIRECTORY = 'Temp'
CIK_LIST = 'cikList'
DEFAULT_DB_USERNAME = 'postgres'
DEFAULT_DB_PASSWORD = 'password'
DEFAULT_DB_NAME = 'postgres'
POST = 'POST'
JSON_EXTENSION = '.json'
CIK_MAP_FILENAME = 'cikMap.json'
TRUE = 'True'
EMPTY = ''
CIK = 'CIK'
CIK_MAP = 'cikMap'
FINANCIAL_FACTS = 'financial_facts'

# Queries
GET_PROCESSED_CIK_QUERY = 'SELECT cik from facts;'
DROP_FACTS_TABLE_QUERY = 'DROP TABLE IF EXISTS facts;'
CREATE_SCHEMA_QUERY = """CREATE SCHEMA financial_facts;
                         GRANT ALL ON SCHEMA financial_facts TO choochera;
                         SET schema 'financial_facts'"""
CREATE_FACTS_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS facts (
                            cik varchar(13) not null primary key,
                            data jsonb
                        );"""
INSERT_DATA_QUERY = """INSERT INTO financial_facts.facts (cik, data)
                     values('%s', (select * from to_jsonb('%s'::JSONB)));COMMIT;"""
UPDATE_DATA_QUERY = """UPDATE facts set data='%s' where cik='%s';COMMIT;
"""
GET_DATA_QUERY = "SELECT data FROM facts where cik = '%s'"
APPEND_CIK_QUERY = " or cik = '%s'"

# Urls
EDGAR_URL = "https://www.sec.gov/Archives/edgar"
DATA_ZIP_PATH = "/daily-index/xbrl/companyfacts.zip"
