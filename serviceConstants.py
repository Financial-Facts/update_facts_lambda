#DB
PORT="5432"
REGION="us-east-1"
DBNAME="financial_facts_db"
DB_USERNAME_KEY="USERNAME_KEY"
DB_PASSWORD_KEY="PASSWORD_KEY"
DB_ENDPOINT_KEY="ENDPOINT_KEY"
S3_KEY="S3_KEY"
S3_SECRET_KEY="S3_SECRET_KEY"
BUCKET_NAME_KEY="BUCKET_NAME_KEY"

# Misc
USER_AGENT_VALUE = 'XYZ/3.0'
EMPTY = ''

# Queries
CREATE_SCHEMA_QUERY = """CREATE SCHEMA financial_facts;
                         GRANT ALL ON SCHEMA financial_facts TO choochera;
                         SET schema 'financial_facts'"""
CREATE_FACTS_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS facts (
                            cik varchar(13) not null primary key,
                            data jsonb
                        );"""
INSERT_DATA_QUERY = """INSERT INTO financial_facts.facts (cik, data)
                     values('%s', (select * from to_jsonb('%s'::JSONB)));COMMIT;"""
UPDATE_DATA_QUERY = """UPDATE facts set data='%s' where cik='%s';COMMIT;"""

# Urls
EDGAR_URL = "https://www.sec.gov/Archives/edgar"
DATA_ZIP_PATH = "/daily-index/xbrl/companyfacts.zip"
