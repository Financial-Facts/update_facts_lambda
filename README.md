# migrate_facts_db

## Description of Function
Downloads financial information from SEC and loads the data as JSON files into an S3 bucket indexed by their respective CIK

## Script Operation Overview

### Step 1
Imports stock json data from sec EDGAR website as zip file and streams files into a Queue for use in following steps

### Step 2
Establishes connection with S3 bucket

### Step 3
Begins processing the files within the stream. The workload is divided across multiple threads which begin processing the files in asynchronous batches, pulling the files from the thread safe Queue. This is continued until all of the files have been processed. Processing includes:
  - Checking the S3 bucket for an existing file of the same name.
    - If file exists in S3 bucket: 
      1. Compare S3 file contents to the current streamed file
      2. If they differ in content, update the S3 bucket, otherwise continue
    - If file does not exist in S3 bucket:
      1. Insert the file into S3 bucket
      
  
