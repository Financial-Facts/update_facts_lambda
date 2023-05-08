# update_facts_lambda

Update_Facts_Lambda is a Python-based software project designed to automate the process of downloading and storing public filing information from the SEC EDGAR API. The software can be invoked multiple times simultaneously, with different chunks of data designated to be processed by each respective invocation for asynchronous lambda processing. This feature makes Update_Facts_Lambda highly scalable and able to handle the large volumes of data.

With Update_Facts_Lambda, you can easily download and store JSON files in an AWS S3 bucket and keep them up-to-date with the latest filings issued by each respective company. The software uses file queueing and multithreaded processing along with asynchronous batches of tasks to ensure that new filings are added or updated within the bucket in a timely and efficient manner.

Key features of the software include seamless communication with the AWS S3 bucket, support for AWS Lambda function invocation, streaming data downloaded directly from the SEC EDGAR API, multi-threaded queue processing, asynchronous batch processing, and the ability for the lambda to be invoked multiple times at once for simultaneous processing of different chunks of the json files. These features make Update_Facts_Lambda a reliable, efficient, and highly scalable solution for managing SEC filing data.

Using Update_Facts_Lambda can significantly reduce the time and effort required to keep your SEC filing data up-to-date, and help you make more informed investment decisions based on the latest filings issued by companies. Additionally, the software provides a scalable and cost-effective solution for managing large volumes of SEC filing data, making it an ideal choice for companies of all sizes.

Overall, Update_Facts_Lambda is a powerful and innovative solution for managing SEC filing data, and is a must-have tool for anyone who needs to stay up-to-date with the latest filings issued by companies while also supporting highly scalable and efficient processing of large amounts of data.

--------------------------
## Flow
![flow](https://user-images.githubusercontent.com/74555083/236692138-350a3603-0bab-43df-bcdf-5b720a2a7876.svg)

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
      
## Sample Output
![image](https://user-images.githubusercontent.com/74555083/236580560-46d639fd-dd91-446d-9e94-09ed053930dd.png)

## Sample Log
```
2023-05-05T17:40:37.906-05:00	INIT_START Runtime Version: python:3.9.v19 Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:e73d5f60c4282fb09ce24a6d3fe8997789616f3a53b903f4ed7c9132a58045f6

2023-05-05T17:40:38.204-05:00	START RequestId: 3d0ff246-b09d-4e74-8e1a-c5c4fe1680d9 Version: $LATEST

2023-05-05T17:40:38.205-05:00	Starting facts update...

2023-05-05T17:40:38.205-05:00	Downloading facts data from EDGAR...

2023-05-05T17:40:51.024-05:00	Dividing processing workload to workers...

2023-05-05T17:40:51.209-05:00	Starting worker 0...

2023-05-05T17:40:51.209-05:00	Starting worker 1...

2023-05-05T17:40:51.222-05:00	Starting worker 2...

2023-05-05T17:40:51.222-05:00	Starting worker 3...

2023-05-05T17:40:51.226-05:00	Starting worker 4...

2023-05-05T17:40:51.232-05:00	Starting worker 5...

2023-05-05T17:40:51.323-05:00	Starting worker 6...

2023-05-05T17:40:51.418-05:00	Starting worker 7...

2023-05-05T17:40:51.522-05:00	Starting worker 8...

2023-05-05T17:40:51.573-05:00	Starting worker 9...

2023-05-05T17:40:51.592-05:00	file_processing_worker_0: CIK0000021239 is up to date!

2023-05-05T17:40:51.794-05:00	file_processing_worker_5: CIK0000028385 is up to date!

...

2023-05-05T17:41:08.055-05:00	file_processing_worker_7: CIK0000036369 is up to date!

2023-05-05T17:41:08.060-05:00	file_processing_worker_7 is finished processing!

2023-05-05T17:41:08.106-05:00	Sticker Price Database Population Complete

2023-05-05T17:41:08.106-05:00	Elapsed time: 0 minutes, 29 seconds

2023-05-05T17:41:08.109-05:00	END RequestId: 3d0ff246-b09d-4e74-8e1a-c5c4fe1680d9
```
      
Created and authored by Matthew Gabriel
