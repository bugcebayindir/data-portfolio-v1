# ETL Pipeline

### Introduction
We built an ETL pipeline that carries out the following tasks:
- Extracts transactional data related with invoices from Redshift
- Transforms data by identifying and removing duplicates
- Transforms invoice_date data type by fixing a variable
- Loads transformed data to AWS S3 bucket

### Requirements
The minimum requirements:
- Docker for Mac: [Docker](https://docs.docker.com/desktop/install/mac-install/) 
- Docker for Windows: [Docker](https://docs.docker.com/desktop/install/windows-install/) 
- AWS Redshift connection details
- AWS S3 Bucket details

### Instructions on how to execute the code

Copy the .env.example file to .env and fill out the environment vars.

Make sure you are executing the code from the etl_pipeline folder.

To run it locally first build the image.

```bash
docker image build -t etl-pipeline:0.1 .
```

Then run the job:
```bash
 docker run --env-file .env etl-pipeline:0.1
```