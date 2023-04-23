# ETL Pipeline

### Introduction
We built an ETL pipeline that carries out the following tasks and unit test for definitions:
- Extracts transactional data related with invoices from Redshift
- Transforms data by identifying and removing duplicates
- Transforms invoice_date data type by fixing a variable
- Loads transformed data to AWS S3 bucket
- Unit test for definition

### Requirements
The minimum requirements:
- Docker for Mac: [Docker](https://docs.docker.com/desktop/install/mac-install/) 
- Docker for Windows:
  - Installation: [Docker](https://docs.docker.com/desktop/install/windows-install/)
  - Manual installation steps for older WSL version: [Docker WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)
- AWS Redshift connection details
- AWS S3 Bucket details

### Instructions on how to execute the code

Copy the .env.example file to .env and fill out the environment vars.

For Docker:

Make sure you are executing the code from the etl_pipeline folder.

- To run it locally first build the image:

```bash
docker image build -t etl-pipeline:0.1 .
```

- Then run the image:
```bash
 docker run --env-file .env etl-pipeline:0.1
```

For unit test:
Make sure you are executing the code from the etl_pipeline folder.

```bash
  python -m unittest
  python -m unittest test.test_transfrom
```