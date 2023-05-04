import os
import psycopg2
import pandas as pd
# import boto3
from io import StringIO
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from extract import extract_transactional_data
# from transform import identify_and_remove_duplicated_data
# from load_data_to_s3 import df_to_s3

dbname = os.getenv("dbname")
host = os.getenv("host")
port = os.getenv("port")
user = os.getenv("user")
password = os.getenv("password")


def connect_to_redshift():

    connect = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )
    print("connection to redshift made")

    return connect


def extract_transactional_data():

    connect = connect_to_redshift()

    query = """select t1.*,
                      case when t2.Description = '?' or t2.Description is null then 'Unknown' 
                        else t2.Description end as Description
                from bootcamp1.online_transactions t1
                left join bootcamp1.stock_description t2 on t1.stock_code = t2.stock_code
                where customer_id <> ''
                    and t1.stock_code not in ('POSTAGE', 'BANK CHARGES', 'D', 'M', 'CRUK')
                    and t1.quantity > 0
               """

    online_transactions_reduced = pd.read_sql(query, connect)

    print(f"The data frame contains {online_transactions_reduced.shape[0]} invoices")

    return online_transactions_reduced


def identify_and_remove_duplicated_data(df):
    """Method that removes identifies and removes duplicates"""

    if df.duplicated().sum() > 0:
        df_cleaned = df.drop_duplicates(keep='first')

        print("-" * 50)
        print("Shape of data before removing duplicates:", df.shape)
        print("Shape of data after removing duplicates:", df_cleaned.shape)
        print("-" * 50)

        return df_cleaned

    else:
        print("no duplicates values found")

        return df


def connect_to_s3():
    """Methods that connects to s3"""

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    return s3_client


def df_to_s3():
    """Function that writes a data frame as a .csv file to a s3 bucket"""

    csv_buffer = StringIO()  # create buffer to temporarily store the Data Frame

    df.to_csv(csv_buffer, index=False)  # code to write the data frame as csv file

    s3_client = connect_to_s3()

    s3_client.put_object(
        Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue()
    )  # this code writes the temp stored csv file and writes to s3

    print(f"The transformed data is saved as CSV in the following location s3://{s3_bucket}/{key}")


dag = DAG("etl-pipeline",
          description="etl-pipeline",
          schedule_interval="* * * * *",
          start_date=datetime(2023,1,1), catchup=False)


extract_operator = PythonOperator(task_id="extract_data",
                                  python_callable=extract_transactional_data,
                                  dag=dag)

transform_operator = PythonOperator(task_id="transform_data",
                                    python_callable=identify_and_remove_duplicated_data,
                                    op_kwargs={"df": "{{ ti.xcom_pull(task_ids='extract_data') }}"},
                                    dag=dag)

# load_operator = PythonOperator(task_id="load_data",
#                                python_callable=df_to_s3,
#                                dag=dag)


extract_operator >> transform_operator
