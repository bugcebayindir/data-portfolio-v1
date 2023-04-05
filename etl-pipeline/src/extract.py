import psycopg2
import pandas as pd

def connect_to_redshift(dbname, host, port, user, password):
    """Method that connects to redshift. This gives a warning so will look for another solution"""

    connect = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )

    print("connection to redshift made")

    return connect

def extract_transactional_data(dbname,host,port,user,password):
    connect = connect_to_redshift(dbname,host,port,user,password)
    query = """
    SELECT ot.*,
            case when sd.description = '?' or sd.description is null then 'Unknown' else sd.description end as description
    FROM bootcamp1.online_transactions ot
    LEFT JOIN bootcamp1.stock_description sd ON ot.stock_code = sd.stock_code
    WHERE ot.customer_id <> '' 
    AND ot.stock_code NOT IN ('BANK CHARGES', 'POSTAGE', 'D', 'M', 'CRUK')
    AND ot.quantity > 0 """

    online_transaction = pd.read_sql(query, connect)
    print("This table contains:",online_transaction.shape[0],"invoices")

    return online_transaction
