import psycopg
import pandas as pd

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="DWH",
    user="postgres",
    password="data_engineering_mihaela"
)

try:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user, version();")
        print(cur.fetchone())

        cur.execute('CREATE SCHEMA IF NOT EXISTS transformation;')

        cur.execute("""
                CREATE TABLE IF NOT EXISTS transformation.crm_sales_details (
                    sls_ord_num    VARCHAR(50),
                    sls_prd_key    VARCHAR(50),
                    sls_cust_id    INT,
                    sls_order_dt   VARCHAR(50),
                    sls_ship_dt    VARCHAR(50),
                    sls_due_dt     VARCHAR(50),
                    sls_sales      NUMERIC(12, 2),
                    sls_quantity   INT,
                    sls_price      NUMERIC(12, 2)
                );
            """)

        

    df = pd.read_sql_query(
        sql="""
            SELECT 
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_sales,
                sls_quantity,
                sls_price
            FROM ingestion.crm_sales_details
        """,
        con=conn
    )

    # Transformation 1: Fix the sales and price
    df.loc[(df['sls_price'] <= 0) | (df['sls_price'].isna()), 'sls_price'] = df['sls_sales'] / df['sls_quantity']

    df.loc[(df['sls_sales'] <= 0) | (df['sls_sales'].isna()), 'sls_sales'] = df['sls_quantity'] * df['sls_price']

    df.loc[df['sls_sales'] != df['sls_quantity'] * df['sls_price'], 'sls_sales'] = df['sls_quantity'] * df['sls_price']

    # Transformation 2: Convert dates to datetime
    date_cols = ['sls_order_dt', 'sls_ship_dt', 'sls_due_dt']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

    # Transformation 3: Fix order dates
    order_count = df.groupby('sls_ord_num')['sls_ord_num'].transform('count')

    # If there is only one item per order set the order date to the ship date
    df.loc[order_count == 1, 'sls_order_dt'] = df['sls_ship_dt']

    # If there are multiple items with the same order number, set the same order date for all the items in the order
    df['sls_order_dt'] = df.groupby('sls_ord_num')['sls_order_dt'].transform('min')
  
    for col in date_cols:
        df[col] = df[col].dt.strftime('%Y/%m/%d')


    # insert cleaned rows
    insert_query = """
    INSERT INTO transformation.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cur:
        # truncate table before loading
        cur.execute("TRUNCATE TABLE transformation.crm_sales_details;")

        # insert cleaned rows
        cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    print("Data inserted into transformation.crm_sales_details")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()