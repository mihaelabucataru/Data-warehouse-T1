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
            CREATE TABLE IF NOT EXISTS transformation.crm_cust_info (
                cst_id INT,
                cst_key VARCHAR(50),
                cst_firstname VARCHAR(50),
                cst_lastname VARCHAR(50),
                cst_marital_status VARCHAR(50),
                cst_gndr VARCHAR(50),
                cst_create_date DATE
            );
        """)

        

    df = pd.read_sql_query(
        sql="""
            SELECT 
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date
            FROM ingestion.crm_cust_info
        """,
        con=conn
    )

    # transformation 1: remove rows with missing customer id
    df = df.dropna(subset=['cst_id'])

    # transformation 2: remove duplicate customer ids, keeps the last occurrence
    df = df.drop_duplicates(subset=['cst_id'], keep='last')

    # transformation 3: clean names (remove spaces)
    df["cst_firstname"] = df["cst_firstname"].astype("string").str.strip()
    df["cst_lastname"] = df["cst_lastname"].astype("string").str.strip()

    # transformation 4: standardize gender, replace missing values with N/A
    df["cst_gndr"] = df["cst_gndr"].replace("M", "Male")
    df["cst_gndr"] = df["cst_gndr"].replace("F", "Female")
    df["cst_gndr"] = df["cst_gndr"].fillna("N/A")

    # transformation 5: standardize marital status, replace missing values with N/A
    df["cst_marital_status"] = df["cst_marital_status"].replace("M", "Married")
    df["cst_marital_status"] = df["cst_marital_status"].replace("S", "Single")
    df["cst_marital_status"] = df["cst_marital_status"].fillna("N/A")

    # insert cleaned rows
    insert_query = """
    INSERT INTO transformation.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cur:
        # truncate table before loading
        cur.execute("TRUNCATE TABLE transformation.crm_cust_info;")

        # insert cleaned rows
        cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    print("Data inserted into transformation.crm_cust_info")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()