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
                CREATE TABLE IF NOT EXISTS transformation.erp_cust_az12 (
                    cid     VARCHAR(50),
                    bdate   VARCHAR(50),
                    gen     VARCHAR(20)
                );
            """)

        

    df = pd.read_sql_query(
        sql="""
            SELECT 
                cid,
                bdate,
                gen
            FROM ingestion.erp_cust_az12
        """,
        con=conn
    )

    # Transformation 1: Fix the id column
    df["cid"] = (
    df["cid"]
    .astype(str)
    .str.extract(r'(\d+)$')[0] # Extracts only the digits at the end of the string
    .astype(int)               
    )

    # Transformation 2: Fix the gender column
    df["gen"] = (
    df["gen"]
    .astype(str)
    .str.strip()
)
    df["gen"] = df["gen"].map({
        "M": "Male",
        "F": "Female",
        "Male": "Male",
        "Female":"Female",
        }).fillna("N/A")

    # insert cleaned rows
    insert_query = """
    INSERT INTO transformation.erp_cust_az12 (
    cid,
    bdate,
    gen
    )
    VALUES (%s, %s, %s)
    """

    with conn.cursor() as cur:
        # truncate table before loading
        cur.execute("TRUNCATE TABLE transformation.erp_cust_az12 ;")

        # insert cleaned rows
        cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    print("Data inserted into transformation.erp_cust_az12 ")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()