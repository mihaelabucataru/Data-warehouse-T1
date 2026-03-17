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
                CREATE TABLE IF NOT EXISTS transformation.erp_loc_a101 (
                    cid     VARCHAR(50),
                    cntry   VARCHAR(100)
                );
            """)

        

    df = pd.read_sql_query(
        sql="""
            SELECT 
                cid,
                cntry
            FROM ingestion.erp_loc_a101
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

    # Transformation 2: Fix the country column
    df["cntry"] = (
        df["cntry"]
        .astype("string")
        .str.strip()
        .replace({"": pd.NA, "[null]": pd.NA})
    )

    countries = {
        "US": "United States",
        "USA": "United States",
        "United States": "United States",
        "DE": "Germany",
        "Germany": "Germany",
        "Australia": "Australia",
        "Canada": "Canada",
        "France": "France",
        "United Kingdom": "United Kingdom",
    }

    df["cntry"] = df["cntry"].replace(countries)
    df["cntry"] = df["cntry"].fillna("N/A")

    # insert cleaned rows
    insert_query = """
    INSERT INTO transformation.erp_loc_a101 (
    cid,
    cntry
    )
    VALUES (%s, %s)
    """

    with conn.cursor() as cur:
        # truncate table before loading
        cur.execute("TRUNCATE TABLE transformation.erp_loc_a101 ;")

        # insert cleaned rows
        cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    print("Data inserted into transformation.erp_loc_a101 ")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()