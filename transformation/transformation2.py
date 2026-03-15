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

        cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")

        cur.execute("DROP TABLE IF EXISTS transformation.crm_prd_info;")

        cur.execute("""
            CREATE TABLE transformation.crm_prd_info (
                prd_id       INT,
                prd_key      VARCHAR(50),
                prd_nm       VARCHAR(255),
                prd_cost     INT,
                prd_line     VARCHAR(50),
                prd_start_dt DATE,
                prd_end_dt   DATE,
                prd_key_2    VARCHAR(5)
            );
        """)

    df = pd.read_sql_query(
        """
        SELECT
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        FROM ingestion.crm_prd_info
        """,
        con=conn
    )

    # Transformation 1: Product Key
    df["prd_key_2"] = df["prd_key"].str[:5].str.replace("-", "_", regex=False)
    # Transformation 2: Product Cost: Fill missing values with 0
    df["prd_cost"] = df["prd_cost"].fillna(0).astype(int)
    # Transformation 3: Product Line: Replace the letters by the full categories
    df["prd_line"] = df["prd_line"].str.strip().replace({
        "T": "Touring",
        "S": "Sport",
        "R": "Road",
        "M": "Mountain"
    })
    # Transformation 4: Fix start dates and end dates
    df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")
    df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors="coerce")

    df = df.sort_values(by=["prd_key", "prd_id"]).copy()

    df["prd_end_dt"] = (
    df.groupby("prd_key")["prd_start_dt"]
    .shift(-1) - pd.Timedelta(days=1)
    )

    df = df.sort_values(by=["prd_id"]).copy() 
    df["prd_start_dt"] = df["prd_start_dt"].dt.date
    df["prd_end_dt"] = df["prd_end_dt"].dt.date
    df["prd_start_dt"] = df["prd_start_dt"].where(df["prd_start_dt"].notna(), None)
    df["prd_end_dt"] = df["prd_end_dt"].where(df["prd_end_dt"].notna(), None)


    df = df[
        [
            "prd_id",
            "prd_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt",
            "prd_key_2"
        ]
    ]

    insert_query = """
        INSERT INTO transformation.crm_prd_info (
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt,
            prd_key_2
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cur:
        cur.executemany(insert_query, df.values.tolist())

    conn.commit()
    print("Done.")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    conn.close()
