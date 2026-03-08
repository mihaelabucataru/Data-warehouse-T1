import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="DWH",   
    user="postgres",
    password="data_engineering_mihaela"
)

conn.autocommit = True

schema_name = "ingestion"   

with conn.cursor() as cur:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
    print(f"Schema '{schema_name}' created successfully")

conn.close()