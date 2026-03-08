import psycopg

NEW_DB_NAME = "DWH"   
HOST = "localhost"
PORT = 5432
USER = "postgres"
PASSWORD = "data_engineering_mihaela"

# new database
conn = psycopg.connect(
    host=HOST,
    port=PORT,
    dbname="postgres",
    user=USER,
    password=PASSWORD,
)


conn.autocommit = True

try:
    with conn.cursor() as cur:
        # check if DB already exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (NEW_DB_NAME,))
        exists = cur.fetchone() is not None

        if exists:
            print(f"Database '{NEW_DB_NAME}' already exists.")
        else:
            cur.execute(f'CREATE DATABASE "{NEW_DB_NAME}";')
            print(f"Database '{NEW_DB_NAME}' created successfully")

finally:
    conn.close()

