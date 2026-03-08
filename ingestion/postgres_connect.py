import psycopg

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",      
    password="data_engineering_mihaela"
)

with conn.cursor() as cur:
    cur.execute("SELECT current_database(), current_user, version();")
    print(cur.fetchone())

conn.close()