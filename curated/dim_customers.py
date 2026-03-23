import psycopg
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

HOST = "localhost"
PORT = 5432
DB_NAME = "DWH"
USER = "postgres"
PASSWORD = "data_engineering_mihaela"

conn = psycopg.connect(
    host=HOST,
    port=PORT,
    dbname=DB_NAME,
    user=USER,
    password=PASSWORD
)

cursor = conn.cursor()

customer_crm_df = pd.read_sql(
    "SELECT * FROM transformation.crm_cust_info;",
    conn
)

customer_erp_df = pd.read_sql(
    "SELECT * FROM transformation.erp_cust_az12;",
    conn
)

location_erp_df = pd.read_sql(
    "SELECT * FROM transformation.erp_loc_a101;",
    conn
)

df = pd.merge(
    left=customer_crm_df,
    right=customer_erp_df,
    how="left",
    left_on="cst_key",
    right_on="cid"
)


df = pd.merge(
    left=df,
    right=location_erp_df,
    how="left",
    left_on="cst_key",
    right_on="cid",
    suffixes = ("","_loc")
)

dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number": df["cst_key"],
    "first_name": df["cst_firstname"],
    "last_name": df["cst_lastname"],
    "country": df["cntry"],
    "marital_status": df["cst_marital_status"],
    "gender": df["cst_gndr"],       
    "birthdate": df["bdate"],
    "create_date": df["cst_create_date"]
})

dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
dim_customers.insert(0,"customer_key",dim_customers.index + 1)

print(dim_customers)

url = URL.create(
    drivername="postgresql+psycopg",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)

engine = create_engine(url)

dim_customers.to_sql(
    name="dim_customers",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_customers loaded into curated.dim_customers")