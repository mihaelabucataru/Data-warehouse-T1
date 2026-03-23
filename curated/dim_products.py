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

product_crm_df = pd.read_sql(
    "SELECT * FROM transformation.crm_prd_info;",
    conn
)

category_erp_df = pd.read_sql(
    "SELECT * FROM transformation.erp_px_cat_g1v2;",
    conn
)

product_crm_df["cat_id"] = (
    product_crm_df["prd_key"]
    .str.split("-")
    .str[0:2]
    .str.join("_")
)

df = pd.merge(
    left=product_crm_df,
    right=category_erp_df,
    how="left",
    left_on="cat_id",
    right_on="id"
)


dim_products = pd.DataFrame({
    "product_number": df["prd_key"],
    "product_name": df["prd_nm"],
    "category_id": df["cat_id"],
    "category": df["cat"],
    "subcategory": df["subcat"],
    "maintenance": df["maintenance"],
    "cost": df["prd_cost"],
    "product_line": df["prd_line"],
    "start_date": df["prd_start_dt"],
    "end_date": df["prd_end_dt"]
})


dim_products = dim_products.sort_values("product_number").reset_index(drop=True)
dim_products.insert(0, "product_key", dim_products.index + 1)

print(dim_products)

url = URL.create(
    drivername="postgresql+psycopg",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)

engine = create_engine(url)


engine = create_engine(url)


dim_products.to_sql(
    name="dim_products",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_products loaded into curated.dim_products")