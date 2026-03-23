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


url = URL.create(
    drivername="postgresql+psycopg",
    username=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DB_NAME
)

engine = create_engine(url)


sales_details_df = pd.read_sql(
    "SELECT * FROM transformation.crm_sales_details;",
    engine
)

dim_products_df = pd.read_sql(
    "SELECT * FROM curated.dim_products;",
    engine
)

dim_customers_df = pd.read_sql(
    "SELECT * FROM curated.dim_customers;",
    engine
)


df = pd.merge(
    left=sales_details_df,
    right=dim_products_df[["product_key", "product_number"]],
    how="left",
    left_on="sls_prd_key",
    right_on="product_number"
)


df = pd.merge(
    left=df,
    right=dim_customers_df[["customer_key", "customer_id"]],
    how="left",
    left_on="sls_cust_id",
    right_on="customer_id"
)


fact_sales = pd.DataFrame({
    "product_key": df["product_key"],
    "customer_key": df["customer_key"],
    "order_number": df["sls_ord_num"],
    "order_date": df["sls_order_dt"],
    "shipping_date": df["sls_ship_dt"],
    "due_date": df["sls_due_dt"],
    "sales": df["sls_sales"],
    "quantity": df["sls_quantity"],
    "price": df["sls_price"]
})

print(fact_sales.head(10))


fact_sales = fact_sales.reset_index(drop=True)
fact_sales.insert(0, "sales_key", fact_sales.index + 1)


fact_sales.to_sql(
    name="fact_sales",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("fact_sales loaded into curated.fact_sales")


conn.close()