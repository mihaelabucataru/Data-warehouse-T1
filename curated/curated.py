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

# dim_products
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

category_erp_df = category_erp_df.drop_duplicates(subset=["id"])

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

print(dim_products.head(10))

dim_products.to_sql(
    name="dim_products",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_products loaded into curated.dim_products")

# dim_customers

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

customer_erp_df["cid"] = customer_erp_df["cid"].astype(int)
location_erp_df["cid"] = location_erp_df["cid"].astype(int)

customer_erp_df = customer_erp_df.drop_duplicates(subset=["cid"])
location_erp_df = location_erp_df.drop_duplicates(subset=["cid"])

df = pd.merge(
    left=customer_crm_df,
    right=customer_erp_df,
    how="left",
    left_on="cst_id",
    right_on="cid"
)

df = pd.merge(
    left=df,
    right=location_erp_df,
    how="left",
    left_on="cst_id",
    right_on="cid",
    suffixes=("", "_loc")
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
dim_customers.insert(0, "customer_key", dim_customers.index + 1)

print(dim_customers.head(10))

dim_customers.to_sql(
    name="dim_customers",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_customers loaded into curated.dim_customers")


# fact_sales

sales_details_df = pd.read_sql(
    "SELECT * FROM transformation.crm_sales_details;",
    engine
)

dim_products_df = pd.read_sql(
    "SELECT * FROM curated.dim_products;",
    engine
)

dim_products_df["product_code"] = (
    dim_products_df["product_number"]
    .str.split("-")
    .str[2:]
    .str.join("-")
)

dim_customers_df = pd.read_sql(
    "SELECT * FROM curated.dim_customers;",
    engine
)

df = pd.merge(
    left=sales_details_df,
    right=dim_products_df[["product_key", "product_code"]],
    how="left",
    left_on="sls_prd_key",
    right_on="product_code"
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