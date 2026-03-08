import psycopg
from psycopg import sql

DB_NAME = "DWH"
SCHEMA_NAME = "ingestion"  
HOST = "localhost"
PORT = 5432                        
USER = "postgres"
PASSWORD = "data_engineering_mihaela"


def main():
    conn = psycopg.connect(
        host=HOST,
        port=PORT,
        dbname=DB_NAME,
        user=USER,
        password=PASSWORD,
    )

    try:
        with conn.cursor() as cur:
          
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                .format(sql.Identifier(SCHEMA_NAME))
            )

 
            cur.execute(
                sql.SQL("SET search_path TO {}")
                .format(sql.Identifier(SCHEMA_NAME))
            )

          
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.crm_cust_info (
                    cst_id              INT,
                    cst_key             VARCHAR(50),
                    cst_firstname       VARCHAR(50),
                    cst_lastname        VARCHAR(50),
                    cst_marital_status  VARCHAR(50),
                    cst_gndr            VARCHAR(50),
                    cst_create_date     DATE
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.crm_prd_info (
                    prd_id         INT,
                    prd_key        VARCHAR(50),
                    prd_nm         VARCHAR(255),
                    prd_cost       NUMERIC(12, 2),
                    prd_line       VARCHAR(50),
                    prd_start_dt   DATE,
                    prd_end_dt     DATE
                );
            """)

       
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.crm_sales_details (
                    sls_ord_num    VARCHAR(50),
                    sls_prd_key    VARCHAR(50),
                    sls_cust_id    INT,
                    sls_order_dt   VARCHAR(50),
                    sls_ship_dt    VARCHAR(50),
                    sls_due_dt     VARCHAR(50),
                    sls_sales      NUMERIC(12, 2),
                    sls_quantity   INT,
                    sls_price      NUMERIC(12, 2)
                );
            """)

          
        
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.erp_cust_az12 (
                    cid     VARCHAR(50),
                    bdate   VARCHAR(50),
                    gen     VARCHAR(20)
                );
            """)

           
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.erp_loc_a101 (
                    cid     VARCHAR(50),
                    cntry   VARCHAR(100)
                );
            """)

          
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion.erp_px_cat_g1v2 (
                    id           VARCHAR(50),
                    cat          VARCHAR(100),
                    subcat       VARCHAR(100),
                    maintenance  VARCHAR(100)
                );
            """)

        conn.commit()
        print(f"All 6 tables created successfully in schema '{SCHEMA_NAME}' inside database '{DB_NAME}'")

    except Exception as e:
        conn.rollback()
        print("Error while creating tables:")
        print(e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()