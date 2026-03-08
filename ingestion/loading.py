import psycopg

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

            
            cur.execute(f'SET search_path TO "{SCHEMA_NAME}"')

       
            with cur.copy("""
                COPY crm_cust_info
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_crm/cust_info.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("crm_cust_info loaded")


         
            with cur.copy("""
                COPY crm_prd_info
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_crm/prd_info.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("crm_prd_info loaded")


           
            with cur.copy("""
                COPY crm_sales_details
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_crm/sales_details.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("crm_sales_details loaded")


         
            with cur.copy("""
                COPY erp_cust_az12
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_erp/CUST_AZ12.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("erp_cust_az12 loaded")


          
            with cur.copy("""
                COPY erp_loc_a101
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_erp/LOC_A101.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("erp_loc_a101 loaded")


          
            with cur.copy("""
                COPY erp_px_cat_g1v2
                FROM STDIN
                WITH (FORMAT csv, HEADER true)
            """) as copy:
                with open("C:/Users/Zenbook/Downloads/datasets/datasets/source_erp/PX_CAT_G1V2.csv", "r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)

            print("erp_px_cat_g1v2 loaded")


        conn.commit()
        print("All tables loaded successfully")

    except Exception as e:
        conn.rollback()
        print("Error occurred:")
        print(e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()