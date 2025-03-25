import logging
import os
import mysql.connector
from files.load import load_data
from files.transform import (
    clean_columns, preprocess_dates, 
    create_dimension_and_fact_tables, replace_nan_with_mode
)
from files.database import get_db_connection, save_and_insert_to_database  # ✅ Import database functions
from dashboard.config import DB_CONFIG  

# ✅ Ensure logs directory exists
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)  

# ✅ Logging setup
logging.basicConfig(
    filename=os.path.join(log_dir, 'etl_process.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def load_and_clean_data(file_path):
    """Load and clean the data."""
    try:
        logging.info("🔄 Loading data...")
        df = load_data(file_path)  # Load CSV
        df = clean_columns(df)  # Clean column names
        df = preprocess_dates(df)  # Convert dates
        df = replace_nan_with_mode(df)  # Handle missing values
        logging.info("✅ Data loaded and cleaned successfully.")
        return df
    except Exception as e:
        logging.error(f"❌ Data load/clean failed: {e}")
        raise

if __name__ == "__main__":
    file_path = 'train.csv'  # ✅ Ensure this file exists in your project directory

    try:
        # ✅ Get Database Connection
        conn = get_db_connection()
        if not conn:
            raise Exception("❌ Database connection failed.")

        cursor = conn.cursor()

        # ✅ Load and Clean Data
        df = load_and_clean_data(file_path)

        # ✅ Create Dimension and Fact Tables from Data
        dims_and_fact = create_dimension_and_fact_tables(df)
        dim_customer, dim_product, dim_shipping, dim_region, dim_date, fact_sales = dims_and_fact

        # ✅ Insert Data into MySQL Tables
        save_and_insert_to_database(cursor, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date)

        # ✅ Commit changes and log success
        conn.commit()
        logging.info("✅ ETL process completed successfully.")
        print("✅ ETL process completed successfully!")

    except Exception as e:
        logging.error(f"❌ ETL process failed: {e}")
        print("❌ ETL failed. Check logs.")

    finally:
        # ✅ Close Connection Safely
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
