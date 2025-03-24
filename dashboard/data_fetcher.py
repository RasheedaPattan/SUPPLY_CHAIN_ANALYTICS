# data_fetcher.py - Fetch Data from MySQL using Queries

import mysql.connector
import pandas as pd
from config import DB_CONFIG  # Import MySQL connection settings
from queries import *  # Import SQL queries

def get_data(query, params=None):
    """
    Fetch data from MySQL and return it as a Pandas DataFrame.

    :param query: SQL query to execute.
    :param params: Tuple of parameters for the query (optional).
    :return: Pandas DataFrame with query results.
    """
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        
        # Execute query with optional parameters
        df = pd.read_sql(query, conn, params=params)
        
        # Close connection
        conn.close()
        
        return df
    except mysql.connector.Error as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()  # Return empty DataFrame if error occurs


# 📌 Fetch Sales Trends Data
def fetch_sales_trends():
    return get_data(SALES_TREND_QUERY)

# 📌 Fetch Category-wise Sales Breakdown
def fetch_category_sales():
    return get_data(CATEGORY_SALES_QUERY)

# 📌 Fetch Customer Segmentation
def fetch_customer_segmentation():
    return get_data("""
        SELECT c.Segment, SUM(f.Sales) as Total_Sales
        FROM fact_sales f
        JOIN dim_customer c ON f.CustomerKey = c.CustomerKey
        GROUP BY c.Segment;
    """)

# 📌 Fetch Regional Sales Analysis
def fetch_region_sales():
    return get_data("""
        SELECT r.Region, SUM(f.Sales) as Total_Sales
        FROM fact_sales f
        JOIN dim_region r ON f.RegionKey = r.RegionKey
        GROUP BY r.Region;
    """)

# 📌 Fetch Shipping Mode Distribution
def fetch_shipping_modes():
    return get_data("""
        SELECT s.Ship_Mode, COUNT(f.Order_ID) as Order_Count
        FROM fact_sales f
        JOIN dim_shipping s ON f.ShippingKey = s.ShippingKey
        GROUP BY s.Ship_Mode;
    """)


# ✅ Run Test if Executed Directly
if __name__ == "__main__":
    print(fetch_sales_trends().head())  # Test fetching sales trends
