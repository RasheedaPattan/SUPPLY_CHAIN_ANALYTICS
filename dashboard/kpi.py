import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ✅ MySQL Configuration
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Rasheeda@123",
    "database": "supply_chaindb"
}

# ✅ Function to Fetch Data using mysql.connector
def get_data(query):
    try:
        # 🔹 Establishing MySQL connection
        conn = mysql.connector.connect(**DB_CONFIG)
        
        # 🔹 Read SQL data (Fixed argument issue)
        df = pd.read_sql_query(query, con=conn)
        
        # 🔹 Close connection after fetching data
        conn.close()
        return df

    except mysql.connector.Error as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

# ✅ Sales Trends Over Time (Fixed `GROUP BY` issue)
sales_trend_query = """
    SELECT d.Year, d.Month, d.Month_Name, SUM(f.Sales) AS Total_Sales
    FROM fact_sales f
    JOIN dim_date d ON f.OrderDateKey = d.DateKey
    GROUP BY d.Year, d.Month, d.Month_Name
    ORDER BY d.Year, d.Month;
"""
sales_trend_df = get_data(sales_trend_query)

plt.figure(figsize=(12, 6))
sns.lineplot(data=sales_trend_df, x="Month_Name", y="Total_Sales", hue="Year", marker="o")
plt.title("Sales Trends Over Time")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.show()

# ✅ Category-wise Sales Breakdown
category_sales_query = """
    SELECT p.Category, SUM(f.Sales) AS Total_Sales
    FROM fact_sales f
    JOIN dim_product p ON f.ProductKey = p.ProductKey
    GROUP BY p.Category;
"""
category_sales_df = get_data(category_sales_query)

plt.figure(figsize=(8, 6))
sns.barplot(data=category_sales_df, x="Category", y="Total_Sales", palette="viridis")
plt.title("Sales by Category")
plt.xlabel("Category")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.show()

# ✅ Customer Segmentation Analysis
customer_segment_query = """
    SELECT c.Segment, SUM(f.Sales) AS Total_Sales
    FROM fact_sales f
    JOIN dim_customer c ON f.CustomerKey = c.CustomerKey
    GROUP BY c.Segment;
"""
customer_segment_df = get_data(customer_segment_query)

plt.figure(figsize=(6, 6))
plt.pie(customer_segment_df["Total_Sales"], labels=customer_segment_df["Segment"], autopct="%1.1f%%", colors=["#ff9999", "#66b3ff", "#99ff99", "#ffcc99"])
plt.title("Customer Segmentation by Sales")
plt.show()

# ✅ Shipping Mode Distribution
shipping_mode_query = """
    SELECT s.Ship_Mode, COUNT(f.Order_ID) AS Order_Count
    FROM fact_sales f
    JOIN dim_shipping s ON f.ShippingKey = s.ShippingKey
    GROUP BY s.Ship_Mode;
"""
shipping_mode_df = get_data(shipping_mode_query)

plt.figure(figsize=(8, 5))
sns.barplot(data=shipping_mode_df, x="Ship_Mode", y="Order_Count", palette="coolwarm")
plt.title("Shipping Mode Distribution")
plt.xlabel("Ship Mode")
plt.ylabel("Number of Orders")
plt.show()

# ✅ Regional Sales Analysis
region_sales_query = """
    SELECT r.Region, SUM(f.Sales) AS Total_Sales
    FROM fact_sales f
    JOIN dim_region r ON f.RegionKey = r.RegionKey
    GROUP BY r.Region;
"""
region_sales_df = get_data(region_sales_query)

plt.figure(figsize=(8, 5))
sns.barplot(data=region_sales_df, x="Region", y="Total_Sales", palette="muted")
plt.title("Regional Sales Analysis")
plt.xlabel("Region")
plt.ylabel("Total Sales")
plt.show()
