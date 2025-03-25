# queries.py - SQL Queries for the Supply Chain Data Integration System

# Sales Trends Over Time
SALES_TREND_QUERY = """
    SELECT d.Year, d.Month_Name, SUM(f.Sales) as Total_Sales
    FROM fact_sales f
    JOIN dim_date d ON f.OrderDateKey = d.DateKey
    GROUP BY d.Year, d.Month_Name
    ORDER BY d.Year, d.Month_Name;
"""

# Category-wise Sales Breakdown
CATEGORY_SALES_QUERY = """
    SELECT p.Category, SUM(f.Sales) as Total_Sales
    FROM fact_sales f
    JOIN dim_product p ON f.ProductKey = p.ProductKey
    GROUP BY p.Category;
"""

# Customer Segmentation
CUSTOMER_SEGMENT_QUERY = """
    SELECT c.Segment, SUM(f.Sales) as Total_Sales
    FROM fact_sales f
    JOIN dim_customer c ON f.CustomerKey = c.CustomerKey
    GROUP BY c.Segment;
"""

# Shipping Mode Distribution
SHIPPING_MODE_QUERY = """
    SELECT s.Ship_Mode, COUNT(f.Order_ID) as Order_Count
    FROM fact_sales f
    JOIN dim_shipping s ON f.ShippingKey = s.ShippingKey
    GROUP BY s.Ship_Mode;
"""

# Regional Sales Analysis
REGION_SALES_QUERY = """
SELECT r.Region, d.Year, SUM(f.Sales) AS Total_Sales
FROM fact_sales f
JOIN dim_region r ON f.RegionKey = r.RegionKey
JOIN dim_date d ON f.OrderDateKey = d.DateKey  -- ✅ Added Date Table for Year Filtering
GROUP BY r.Region, d.Year;

"""

# Insert Data into `dim_product`
INSERT_PRODUCT_QUERY = """
    INSERT INTO dim_product (ProductKey, Category, Product_Name)
    VALUES (%s, %s, %s);
"""

# Insert Data into `dim_customer`
INSERT_CUSTOMER_QUERY = """
    INSERT INTO dim_customer (CustomerKey, Segment, Customer_Name)
    VALUES (%s, %s, %s);
"""

# Insert Data into `fact_sales`
INSERT_SALES_QUERY = """
    INSERT INTO fact_sales (Order_ID, ProductKey, CustomerKey, Sales, OrderDateKey, ShippingKey, RegionKey)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
