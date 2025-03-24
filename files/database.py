import mysql.connector

# ✅ MySQL Configuration (Update Your Credentials)
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",  # Change if needed
    "password": "Rasheeda@123",  # Replace with your actual password
    "database": "supply_chaindb"
}

# ✅ Corrected SQL Script (Use `--` for comments, ensure spacing)
SQL_SCRIPT = """
CREATE DATABASE IF NOT EXISTS supply_chaindb;
USE supply_chaindb;

-- Customer Dimension Table
CREATE TABLE IF NOT EXISTS dim_customer (
    CustomerKey INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID VARCHAR(50) UNIQUE NOT NULL,
    Customer_Name VARCHAR(255),
    Segment VARCHAR(50)
);

-- Product Dimension Table
CREATE TABLE IF NOT EXISTS dim_product (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(50) UNIQUE NOT NULL,
    Category VARCHAR(100),
    Sub_Category VARCHAR(100),
    Product_Name VARCHAR(255)
);

-- Shipping Dimension Table
CREATE TABLE IF NOT EXISTS dim_shipping (
    ShippingKey INT AUTO_INCREMENT PRIMARY KEY,
    Ship_Mode VARCHAR(50) UNIQUE NOT NULL
);

-- Region Dimension Table
CREATE TABLE IF NOT EXISTS dim_region (
    RegionKey INT AUTO_INCREMENT PRIMARY KEY,
    Country VARCHAR(100),
    City VARCHAR(100),
    State VARCHAR(100),
    Postal_Code VARCHAR(20),
    Region VARCHAR(50)
);

-- Date Dimension Table
CREATE TABLE IF NOT EXISTS dim_date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT,
    Month_Name VARCHAR(20),
    Day_Of_Week VARCHAR(20),
    Day_Of_Year INT,
    Is_Weekend BOOLEAN
);

-- Fact Table (Sales Transactions)
CREATE TABLE IF NOT EXISTS fact_sales (
    Order_ID VARCHAR(50) PRIMARY KEY,
    OrderDateKey INT,
    ShipDateKey INT,
    CustomerKey INT,
    ProductKey INT,
    ShippingKey INT,
    RegionKey INT,
    Sales DECIMAL(10,2),
    FOREIGN KEY (OrderDateKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY (ShipDateKey) REFERENCES dim_date(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES dim_customer(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES dim_product(ProductKey),
    FOREIGN KEY (ShippingKey) REFERENCES dim_shipping(ShippingKey),
    FOREIGN KEY (RegionKey) REFERENCES dim_region(RegionKey)
);
"""
def get_db_connection():
    """Establish and return a MySQL database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Database Connection Error: {e}")
        return None
try:
    # ✅ Connect to MySQL Server
    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )

    cursor = conn.cursor()

    # ✅ Create Database First
    cursor.execute("CREATE DATABASE IF NOT EXISTS supply_chaindb")
    cursor.execute("USE supply_chaindb")

    # ✅ Execute SQL Commands One by One
    for statement in SQL_SCRIPT.strip().split(";"):
        if statement.strip():  
            cursor.execute(statement)

    conn.commit()
    print("✅ Star Schema created successfully!")

except mysql.connector.Error as e:
    print(f"❌ Error: {e}")

finally:
    # ✅ Ensure Safe Closing
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()
