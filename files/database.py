import mysql.connector

# ✅ MySQL Configuration (Update Your Credentials)
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",  # Change if needed
    "password": "Rasheeda@123",  # Replace with your actual password
    "database": "supply_chaindb"
}

# ✅ SQL Script to Create Tables
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

def create_tables(cursor):
    """Create all required tables in the database."""
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS supply_chaindb")
        cursor.execute("USE supply_chaindb")

        for statement in SQL_SCRIPT.strip().split(";"):
            if statement.strip():  
                cursor.execute(statement)

        print("✅ Tables created successfully!")

    except mysql.connector.Error as e:
        print(f"❌ Error while creating tables: {e}")
        
"""
def save_and_insert_to_database(cursor, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
    ""Insert data into MySQL tables.""
    try:
        # ✅ Insert Customers
        for _, row in dim_customer.iterrows():
            cursor.execute(""
                INSERT INTO dim_customer (Customer_ID, Customer_Name, Segment)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE Customer_Name=VALUES(Customer_Name), Segment=VALUES(Segment)
            "", (row["Customer_ID"], row["Customer_Name"], row["Segment"]))

        # ✅ Insert Products
        for _, row in dim_product.iterrows():
            cursor.execute(""
                INSERT INTO dim_product (Product_ID, Category, Sub_Category, Product_Name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Category=VALUES(Category), Sub_Category=VALUES(Sub_Category), Product_Name=VALUES(Product_Name)
            "", (row["Product_ID"], row["Category"], row["Sub_Category"], row["Product_Name"]))

        # ✅ Insert Shipping
        for _, row in dim_shipping.iterrows():
            cursor.execute(""
                INSERT INTO dim_shipping (Ship_Mode)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE Ship_Mode=VALUES(Ship_Mode)
            "", (row["Ship_Mode"],))

        # ✅ Insert Region
        for _, row in dim_region.iterrows():
            cursor.execute(""
                INSERT INTO dim_region (Country, City, State, Postal_Code, Region)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE City=VALUES(City), State=VALUES(State), Postal_Code=VALUES(Postal_Code), Region=VALUES(Region)
            "", (row["Country"], row["City"], row["State"], row["Postal_Code"], row["Region"]))

        # ✅ Insert Dates
        for _, row in dim_date.iterrows():
            cursor.execute(""
                INSERT INTO dim_date (DateKey, Date, Day, Month, Quarter, Year, Month_Name, Day_Of_Week, Day_Of_Year, Is_Weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Date=VALUES(Date), Day=VALUES(Day), Month=VALUES(Month), Quarter=VALUES(Quarter),
                Year=VALUES(Year), Month_Name=VALUES(Month_Name), Day_Of_Week=VALUES(Day_Of_Week), Day_Of_Year=VALUES(Day_Of_Year), Is_Weekend=VALUES(Is_Weekend)
            "", (row["DateKey"], row["Date"], row["Day"], row["Month"], row["Quarter"], row["Year"],
                  row["Month_Name"], row["Day_Of_Week"], row["Day_Of_Year"], row["Is_Weekend"]))

        # ✅ Insert Fact Sales Data
        for _, row in fact_sales.iterrows():
            cursor.execute(""
                INSERT INTO fact_sales (Order_ID, OrderDateKey, ShipDateKey, CustomerKey, ProductKey, ShippingKey, RegionKey, Sales)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Sales=VALUES(Sales)
            "", (row["Order_ID"], row["OrderDateKey"], row["ShipDateKey"], row["CustomerKey"],
                  row["ProductKey"], row["ShippingKey"], row["RegionKey"], row["Sales"]))

        print("✅ Data inserted into MySQL tables successfully!")

    except mysql.connector.Error as e:
        print(f"❌ Error while inserting data: {e}")
"""


def save_and_insert_to_database(cursor, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
    """Insert data into MySQL tables."""
    try:
        # ✅ Insert Products FIRST
        for _, row in dim_product.iterrows():
            cursor.execute("""
                INSERT INTO dim_product (Product_ID, Category, Sub_Category, Product_Name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Category=VALUES(Category), Sub_Category=VALUES(Sub_Category), Product_Name=VALUES(Product_Name)
            """, (row["Product_ID"], row["Category"], row["Sub_Category"], row["Product_Name"]))

        # ✅ Insert Customers
        for _, row in dim_customer.iterrows():
            cursor.execute("""
                INSERT INTO dim_customer (Customer_ID, Customer_Name, Segment)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE Customer_Name=VALUES(Customer_Name), Segment=VALUES(Segment)
            """, (row["Customer_ID"], row["Customer_Name"], row["Segment"]))

        # ✅ Insert Fact Sales LAST (after dimensions)
        for _, row in fact_sales.iterrows():
            cursor.execute("""
                INSERT INTO fact_sales (Order_ID, OrderDateKey, ShipDateKey, CustomerKey, ProductKey, ShippingKey, RegionKey, Sales)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Sales=VALUES(Sales)
            """, (row["Order_ID"], row["OrderDateKey"], row["ShipDateKey"], row["CustomerKey"],
                  row["ProductKey"], row["ShippingKey"], row["RegionKey"], row["Sales"]))

        print("✅ Data inserted into MySQL tables successfully!")

    except mysql.connector.Error as e:
        print(f"❌ Error while inserting data: {e}")

if __name__ == "__main__":
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            create_tables(cursor)  # ✅ Create tables
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Star Schema created successfully!")
    except mysql.connector.Error as e:
        print(f"❌ Error: {e}")
