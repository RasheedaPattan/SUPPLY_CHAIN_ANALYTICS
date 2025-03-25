import streamlit as st
import pandas as pd
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt
from config import DB_CONFIG
from queries import *
from data_fetcher import get_data

# ✅ Streamlit Page Configuration
st.set_page_config(page_title="📊 Supply Chain Dashboard", layout="wide")

# 🎨 Custom Styling for a Better Look
st.markdown("""
    <style>
    .sidebar .sidebar-content { background-color: #f5f5f5; padding: 10px; }
    .stApp { background-color: #fafafa; }
    .css-1d391kg { font-size: 16px; }
    .stMarkdown { font-size: 16px; }
    </style>
""", unsafe_allow_html=True)

# 🎯 Sidebar Navigation
st.sidebar.title("📌 Navigation")
page = st.sidebar.radio("Select Analysis", 
                        ["📈 Sales Trends", "📊 Sales by Category", 
                         "🛍 Customer Segmentation", "🚚 Shipping Mode", "🌎 Regional Sales"])

# ✅ Fetch Data from MySQL
@st.cache_data
def load_data():
    return {
        "sales_trend": get_data(SALES_TREND_QUERY),
        "category_sales": get_data(CATEGORY_SALES_QUERY),
        "customer_segment": get_data(CUSTOMER_SEGMENT_QUERY),
        "shipping_mode": get_data(SHIPPING_MODE_QUERY),
        "region_sales": get_data(REGION_SALES_QUERY)  # ✅ Fetch Updated Regional Sales Data
    }

data = load_data()

# 🎯 Sidebar Filters
st.sidebar.subheader("📊 Data Filters")
selected_year = st.sidebar.selectbox("Select Year", ["All"] + sorted(data["sales_trend"]["Year"].unique()))
selected_region = st.sidebar.selectbox("Select Region", ["All"] + sorted(data["region_sales"]["Region"].unique()))

# ✅ Filtering Data Based on Sidebar Selection
if selected_year != "All":
    data["sales_trend"] = data["sales_trend"][data["sales_trend"]["Year"] == selected_year]
    
    if "Year" in data["region_sales"].columns:  # ✅ Check if Year exists
        data["region_sales"] = data["region_sales"][data["region_sales"]["Year"] == selected_year]  # ✅ Apply Year Filter
    else:
        st.error("⚠️ Year column is missing from region_sales. Check SQL query.")


# 🎯 Dashboard Title
st.title("📊 Supply Chain Data Integration Dashboard")

# 🎯 Dynamic Content Based on Selected Page
if page == "📈 Sales Trends":
    st.subheader("📈 Sales Trends Over Time")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        fig, ax = plt.subplots(figsize=(9, 4))
        sns.lineplot(data=data["sales_trend"], x="Month_Name", y="Total_Sales", hue="Year", marker="o", ax=ax)
        plt.xticks(rotation=45)
        st.pyplot(fig)

    with col2:
        st.metric(label="📊 Total Sales", value=f"${data['sales_trend']['Total_Sales'].sum():,.2f}")
        st.metric(label="📅 Number of Months", value=data['sales_trend']['Month_Name'].nunique())

elif page == "📊 Sales by Category":
    st.subheader("📊 Sales by Category")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=data["category_sales"], x="Category", y="Total_Sales", palette="viridis", ax=ax)
    plt.xticks(rotation=45)
    st.pyplot(fig)

elif page == "🛍 Customer Segmentation":
    st.subheader("🛍 Customer Segmentation by Sales")
    fig, ax = plt.subplots(figsize=(6, 6))
    colors = ["#ff9999", "#66b3ff", "#99ff99", "#ffcc99"]
    ax.pie(data["customer_segment"]["Total_Sales"], 
           labels=data["customer_segment"]["Segment"], autopct="%1.1f%%", colors=colors)
    st.pyplot(fig)

elif page == "🚚 Shipping Mode":
    st.subheader("🚚 Shipping Mode Distribution")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=data["shipping_mode"], x="Ship_Mode", y="Order_Count", palette="coolwarm", ax=ax)
    st.pyplot(fig)

elif page == "🌎 Regional Sales":
    st.subheader("🌎 Regional Sales Analysis")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=data["region_sales"], x="Region", y="Total_Sales", palette="muted", ax=ax)
    plt.xticks(rotation=45)
    st.pyplot(fig)

# 🎯 Success Message
st.sidebar.success("✅ Dashboard Loaded Successfully!")
