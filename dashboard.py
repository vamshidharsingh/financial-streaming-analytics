import os
import pandas as pd
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "adminpassword")
DB_NAME = os.getenv("DB_NAME", "financial_data")

st.set_page_config(
    page_title="Real-Time Financial Dashboard",
    page_icon="💸",
    layout="wide"
)

def get_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        query = "SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 1000;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

st.title("Real-Time Financial Data Analytics Pipeline")

df = get_data()

if not df.empty:
    st.write("### Live Transactions (Last 1000)")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="Total Transactions", value=len(df))
    
    with col2:
        total_volume = df['amount'].sum()
        st.metric(label="Total Volume (Mixed Currency)", value=f"{total_volume:,.2f}")
        
    with col3:
        success_rate = (len(df[df['status'] == 'completed']) / len(df)) * 100
        st.metric(label="Success Rate", value=f"{success_rate:.2f}%")
        
    st.write("### Transactions Over Time")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    time_df = df.set_index('timestamp').resample('1T').size()
    st.line_chart(time_df)
    
    st.write("### Transactions by Type")
    type_counts = df['transaction_type'].value_counts()
    st.bar_chart(type_counts)
    
    st.write("### Raw Data")
    st.dataframe(df)
else:
    st.warning("No data available yet. Ensure the producer and consumer are running.")
