import streamlit as st
import requests
import pandas as pd
import io

# Backend FastAPI URL
BACKEND_URL = "http://localhost:8000"

# Streamlit App Title
st.title("📊 SEC Financial Data")
st.header("Assignment 2 - Team 6")

# Fetch JSON Data from View
st.header("📄 View JSON")
if st.button("Fetch JSON Data"):
    url = f"{BACKEND_URL}/json_view/"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()["json_view_data"]
        df = pd.DataFrame(data)

        # Limit displayed data to 100 rows
        st.write("### Displaying first 100 rows")
        st.dataframe(df.head(100))  

        # Enable CSV download without storing
        if not df.empty:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name="financial_data.csv",
                mime="text/csv"
            )
    else:
        st.error("No data found in the view.")
