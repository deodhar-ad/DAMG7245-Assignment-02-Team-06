import streamlit as st
import requests
import pandas as pd
import io
import plotly.express as px
import plotly.graph_objects as go

# Backend FastAPI URL
BASE_URL = "https://damg7245-assignment-2-team-6.onrender.com"

# Streamlit App Title
st.title("📊 SEC Financial Data")
st.header("Assignment 2 - Team 6")

tab_json, tab_denorm, tab_raw  = st.tabs(["JSON", "Denormalized", "Raw Data"])

with tab_json:
    # Fetch JSON Data from View
    st.header("📄 View JSON")
    if st.button("Fetch JSON Data"):
        url = f"{BASE_URL}/json_view/"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()["json_view_data"]
            df = pd.DataFrame(data)

            # Limit displayed data to 100 rows
            st.write("### Displaying first 20 rows")
            st.dataframe(df.head(20))  

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


with tab_denorm:
    st.header("Denormalized Tables Preview & Visualizations")
    
    # Create a dropdown for table selection
    table_choice = st.selectbox("Select Table", ["balance_sheet", "income_statement", "cash_flow"])
    
    # Build the preview endpoint. Here we assume your FastAPI endpoint is of the form:
    #   GET /denormalized/preview/<table_name>
    preview_endpoint = f"{BASE_URL}/denormalized/preview/{table_choice}"
    
    st.info(f"Fetching preview data for **{table_choice}** ...")
    response = requests.get(preview_endpoint)
    if response.status_code == 200:
        data = response.json().get("data")
        columns = response.json().get("columns")
        df = pd.DataFrame()
        if columns and data:
            df = pd.DataFrame(data,columns=columns)
            st.subheader("Preview of first 20 rows")
            st.dataframe(df)
        else:
            st.warning("No data available to display")
        
        # Visualization based on table selection:
        if table_choice == "balance_sheet":
            # For balance_sheet, we assume columns exist like:
            # company_name, total_assets, total_liabilities, total_equity.
            if not df.empty and {"COMPANY_NAME", "TOTAL_ASSETS", "TOTAL_LIABILITIES", "TOTAL_EQUITY"}.issubset(set(df.columns)):
                st.subheader("Balance Sheet: Stacked Bar Chart")

                # Create a stacked bar chart with company on the x-axis
                fig = px.bar(
                    df,
                    x="COMPANY_NAME",
                    y=["TOTAL_ASSETS", "TOTAL_LIABILITIES", "TOTAL_EQUITY"],
                    title="Balance Sheet Components by Company",
                    labels={"value": "Amount", "variable": "Component"}
                )
                fig.update_layout(width=400, height=500)
                st.plotly_chart(fig)
            else:
                st.warning("Required columns not available for balance_sheet visualization.")
        
        elif table_choice == "income_statement":
            # For income_statement, assume columns like revenue, operating_income, and net_income.
            if not df.empty and {"REVENUE", "OPERATING_INCOME", "NET_INCOME"}.issubset(set(df.columns)):
                st.subheader("Income Statement: Waterfall Chart")
                valid_rows = df[df[["REVENUE", "OPERATING_INCOME", "NET_INCOME"]].notnull().all(axis=1)]
                row = valid_rows.iloc[0]
                # Measures: revenue as absolute, then operating and net income as relative changes
                labels = ["Revenue", "Operating Income", "Net Income"]
                values = [row["REVENUE"], row["OPERATING_INCOME"], row["NET_INCOME"]]
                # Calculate dynamic y-axis range
                min_value = min(values) * 1.2 if min(values) < 0 else min(values)*0.8
                max_value = max(values) * 1.2
                measures = ["absolute", "relative", "relative"]
                fig = go.Figure(go.Waterfall(
                    orientation="v",
                    measure=measures,
                    x=labels,
                    y=values,
                    connector={"line": {"color": "rgb(63, 63, 63)"}},
                    width=0.6
                ))
                fig.update_layout(title="Income Statement Waterfall",
                                  width=400,height=500,
                                  yaxis=dict(title="Amount",range=[min_value, max_value],tickformat=".2s"),
                                  xaxis=dict(title="Income Statement Components"),
                                  margin=dict(l=50, r=50, t=50, b=50)
                                  )
                st.plotly_chart(fig)
            else:
                st.warning("Required columns not available for income_statement visualization.")
        
        elif table_choice == "cash_flow":
            # For cash_flow, assume columns like operating_cash_flow, investing_cash_flow, financing_cash_flow.
            if not df.empty and {"OPERATING_CASH_FLOW", "INVESTING_CASH_FLOW", "FINANCING_CASH_FLOW"}.issubset(set(df.columns)):
                st.subheader("Cash Flow: Waterfall Chart")
                valid_rows = df[df[["OPERATING_CASH_FLOW", "INVESTING_CASH_FLOW", "FINANCING_CASH_FLOW"]].notnull().all(axis=1)]
                row = valid_rows.iloc[0]
                labels = ["Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow"]
                values = [row["OPERATING_CASH_FLOW"], row["INVESTING_CASH_FLOW"], row["FINANCING_CASH_FLOW"]]
                # Calculate dynamic y-axis range
                min_value = min(values) * 1.2 if min(values) < 0 else min(values)*0.8
                max_value = max(values) * 1.2
                measures = ["absolute", "relative", "relative"]
                fig = go.Figure(go.Waterfall(
                    orientation="v",
                    measure=measures,
                    x=labels,
                    y=values,
                    connector={"line": {"color": "rgb(63, 63, 63)"}},
                    width=0.6
                ))
                fig.update_layout(title="Cash Flow Waterfall",
                                  width=400,height=500,
                                  yaxis=dict(title="Amount",range=[min_value, max_value],tickformat=".2s"),
                                  xaxis=dict(title="Cash Flow Components"),
                                  margin=dict(l=50, r=50, t=50, b=50)
                                  )
                st.plotly_chart(fig)
            else:
                st.warning("Required columns not available for cash_flow visualization.")
    else:
        st.error("Failed to load preview data.")
    
    # Optionally, add a download button using an endpoint for the full file.
    download_url = f"{BASE_URL}/denormalized/download/{table_choice}"
    csv_data = requests.get(download_url).content
    st.download_button(
        label="Download Full Data as CSV",
        data=csv_data,
        file_name=f"{table_choice}.csv",
        mime="text/csv"
    )

with tab_raw:
    st.header("Raw Data Preview & Visualizations")

    # Create a dropdown for selecting raw data tables
    raw_tables = ["sec_numbers", "sec_submissions", "sec_tags", "sec_presentation"]
    table_choice_raw = st.selectbox("Select Raw Data Table", raw_tables)

    # Build the preview endpoint
    preview_endpoint_raw = f"{BASE_URL}/normalized/preview/{table_choice_raw}"

    st.info(f"Fetching preview data for **{table_choice_raw}** ...")
    response = requests.get(preview_endpoint_raw)

    if response.status_code == 200:
        data = response.json().get("data")
        columns = response.json().get("columns")
        df = pd.DataFrame()

        if columns and data:
            df = pd.DataFrame(data, columns=columns)
            st.subheader("Preview of first 20 rows")
            st.dataframe(df)
        else:
            st.warning("No data available to display")

        # Visualizations based on table selection
        viz_placeholder = st.empty()
        if table_choice_raw == "sec_numbers":
            if not df.empty and {"TAG", "VALUE"}.issubset(set(df.columns)):
                viz_placeholder.subheader("Top 10 Reported Values by Tag")
                
                top_tags = df.groupby("TAG")["VALUE"].sum().nlargest(10).reset_index()
                fig = px.bar(
                    top_tags,
                    x="TAG",
                    y="VALUE",
                    title="Top 10 SEC Reported Values",
                    labels={"VALUE": "Reported Value", "TAG": "Tag"},
                )
                fig.update_layout(width=600, height=500)
                viz_placeholder.plotly_chart(fig)
            else:
                viz_placeholder.warning("Required columns not available for sec_numbers visualization.")

        elif table_choice_raw == "sec_submissions":
            if not df.empty and {"COUNTRYBA"}.issubset(set(df.columns)):
                viz_placeholder.subheader("Submissions by Country")
                
                country_counts = df["COUNTRYBA"].value_counts().reset_index()
                country_counts.columns = ["Country", "Submissions"]
                
                fig = px.pie(
                    country_counts,
                    names="Country",
                    values="Submissions",
                    title="SEC Submissions by Country",
                )
                fig.update_layout(width=500, height=500)
                viz_placeholder.plotly_chart(fig)
            else:
                viz_placeholder.warning("Required columns not available for sec_submissions visualization.")

        elif table_choice_raw == "sec_tags":
            if not df.empty and {"DATATYPE"}.issubset(set(df.columns)):
                viz_placeholder.subheader("Tag Data Types Distribution")
                
                datatype_counts = df["DATATYPE"].value_counts().reset_index()
                datatype_counts.columns = ["Data Type", "Count"]
                
                fig = px.bar(
                    datatype_counts,
                    x="Data Type",
                    y="Count",
                    title="Distribution of SEC Tag Data Types",
                    labels={"Count": "Number of Tags"},
                )
                fig.update_layout(width=600, height=500)
                viz_placeholder.plotly_chart(fig)
            else:
                viz_placeholder.warning("Required columns not available for sec_tags visualization.")
        else: viz_placeholder.error("No Visulizations for this table")

    else:
        st.error("Failed to load preview data.")

    # Download Button
    download_url_raw = f"{BASE_URL}/normalized/download/{table_choice_raw}"
    csv_data_raw = requests.get(download_url_raw).content
    st.download_button(
        label="Download Full Raw Data as CSV",
        data=csv_data_raw,
        file_name=f"{table_choice_raw}.csv",
        mime="text/csv"
    )