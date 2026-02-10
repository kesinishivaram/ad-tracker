import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import os

st.title("Political Ads Tracker")

st.header("Google Political Ads Tracker")

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

advertiser_name = st.text_input("Enter Advertiser Name", "")


@st.cache_data(ttl=86400)
def run_query(advertiser_name):

    query = """
    WITH advertiser_base AS (
      SELECT
        advertiser_id,
        advertiser_name
      FROM `bigquery-public-data.google_political_ads.advertiser_stats`
      WHERE LOWER(advertiser_name) LIKE LOWER(@advertiser_name)
    ),

    creatives AS (
      SELECT
        ad_id,
        advertiser_id,
        ad_type,
        ad_url,
        date_range_start,
        date_range_end,
        impressions,
        spend_range_min_usd,
        spend_range_max_usd,
        (spend_range_min_usd + spend_range_max_usd)/2 AS spend_usd,
        geo_targeting_included,
        age_targeting,
        gender_targeting
      FROM `bigquery-public-data.google_political_ads.creative_stats`
    )

    SELECT
      a.advertiser_name AS screen_name,
      c.ad_id AS tweet_id,
      c.ad_url AS tweet_url,
      c.date_range_start AS day_of_start_date_adgroup,
      c.date_range_end AS day_of_end_date_adgroup,
      c.ad_type AS targeting_name,
      c.geo_targeting_included AS geo_targeting,
      c.gender_targeting AS gender_targeting,
      c.age_targeting AS age_targeting,
      c.impressions AS impressions,
      c.spend_usd AS spend_usd
    FROM advertiser_base a
    LEFT JOIN creatives c
      ON a.advertiser_id = c.advertiser_id
    ORDER BY c.date_range_start DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "advertiser_name", "STRING", f"%{advertiser_name}%"
            ),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result()

    df = pd.DataFrame([dict(row) for row in rows])

    # Rename columns to have human-friendly names with spaces
    df = df.rename(columns={
        "screen_name": "Advertiser Name",
        "tweet_id": "Ad Id",
        "tweet_url": "Ad Url",
        "day_of_start_date_adgroup": "Start Date",
        "day_of_end_date_adgroup": "End Date",
        "targeting_name": "Ad Type",
        "geo_targeting": "Geography Targeting",
        "gender_targeting": "Gender Targeting",
        "age_targeting": "Age Targeting",
        "impressions": "Impressions",
        "spend_usd": "Spend"
    })

    return df


if advertiser_name:
    with st.spinner("Fetching advertiser data..."):
        df = run_query(advertiser_name)

    if not df.empty:
        st.success(f"Returned {len(df)} records")
        st.dataframe(df)

        csv = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{advertiser_name}_google_ads.csv",
            mime="text/csv",
        )
    else:
        st.warning("No results found.")

st.header("X Political Ads Tracker")

x_advertiser_name = st.text_input("Enter Advertiser Name", "", key="x_advertiser")

try:
    df_x = pd.read_csv("10-february-2026-political-ads-data.csv")
except Exception as e:
    st.error(f"Error reading CSV: {e}")
else:
    # Validate required columns
    required_columns = [
        "Screen Name",
        "Tweet Id",
        "Tweet Url",
        "Day of Start Date Adgroup",
        "Day of End Date Adgroup",
        "Targeting Name",
        "Interest Targeting",
        "Geo Targeting",
        "Gender Targeting",
        "Age Targeting",
        "Impressions",
        "Spend_USD",
    ]
    
    missing_columns = [col for col in required_columns if col not in df_x.columns]
    if missing_columns:
        st.error(f"Missing required columns: {', '.join(missing_columns)}")
    else:
        # Filter by advertiser name if provided
        if x_advertiser_name:
            mask = df_x["Screen Name"].astype(str).str.contains(x_advertiser_name, case=False, na=False)
            df_x_filtered = df_x[mask]
        else:
            df_x_filtered = df_x
        
        if not df_x_filtered.empty:
            st.success(f"Returned {len(df_x_filtered)} records")
            df_x_filtered = df_x_filtered.sort_values("Day of Start Date Adgroup", ascending=False)
            st.dataframe(df_x_filtered)
            
            csv = df_x_filtered.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{x_advertiser_name}_x_political_ads.csv",
                mime="text/csv",
            )
        else:
            st.warning("No results found.")