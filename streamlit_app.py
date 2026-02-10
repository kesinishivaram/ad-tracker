import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import date

st.title("Google Political Ads – Advertiser Intelligence Dashboard")

# ---- AUTH ----
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# ---- USER INPUTS ----
advertiser_keyword = st.text_input("Advertiser Name Contains", "")
state_filter = st.text_input("State Code (e.g. TX, CA, NY)", "TX")
min_spend = st.number_input("Minimum Total US Spend ($)", value=0)

# ---- QUERY FUNCTION ----
@st.cache_data(ttl=600)
def run_query(advertiser_keyword, state_filter, min_spend):

    query = """
    WITH advertiser_base AS (
      SELECT
        advertiser_id,
        advertiser_name,
        spend_usd AS total_us_spend,
        total_creatives
      FROM `bigquery-public-data.google_political_ads.advertiser_stats`
      WHERE spend_usd > @min_spend
        AND LOWER(advertiser_name) LIKE LOWER(@advertiser_keyword)
    ),

    state_spend AS (
      SELECT
        advertiser_id,
        country_subdivision_primary AS state_code,
        spend_usd AS advertiser_state_spend_usd
      FROM `bigquery-public-data.google_political_ads.advertiser_geo_spend`
      WHERE country = 'US'
        AND country_subdivision_primary = @state_filter
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
        (spend_range_min_usd + spend_range_max_usd)/2 AS ad_spend_midpoint_usd,
        geo_targeting_included,
        age_targeting,
        gender_targeting
      FROM `bigquery-public-data.google_political_ads.creative_stats`
    )

    SELECT
      a.advertiser_id,
      a.advertiser_name,
      a.total_us_spend,
      a.total_creatives,
      s.state_code,
      s.advertiser_state_spend_usd,
      c.ad_id,
      c.ad_type,
      c.ad_url,
      c.date_range_start,
      c.date_range_end,
      c.impressions,
      c.spend_range_min_usd,
      c.spend_range_max_usd,
      c.ad_spend_midpoint_usd,
      c.geo_targeting_included,
      c.age_targeting,
      c.gender_targeting
    FROM advertiser_base a
    LEFT JOIN state_spend s
      ON a.advertiser_id = s.advertiser_id
    LEFT JOIN creatives c
      ON a.advertiser_id = c.advertiser_id
    ORDER BY a.total_us_spend DESC
    LIMIT 500
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "advertiser_keyword", "STRING", f"%{advertiser_keyword}%"
            ),
            bigquery.ScalarQueryParameter(
                "state_filter", "STRING", state_filter
            ),
            bigquery.ScalarQueryParameter(
                "min_spend", "FLOAT64", float(min_spend)
            ),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    rows_raw = query_job.result()
    return [dict(row) for row in rows_raw]


# ---- RUN BUTTON ----
if st.button("Run Intelligence Query"):
    with st.spinner("Querying political ad data..."):
        rows = run_query(advertiser_keyword, state_filter, min_spend)

    if rows:
        st.success(f"Returned {len(rows)} records")
        st.dataframe(rows)
    else:
        st.warning("No results found.")
