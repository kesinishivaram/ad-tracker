import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import requests
import time
import json
from x_ads_scraper import download_and_extract_csv, filter_by_advertiser, standardize_columns

st.set_page_config(layout="wide")

st.markdown("<h1 style='text-align: center;'>Political Ads Tracker</h1>", unsafe_allow_html=True)

st.markdown("<h2 style='text-align: left;'><span style='color: #4285F4;'>G</span><span style='color: #EA4335;'>o</span><span style='color: #FBBC05;'>o</span><span style='color: #4285F4;'>g</span><span style='color: #EA4335;'>l</span><span style='color: #FBBC05;'>e</span></h2>", unsafe_allow_html=True)

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
        st.dataframe(df, column_config={
            "Ad Url": st.column_config.LinkColumn()
        }, use_container_width=True, height=400)

        csv = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{advertiser_name}_google_ads.csv",
            mime="text/csv",
        )
    else:
        st.warning("No results found.")

st.markdown("<h2 style='text-align: left;'><span style='color: #0084F3;'>M</span><span style='color: #0084F3;'>e</span><span style='color: #0084F3;'>t</span><span style='color: #0084F3;'>a</span></h2>", unsafe_allow_html=True)

meta_advertiser_name = st.text_input("Enter Advertiser Name", "", key="meta_advertiser")

@st.cache_data(ttl=86400)
def fetch_meta_ads(advertiser_name):
    meta_access_token = st.secrets["meta_access_token"]

    base_url = "https://graph.facebook.com/v17.0/ads_archive"
    fields = (
        "id,page_id,page_name,bylines,"
        "ad_creation_time,ad_delivery_start_time,ad_delivery_stop_time,"
        "ad_creative_bodies,ad_creative_link_titles,ad_snapshot_url,"
        "spend,impressions,currency,"
        "ad_reached_countries,delivery_by_region,publisher_platforms,demographic_distribution"
    )

    try:
        countries_param = json.dumps(["US"])

        params = {
            "access_token": meta_access_token,
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "ad_reached_countries": countries_param,
            "fields": fields,
            "limit": 100,
            "search_terms": advertiser_name,
        }

        all_ads = []
        url = base_url
        page_count = 0
        max_pages = 10

        while True:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                code = data["error"].get("code")
                if code == 613:
                    time.sleep(60)
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                else:
                    st.error(f"API Error: {data['error'].get('message')}")
                    return pd.DataFrame()

            batch = data.get("data", [])

            matched = []
            for ad in batch:
                page_name = (ad.get("page_name") or "").lower()
                if advertiser_name.lower() in page_name:
                    matched.append(ad)

            if matched:
                all_ads.extend(matched)

            page_count += 1

            paging = data.get("paging", {})
            next_url = paging.get("next")
            if not next_url or page_count >= max_pages:
                break

            url = next_url
            params = {}
            time.sleep(0.5)

        if not all_ads:
            return pd.DataFrame()

        rows = []
        for ad in all_ads:
            demo = ad.get("demographic_distribution") or {}
            gender_targeting = None
            age_targeting = None
            if isinstance(demo, dict):
                gender_targeting = demo.get("gender") or demo.get("genders")
                age_targeting = demo.get("age") or demo.get("ages")
                if gender_targeting is None:
                    gender_targeting = json.dumps(demo)
                if age_targeting is None:
                    age_targeting = json.dumps(demo)
            else:
                gender_targeting = str(demo)
                age_targeting = str(demo)
            delivery_by_region = ad.get("delivery_by_region") or []
            geo_targeting = ""
            if isinstance(delivery_by_region, list):
                regions = [region.get("region", "") for region in delivery_by_region if isinstance(region, dict)]
                geo_targeting = ", ".join(regions) if regions else ""

            row = {
                "Advertiser Name": ad.get("page_name") or advertiser_name,
                "Ad Id": ad.get("id", ""),
                "Ad Url": ad.get("ad_snapshot_url", ""),
                "Start Date": ad.get("ad_delivery_start_time", ""),
                "End Date": ad.get("ad_delivery_stop_time", ""),
                "Ad Type": "POLITICAL_AND_ISSUE_ADS",
                "Geography Targeting": geo_targeting,
                "Gender Targeting": gender_targeting,
                "Age Targeting": age_targeting,
                "Impressions": ad.get("impressions", ""),
                "Spend": ad.get("spend", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        try:
            df["Start Date"] = pd.to_datetime(df["Start Date"]) 
        except Exception:
            pass
        return df

    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching Meta ads: {e}")
        return pd.DataFrame()

if meta_advertiser_name:
    with st.spinner("Fetching Meta advertiser data..."):
        df_meta = fetch_meta_ads(meta_advertiser_name)
    
    if not df_meta.empty:
        st.success(f"Returned {len(df_meta)} records")
        df_meta = df_meta.sort_values("Start Date", ascending=False)
        st.dataframe(df_meta, column_config={
            "Ad Url": st.column_config.LinkColumn()
        }, use_container_width=True, height=400)
        
        csv = df_meta.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{meta_advertiser_name}_meta_political_ads.csv",
            mime="text/csv",
        )
    else:
        st.warning("No results found.")


st.header("X")

x_advertiser_name = st.text_input("Enter Advertiser Name", "", key="x_advertiser")

@st.cache_data(ttl=86400)
def fetch_x_ads(advertiser_name):
    try:
        df = download_and_extract_csv()
        df = standardize_columns(df)
        
        if advertiser_name:
            df = filter_by_advertiser(df, advertiser_name)
        
        if 'Start Date' in df.columns:
            try:
                df['Start Date'] = pd.to_datetime(df['Start Date'])
                df = df.sort_values('Start Date', ascending=False)
            except Exception as e:
                st.warning(f"Could not parse dates: {e}")
        
        return df
    
    except Exception as e:
        st.error(f"Error fetching X political ads data: {e}")
        return pd.DataFrame()

if x_advertiser_name:
    with st.spinner("Fetching X advertiser data..."):
        df_x_filtered = fetch_x_ads(x_advertiser_name)
    
    if not df_x_filtered.empty:
        st.success(f"Returned {len(df_x_filtered)} records")
        
        st.dataframe(df_x_filtered, column_config={
            "Ad Url": st.column_config.LinkColumn()
        }, use_container_width=True, height=400)
        
        csv = df_x_filtered.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{x_advertiser_name}_x_political_ads.csv",
            mime="text/csv",
        )
    else:
        st.warning("No X political ads found for this advertiser. Data is updated every 2 days from X's official disclosure page.")


st.markdown("**Download combined CSV**")


def _gather_datasets():
    parts = []
    if "df" in globals() and isinstance(globals().get("df"), pd.DataFrame) and not globals().get("df").empty:
        df_copy = globals().get("df").copy()
        df_copy["Platform"] = "Google"
        parts.append(df_copy)
    if "df_meta" in globals() and isinstance(globals().get("df_meta"), pd.DataFrame) and not globals().get("df_meta").empty:
        df_meta_copy = globals().get("df_meta").copy()
        df_meta_copy["Platform"] = "Meta"
        parts.append(df_meta_copy)
    if "df_x_filtered" in globals() and isinstance(globals().get("df_x_filtered"), pd.DataFrame) and not globals().get("df_x_filtered").empty:
        df_x_copy = globals().get("df_x_filtered").copy()
        df_x_copy["Platform"] = "X"
        parts.append(df_x_copy)
    return parts

all_parts = _gather_datasets()
if all_parts:
    combined = pd.concat(all_parts, ignore_index=True, sort=False)
    csv_all = combined.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download combined CSV",
        data=csv_all,
        file_name="all_ads_combined.csv",
        mime="text/csv",
    )
else:
    st.info("No datasets available to combine. Fetch Google, Meta, or X results first.")