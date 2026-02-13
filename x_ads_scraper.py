import requests
import pandas as pd
import zipfile
import io
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

X_DATA_BASE_URL = "https://business.x.com/content/dam/business-twitter/political-ads-data"


def generate_possible_dates(days_back=7):
    dates = []
    today = datetime.now()
    
    for i in range(days_back):
        date = today - timedelta(days=i)
        formatted_date = f"{date.day}-{date.strftime('%B')}-{date.year}"
        dates.append((formatted_date, date))
    
    return dates


def find_latest_data_file():
    possible_dates = generate_possible_dates(days_back=7)
    
    for date_str, date_obj in possible_dates:
        url = f"{X_DATA_BASE_URL}/{date_str}-political-ads-data.zip"
        
        try:
            logger.info(f"Checking for file: {date_str}")
            response = requests.get(url, timeout=10, stream=True, allow_redirects=True)
            
            if response.status_code == 200:
                logger.info(f"Found latest data file: {date_str}")
                return url, date_str
        except requests.RequestException as e:
            logger.debug(f"Date {date_str} not found: {e}")
            continue
    
    logger.warning("Could not find any recent X political ads data file")
    return None, None


def download_and_extract_csv():
    url, date_str = find_latest_data_file()
    
    if not url:
        raise Exception("Could not find latest X political ads data file")
    
    try:
        logger.info(f"Downloading X political ads data from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Extracting CSV from ZIP file")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"Files in ZIP: {file_list}")
            
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                raise Exception(f"No CSV files found in ZIP. Contents: {file_list}")
            
            csv_path = csv_files[0]
            logger.info(f"Reading CSV: {csv_path}")
            
            with zip_file.open(csv_path) as csv_file:
                df = pd.read_csv(csv_file)
        
        logger.info(f"Successfully loaded {len(df)} rows from X political ads data")
        return df
    
    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        raise Exception(f"Failed to download X political ads data: {e}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting ZIP: {e}")
        raise Exception(f"Failed to extract ZIP file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def filter_by_advertiser(df, advertiser_name):
    if not advertiser_name:
        return df
    
    screen_name_columns = [col for col in df.columns if 'screen' in col.lower() or 'name' in col.lower()]
    
    if not screen_name_columns:
        logger.warning("Could not find screen name column. Available columns: " + str(df.columns.tolist()))
        return df
    
    screen_name_col = screen_name_columns[0]
    
    filtered_df = df[df[screen_name_col].str.lower().str.contains(advertiser_name.lower(), na=False)]
    
    return filtered_df


def standardize_columns(df):
    column_mapping = {
        'Screen Name': 'Advertiser Name',
        'Tweet Id': 'Ad Id',
        'Tweet Url': 'Ad Url',
        'Day of Start Date Adgroup': 'Start Date',
        'Day of End Date Adgroup': 'End Date',
        'Targeting Name': 'Ad Type',
        'Interest Targeting': 'Interest Targeting',
        'Geo Targeting': 'Geography Targeting',
        'Gender Targeting': 'Gender Targeting',
        'Age Targeting': 'Age Targeting',
        'Impressions': 'Impressions',
        'Spend_USD': 'Spend',
    }
    
    rename_dict = {}
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            rename_dict[old_col] = new_col
    
    df = df.rename(columns=rename_dict)
    
    return df
