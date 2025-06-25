#!/usr/bin/env python
# -*- coding: utf-8 -*-
import streamlit as st
import requests
import json
import urllib3
import certifi
import pandas as pd
import logging
import os
from dotenv import load_dotenv
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import ibm_boto3
from ibm_botocore.client import Config
from tenacity import retry, stop_after_attempt, wait_exponential
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
from io import BytesIO
import traceback
import time
from functools import wraps

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

# IBM COS Configuration
COS_API_KEY = os.getenv("COS_API_KEY")
COS_SERVICE_INSTANCE_ID = os.getenv("COS_SERVICE_INSTANCE_ID")
COS_ENDPOINT = os.getenv("COS_ENDPOINT")
COS_BUCKET = os.getenv("COS_BUCKET")

# WatsonX configuration
WATSONX_API_URL = os.getenv("WATSONX_API_URL_1")
MODEL_ID = os.getenv("MODEL_ID_1")
PROJECT_ID = os.getenv("PROJECT_ID_1")
API_KEY = os.getenv("API_KEY_1")

# API Endpoints
LOGIN_URL = "https://dms.asite.com/apilogin/"
IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"

def function_timer(show_args=False):
    """Decorator to measure and display function execution time"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            func_name = func.__name__.replace('_', ' ').title()
            arg_info = f" with args: {args[1:]}" if show_args and args else ""
            st.info(f"⏱️ {func_name}{arg_info} executed in {duration:.2f} seconds")
            return result
        return wrapper
    return decorator

@function_timer()
async def login_to_asite(email, password):
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"emailId": email, "password": password}
    try:
        response = requests.post(LOGIN_URL, headers=headers, data=payload, verify=certifi.where(), timeout=30)
        if response.status_code == 200:
            session_id = response.json().get("UserProfile", {}).get("Sessionid")
            if session_id:
                logger.info(f"Login successful, Session ID: {session_id}")
                st.session_state['session_id'] = session_id
                st.sidebar.success(f"✅ Login successful")
                return session_id
            else:
                logger.error("No Session ID found in login response")
                st.sidebar.error("❌ No Session ID in response")
                return None
        logger.error(f"Login failed: {response.status_code} - {response.text}")
        st.sidebar.error(f"❌ Login failed: {response.status_code}")
        return None
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        st.sidebar.error(f"❌ Login error: {str(e)}")
        return None

# COS File Fetching Function
@function_timer()
def get_cos_files():
    try:
        cos_client = initialize_cos_client()
        if not cos_client:
            st.error("❌ Failed to initialize COS client.")
            return []

        response = cos_client.list_objects_v2(Bucket=COS_BUCKET, Prefix="Veridia/")
        if 'Contents' not in response:
            st.error(f"❌ No files found in the 'Veridia' folder of bucket '{COS_BUCKET}'. Please ensure the folder exists and contains files.")
            logger.error("No objects found in Veridia folder")
            return []

        all_files = [obj['Key'] for obj in response.get('Contents', [])]
        st.write("**All files in Veridia folder:**")
        if all_files:
            st.write("\n".join(all_files))
        else:
            st.write("No files found.")
            logger.warning("Veridia folder is empty")
            return []

        # Pattern for Finishing Tracker files
        finishing_pattern = re.compile(
            r"Veridia/Tower\s*([4|5])\s*Finishing\s*Tracker[\(\s]*(.*?)(?:[\)\s]*\.xlsx)$",
            re.IGNORECASE
        )
        # Pattern for Anti. Slab Cycle file
        slab_cycle_pattern = re.compile(
            r"Veridia/Veridia Anti\. Slab Cycle With Possesion dates.*\.xlsx$",
            re.IGNORECASE
        )

        date_formats = [
            "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"
        ]

        file_info = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            # Check for Finishing Tracker files
            finishing_match = finishing_pattern.match(key)
            if finishing_match:
                tower_num = finishing_match.group(1)
                date_str = finishing_match.group(2).strip('()').strip()
                parsed_date = None
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_date:
                    file_info.append({
                        'key': key,
                        'tower': tower_num,
                        'date': parsed_date,
                        'type': 'finishing'
                    })
                else:
                    logger.warning(f"Could not parse date in filename: {key} (date: {date_str})")
                    st.warning(f"Skipping file with unparseable date: {key}")
            # Check for Anti. Slab Cycle file
            elif slab_cycle_pattern.match(key):
                # No date parsing needed since the filename doesn't include a date
                file_info.append({
                    'key': key,
                    'tower': None,  # No specific tower associated
                    'date': obj['LastModified'],  # Use LastModified timestamp
                    'type': 'slab_cycle'
                })

        if not file_info:
            st.error("❌ No Excel files matched the expected patterns in the 'Veridia' folder. Expected formats: 'Tower 4/5 Finishing Tracker(date).xlsx' or 'Veridia Anti. Slab Cycle With Possesion dates*.xlsx'.")
            logger.error("No files matched the expected patterns")
            return []

        # Separate Finishing and Slab Cycle files
        finishing_files = {}
        slab_cycle_files = []
        for info in file_info:
            if info['type'] == 'finishing':
                tower = info['tower']
                if tower not in finishing_files or info['date'] > finishing_files[tower]['date']:
                    finishing_files[tower] = info
            elif info['type'] == 'slab_cycle':
                slab_cycle_files.append(info)

        # Select the latest Slab Cycle file (if multiple exist)
        if slab_cycle_files:
            latest_slab_file = max(slab_cycle_files, key=lambda x: x['date'])
            files = [info['key'] for info in finishing_files.values()] + [latest_slab_file['key']]
        else:
            files = [info['key'] for info in finishing_files.values()]

        if not files:
            st.error("❌ No valid Excel files found for Tower 4, Tower 5, or Anti. Slab Cycle after filtering.")
            logger.error("No valid files after filtering")
            return []

        st.success(f"Found {len(files)} matching files: {', '.join(files)}")
        return files
    except Exception as e:
        st.error(f"❌ Error fetching COS files: {str(e)}")
        logger.error(f"Error fetching COS files: {str(e)}")
        return []

@function_timer()
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_access_token(api_key):
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": api_key}
    response = requests.post(IAM_TOKEN_URL, headers=headers, data=data, verify=certifi.where(), timeout=30)
    if response.status_code == 200:
        token_info = response.json()
        logger.info("Access token generated successfully")
        return token_info['access_token']
    else:
        logger.error(f"Failed to get access token: {response.status_code} - {response.text}")
        st.error(f"❌ Failed to get access token: {response.status_code}")
        raise Exception("Failed to get access token")

@function_timer()
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def initialize_cos_client():
    try:
        cos_client = ibm_boto3.client(
            's3',
            ibm_api_key_id=COS_API_KEY,
            ibm_service_instance_id=COS_SERVICE_INSTANCE_ID,
            config=Config(signature_version='oauth', connect_timeout=60, read_timeout=60, retries={'max_attempts': 5}),
            endpoint_url=COS_ENDPOINT
        )
        logger.info("COS client initialized successfully")
        return cos_client
    except Exception as e:
        logger.error(f"Error initializing COS client: {str(e)}")
        st.error(f"❌ Error initializing COS client: {str(e)}")
        raise

async def validate_session():
    url = "https://dmsak.asite.com/api/workspace/workspacelist"
    headers = {'Cookie': f'ASessionID={st.session_state.get("session_id", "")}'}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                logger.info("Session validated successfully")
                return True
            else:
                logger.error(f"Session validation failed: {response.status}")
                return False

async def refresh_session_if_needed(email, password):
    if 'session_id' not in st.session_state or not await validate_session():
        logger.info("Session invalid or missing, attempting login")
        new_session_id = await login_to_asite(email, password)
        if new_session_id:
            st.session_state['session_id'] = new_session_id
            return new_session_id
        else:
            raise Exception("Failed to establish/refresh session")
    return st.session_state['session_id']

@function_timer()
async def get_workspace_and_project_ids(email, password):
    await refresh_session_if_needed(email, password)
    headers = {
        'Cookie': f'ASessionID={st.session_state["session_id"]}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    # Fetch Workspace ID
    url = "https://dmsak.asite.com/api/workspace/workspacelist"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        st.session_state['workspace_id'] = response.json()['asiteDataList']['workspaceVO'][4]['Workspace_Id']
        st.write(f"Workspace ID: {st.session_state['workspace_id']}")
    else:
        st.error("❌ Failed to fetch Workspace ID")
        logger.error(f"Failed to fetch Workspace ID: {response.status_code}")
        return False

    # Fetch Project IDs
    url = f"https://adoddleak.asite.com/commonapi/qaplan/getQualityPlanList;searchCriteria={{'criteria': [{{'field': 'planCreationDate','operator': 6,'values': ['11-Mar-2025']}}], 'projectId': {st.session_state['workspace_id']}, 'recordLimit': 1000, 'recordStart': 1}}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'data' in data and data['data']:
            st.session_state['project_ids'] = {
                'finishing': data['data'][4]['planId'],
                'structure': data['data'][6]['planId'],
                'external': data['data'][3]['planId'],
                'lift': data['data'][5]['planId'],
                'common_area': data['data'][2]['planId']
            }
            for key, value in st.session_state['project_ids'].items():
                st.write(f"Veridia - {key.replace('_', ' ').title()} Project ID: {value}")
        else:
            st.error("❌ No project data found")
            return False
    except Exception as e:
        st.error(f"❌ Error fetching Project IDs: {str(e)}")
        return False
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_data(session, url, headers):
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 204:
                logger.info("No content returned (204)")
                return None
            else:
                text = await response.text()
                logger.error(f"Error fetching data: {response.status} - {text[:500]}")
                raise Exception(f"Error fetching data: {response.status}")
    except Exception as e:
        logger.error(f"Fetch failed: {str(e)}")
        raise

@function_timer()
async def fetch_all_data(email, password):
    if 'data' in st.session_state and all(st.session_state['data'].values()):
        st.write("Using cached data from session state")
        return st.session_state['data']
    
    project_types = ['finishing', 'structure', 'external', 'lift', 'common_area']
    data = {pt: {'associations': pd.DataFrame(), 'activities': pd.DataFrame(), 'locations': pd.DataFrame()} for pt in project_types}
    record_limit = 1000
    
    await refresh_session_if_needed(email, password)
    headers = {'Cookie': f'ASessionID={st.session_state["session_id"]}'}
    
    async with aiohttp.ClientSession() as session:
        for project_type in project_types:
            plan_id = st.session_state['project_ids'].get(project_type)
            if not plan_id:
                st.error(f"❌ No plan ID for {project_type}")
                continue

            # Fetch Associations
            start_record = 1
            all_associations = []
            st.write(f"Fetching {project_type.title()} associations...")
            while True:
                url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state['workspace_id']}&planId={plan_id}&recordStart={start_record}&recordLimit={record_limit}"
                try:
                    await refresh_session_if_needed(email, password)
                    headers['Cookie'] = f'ASessionID={st.session_state["session_id"]}'
                    response_data = await fetch_data(session, url, headers)
                    if response_data is None:
                        st.write(f"No more {project_type.title()} association data (204)")
                        break
                    associations = response_data.get('associationList', response_data if isinstance(response_data, list) else [])
                    all_associations.extend(associations)
                    st.write(f"Fetched {len(associations)} {project_type.title()} association records (Total: {len(all_associations)})")
                    if len(associations) < record_limit:
                        break
                    start_record += record_limit
                    await asyncio.sleep(0.5)
                except Exception as e:
                    st.error(f"❌ Error fetching {project_type.title()} associations: {str(e)}")
                    break
            data[project_type]['associations'] = pd.DataFrame(all_associations)

            # Fetch Activities
            start_record = 1
            all_activities = []
            st.write(f"Fetching {project_type.title()} activities...")
            while True:
                url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state['workspace_id']}&planId={plan_id}&recordStart={start_record}&recordLimit={record_limit}"
                try:
                    await refresh_session_if_needed(email, password)
                    headers['Cookie'] = f'ASessionID={st.session_state["session_id"]}'
                    response_data = await fetch_data(session, url, headers)
                    if response_data is None:
                        st.write(f"No more {project_type.title()} activity data (204)")
                        break
                    activities = response_data.get('activityList', response_data if isinstance(response_data, list) else [])
                    all_activities.extend(activities)
                    st.write(f"Fetched {len(activities)} {project_type.title()} activity records (Total: {len(all_activities)})")
                    if len(activities) < record_limit:
                        break
                    start_record += record_limit
                    await asyncio.sleep(0.5)
                except Exception as e:
                    st.error(f"❌ Error fetching {project_type.title()} activities: {str(e)}")
                    break
            data[project_type]['activities'] = pd.DataFrame(all_activities)

            # Fetch Locations
            start_record = 1
            all_locations = []
            st.write(f"Fetching {project_type.title()} locations...")
            while True:
                url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state['workspace_id']}&planId={plan_id}&recordStart={start_record}&recordLimit={record_limit}"
                try:
                    await refresh_session_if_needed(email, password)
                    headers['Cookie'] = f'ASessionID={st.session_state["session_id"]}'
                    response_data = await fetch_data(session, url, headers)
                    if response_data is None:
                        st.write(f"No more {project_type.title()} location data (204)")
                        break
                    locations = response_data.get('locationList', response_data if isinstance(response_data, list) else [])
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} for loc in locations]
                    all_locations.extend(location_data)
                    st.write(f"Fetched {len(location_data)} {project_type.title()} location records (Total: {len(all_locations)})")
                    if len(location_data) < record_limit:
                        break
                    start_record += record_limit
                    await asyncio.sleep(0.5)
                except Exception as e:
                    st.error(f"❌ Error fetching {project_type.title()} locations: {str(e)}")
                    break
            data[project_type]['locations'] = pd.DataFrame(all_locations)

    # Process and store data
    desired_columns = ['activitySeq', 'qiLocationId', 'statusName']
    for project_type in project_types:
        df = data[project_type]['associations']
        if 'statusName' not in df.columns and 'statusColor' in df.columns:
            status_mapping = {'#4CAF50': 'Completed', '#4CB0F0': 'Not Started', '#4C0F0': 'Not Started'}
            df['statusName'] = df['statusColor'].map(status_mapping).fillna('Unknown')
        data[project_type]['associations'] = df[desired_columns] if not df.empty else pd.DataFrame(columns=desired_columns)
        data[project_type]['activities'] = data[project_type]['activities'][['activityName', 'activitySeq', 'formTypeId']] if not data[project_type]['activities'].empty else pd.DataFrame(columns=['activityName', 'activitySeq', 'formTypeId'])
        
        st.write(f"VERIDIA {project_type.upper()} DATA ({', '.join(desired_columns)})")
        st.write(f"Total records: {len(data[project_type]['associations'])}")
        st.write(data[project_type]['associations'])
        st.write(f"VERIDIA {project_type.upper()} ACTIVITY DATA")
        st.write(f"Total records: {len(data[project_type]['activities'])}")
        st.write(data[project_type]['activities'])
        st.write(f"VERIDIA {project_type.upper()} LOCATION DATA")
        st.write(f"Total records: {len(data[project_type]['locations'])}")
        st.write(data[project_type]['locations'])

    st.session_state['data'] = data
    return data

@function_timer()
def process_data(df, activity_df, location_df, dataset_name):
    completed = df[df['statusName'] == 'Completed'].copy()
    asite_activities = [
        "Wall Conducting", "Plumbing Works", "POP & Gypsum Plaster", "Wiring & Switch Socket",
        "Slab Conducting", "Electrical Cable", "Door/Window Frame", "Waterproofing - Sunken",
        "Wall Tile", "Floor Tile", "Door/Window Shutter", "Shuttering", "Reinforcement",
        "Sewer Line", "Rain Water/Storm Line", "Granular Sub-base", "WMM",
        "Saucer drain/Paver block", "Kerb Stone", "Concreting"
    ]
    count_table = pd.DataFrame({'Count': [0] * len(asite_activities)}, index=asite_activities)
    
    if completed.empty:
        logger.warning(f"No completed activities found in {dataset_name} data")
        return pd.DataFrame(), 0, count_table

    completed = completed.merge(location_df[['qiLocationId', 'name']], on='qiLocationId', how='left')
    completed = completed.merge(activity_df[['activitySeq', 'activityName']], on='activitySeq', how='left')
    
    if completed['name'].isna().all():
        logger.error(f"All 'name' values are missing in {dataset_name} data")
        st.error(f"❌ All 'name' values are missing in {dataset_name} data")
        completed['name'] = 'Unknown'
    else:
        completed['name'] = completed['name'].fillna('Unknown')

    completed['tower_name'] = completed['name'].apply(lambda x: x.split('/')[1] if '/' in x else x)
    grouped = completed.groupby(['tower_name', 'activityName']).size().reset_index(name='CompletedCount')
    
    total_completed = len(completed)
    for activity in asite_activities:
        count = grouped[grouped['activityName'] == activity]['CompletedCount'].sum()
        count_table.loc[activity, 'Count'] = count
    
    return grouped, total_completed, count_table

@function_timer()
def process_manually(analysis_df, total, dataset_name, location_df, chunk_size=1000, max_workers=4):
    if analysis_df.empty:
        st.warning(f"No completed activities found for {dataset_name}.")
        return {"towers": {}, "total": 0}

    chunks = [analysis_df[i:i + chunk_size] for i in range(0, len(analysis_df), chunk_size)]
    chunk_results = {}
    progress_bar = st.progress(0)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {executor.submit(process_chunk, chunk, idx, dataset_name, location_df): idx for idx, chunk in enumerate(chunks)}
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                generated_text, idx = future.result()
                chunk_results[idx] = generated_text
                progress_bar.progress((chunk_idx + 1) / len(chunks))
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1}: {str(e)}")

    parsed_data = {}
    for chunk_idx in sorted(chunk_results.keys()):
        generated_text = chunk_results[chunk_idx]
        if not generated_text:
            continue
        current_tower = None
        tower_activities = []
        lines = generated_text.split("\n")
        for line in lines:
            line = line.strip()
            if line.startswith("Tower:"):
                if current_tower and tower_activities:
                    parsed_data[current_tower] = tower_activities
                    tower_activities = []
                current_tower = line.split("Tower:", 1)[1].strip()
            elif line.startswith("Total Completed Activities:"):
                continue
            elif not line.startswith("activityName"):
                match = re.match(r'^\s*(.+?)\s+(\d+)$', line)
                if match and current_tower:
                    activity_name = match.group(1).strip()
                    count = int(match.group(2))
                    tower_activities.append({"activityName": activity_name, "completedCount": count})
        if current_tower and tower_activities:
            parsed_data[current_tower] = tower_activities

    aggregated_data = {}
    for tower_name, activities in parsed_data.items():
        tower_short_name = tower_name.split('/')[1] if '/' in tower_name else tower_name
        aggregated_data[tower_short_name] = {act['activityName']: act['completedCount'] for act in activities}
    
    return {"towers": aggregated_data, "total": total}

@function_timer()
def process_chunk(chunk, chunk_idx, dataset_name, location_df):
    towers_data = {}
    for _, row in chunk.iterrows():
        tower_name = row['tower_name']
        activity_name = row['activityName']
        count = int(row['CompletedCount'])
        if tower_name not in towers_data:
            towers_data[tower_name] = []
        towers_data[tower_name].append({"activityName": activity_name, "completedCount": count})
    
    output = ""
    total_activities = 0
    for tower_name, activities in sorted(towers_data.items()):
        output += f"Tower: {tower_name}\nactivityName            CompletedCount\n"
        activity_dict = {}
        for activity in activities:
            name = activity['activityName']
            count = activity['completedCount']
            activity_dict[name] = activity_dict.get(name, 0) + count
        for name, count in sorted(activity_dict.items()):
            output += f"{name:<30} {count}\n"
            total_activities += count
    output += f"Total Completed Activities: {total_activities}"
    return output, chunk_idx

@function_timer()
def generate_consolidated_checklist_excel(ai_data, slab_data):
    try:
        if isinstance(ai_data, str):
            ai_data = json.loads(ai_data)
        if not isinstance(ai_data, dict) or "COS" not in ai_data or "Asite" not in ai_data:
            st.error("❌ Invalid AI data format")
            return None

        cos_to_asite_mapping = {
            "EL-First Fix": "Wall Conducting", "Installation of doors": ["Door/Window Frame", "Door/Window Shutter"],
            "Min. count of UP-First Fix and CP-First Fix": "Plumbing Works", "Water Proofing Works": "Waterproofing - Sunken",
            "Gypsum & POP Punning": "POP & Gypsum Plaster", "Wall Tile": "Wall Tile", "Floor Tile": "Floor Tile",
            "EL-Second Fix": "Wiring & Switch Socket", "Sewer Line": "Sewer Line", "Line Storm Line": "Rain Water/Storm",
            "GSB": "Granular Sub-base", "WMM": "WMM", "Saucer drain": "Saucer drain/Paver block",
            "Kerb Stone": "Kerb Stone", "Electrical": "Electrical Cable", "Concreting": "Concreting"
        }
        slab_cast_activities = ["Shuttering", "Reinforcement", "Concreting"]
        
        consolidated_rows = []
        slab_data_dict = {}
        for tower_name, total_count in slab_data.items():
            if tower_name == "T4":
                half_count = total_count // 2
                slab_data_dict["T4A"] = half_count + (total_count % 2)
                slab_data_dict["T4B"] = half_count
            else:
                slab_data_dict[tower_name] = total_count

        cos_data_dict = {}
        for tower_data in ai_data.get("COS", []):
            tower_name = tower_data.get("Tower", "Unknown Tower").replace("Tower ", "T").replace("(", "").replace(")", "")
            for category_data in tower_data.get("Categories", []):
                category = category_data.get("Category", "Unknown Category")
                category = {"ED Civil": "Civil Works", "MEP": "MEP Works", "Interior Finishing": "Interior Finishing Works", "Structure Work": "Structure Works"}.get(category, category)
                for activity in category_data.get("Activities", []):
                    activity_name = activity.get("Activity Name", "Unknown Activity")
                    count = int(activity.get("Total", 0)) if pd.notna(activity.get("Total")) else 0
                    open_missing = activity.get("OpenMissingOverride", None)
                    if tower_name == "T4":
                        half_count = count // 2
                        cos_data_dict[("T4A", activity_name, category)] = {"count": half_count + (count % 2), "open_missing": open_missing}
                        cos_data_dict[("T4B", activity_name, category)] = {"count": half_count, "open_missing": open_missing}
                    else:
                        cos_data_dict[(tower_name, activity_name, category)] = {"count": count, "open_missing": open_missing}

        asite_data_dict = {}
        for tower_data in ai_data.get("Asite", []):
            tower_name = tower_data.get("Tower", "Unknown Tower").replace("Tower ", "T").replace("(", "").replace(")", "")
            for category_data in tower_data.get("Categories", []):
                category = category_data.get("Category", "Unknown Category")
                category = {"ED Civil": "Civil Works", "MEP": "MEP Works", "Interior Finishing": "Interior Finishing Works", "Structure Work": "Structure Works"}.get(category, category)
                for activity in category_data.get("Activities", []):
                    activity_name = activity.get("Activity Name", "Unknown Activity")
                    count = int(activity.get("Total", 0)) if pd.notna(activity.get("Total")) else 0
                    open_missing = activity.get("OpenMissingOverride", None)
                    if tower_name == "T4":
                        half_count = count // 2
                        asite_data_dict[("T4A", activity_name, category)] = {"count": half_count + (count % 2), "open_missing": open_missing}
                        asite_data_dict[("T4B", activity_name, category)] = {"count": half_count, "open_missing": open_missing}
                    else:
                        asite_data_dict[(tower_name, activity_name, category)] = {"count": count, "open_missing": open_missing}

        normalized_cos_data = {}
        for (tower, cos_activity, category), data in cos_data_dict.items():
            count = data["count"]
            open_missing = data["open_missing"]
            if cos_activity in slab_cast_activities or cos_activity == "Slab Conducting":
                asite_activity = "Concreting"
                key = (tower, asite_activity, category)
                normalized_cos_data[key] = {"count": normalized_cos_data.get(key, {"count": 0})["count"] + count, "open_missing": open_missing}
            elif cos_activity in ["UP-First Fix", "CP-First Fix"]:
                asite_activity = "Plumbing Works"
                key = (tower, asite_activity, category)
                existing_count = normalized_cos_data.get(key, {"count": float('inf')})["count"]
                normalized_cos_data[key] = {"count": min(existing_count, count), "open_missing": open_missing}
            elif cos_activity in cos_to_asite_mapping:
                asite_activity = cos_to_asite_mapping[cos_activity]
                if isinstance(asite_activity, list):
                    for act in asite_activity:
                        normalized_cos_data[(tower, act, category)] = {"count": count, "open_missing": open_missing}
                else:
                    normalized_cos_data[(tower, asite_activity, category)] = {"count": count, "open_missing": open_missing}
            else:
                normalized_cos_data[(tower, cos_activity, category)] = {"count": count, "open_missing": open_missing}

        # Merge slab data into Concreting
        concreting_categories = {tower: ["MEP Works", "Structure Works"] for tower in set(tower for tower, _, _ in asite_data_dict.keys() if _ == "Concreting")}
        for tower_name, slab_count in slab_data_dict.items():
            for category in concreting_categories.get(tower_name, ["Structure Works"]):
                normalized_cos_data[(tower_name, "Concreting", category)] = {"count": slab_count, "open_missing": None}

        all_keys = set(normalized_cos_data.keys()).union(set(asite_data_dict.keys()))
        for key in all_keys:
            tower_name, activity_name, category = key
            if activity_name == "No. of Slab cast":
                continue
            cos_data = normalized_cos_data.get(key, {"count": 0, "open_missing": None})
            asite_data = asite_data_dict.get(key, {"count": 0, "open_missing": None})
            cos_count = cos_data["count"]
            asite_count = asite_data["count"]
            open_missing = cos_data["open_missing"] if cos_data["open_missing"] is not None else asite_data["open_missing"]
            open_missing_count = abs(cos_count - asite_count) if open_missing is None else open_missing
            consolidated_rows.append({
                "Tower": tower_name,
                "Category": category,
                "Activity Name": activity_name,
                "Completed Work*(Count of Flat)": cos_count,
                "In Progress Work*(Count of Flat)": 0,
                "Closed checklist against completed work": asite_count,
                "Open/Missing check list": open_missing_count
            })

        df = pd.DataFrame(consolidated_rows)
        if df.empty:
            st.warning("No data available to generate consolidated checklist.")
            output = BytesIO()
            workbook = Workbook()
            worksheet = workbook.active
            worksheet.title = "Consolidated Checklist"
            worksheet.cell(row=1, column=1).value = "No data available"
            workbook.save(output)
            output.seek(0)
            return output

        df.sort_values(by=["Tower", "Category", "Activity Name"], inplace=True)
        output = BytesIO()
        workbook = Workbook()
        if "Sheet" in workbook.sheetnames:
            workbook.remove(workbook["Sheet"])

        header_font = Font(bold=True)
        category_font = Font(bold=True, italic=True)
        border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        center_alignment = Alignment(horizontal='center')

        worksheet1 = workbook.create_sheet(title="Consolidated Checklist")
        current_row = 1
        for tower, tower_group in df.groupby('Tower'):
            worksheet1.cell(row=current_row, column=6).value = tower
            worksheet1.cell(row=current_row, column=6).font = header_font
            current_row += 1
            for category, cat_group in tower_group.groupby('Category'):
                worksheet1.cell(row=current_row, column=6).value = f"{tower} June Checklist Status - {category}"
                worksheet1.cell(row=current_row, column=6).font = category_font
                current_row += 1
                headers = ["ACTIVITY NAME", "Completed Work*(Count of Flat)", "In Progress Work*(Count of Flat)", "Closed checklist against completed work", "Open/Missing check list"]
                for col, header in enumerate(headers, start=6):
                    cell = worksheet1.cell(row=current_row, column=col)
                    cell.value = header
                    cell.font = header_font
                    cell.border = border
                    cell.alignment = center_alignment
                current_row += 1
                for _, row in cat_group.iterrows():
                    worksheet1.cell(row=current_row, column=6).value = row["Activity Name"]
                    worksheet1.cell(row=current_row, column=7).value = row["Completed Work*(Count of Flat)"]
                    worksheet1.cell(row=current_row, column=8).value = row["In Progress Work*(Count of Flat)"]
                    worksheet1.cell(row=current_row, column=9).value = row["Closed checklist against completed work"]
                    worksheet1.cell(row=current_row, column=10).value = row["Open/Missing check list"]
                    for col in range(6, 11):
                        worksheet1.cell(row=current_row, column=col).border = border
                        worksheet1.cell(row=current_row, column=col).alignment = center_alignment
                    current_row += 1
                total_open_missing = cat_group["Open/Missing check list"].sum()
                worksheet1.cell(row=current_row, column=6).value = "TOTAL pending checklist June"
                worksheet1.cell(row=current_row, column=9).value = total_open_missing
                for col in range(6, 11):
                    cell = worksheet1.cell(row=current_row, column=col)
                    cell.font = category_font
                    cell.border = border
                    cell.alignment = center_alignment
                current_row += 1
            current_row += 1

        for col in worksheet1.columns:
            max_length = max(len(str(cell.value)) for cell in col if cell.value)
            worksheet1.column_dimensions[col[0].column_letter].width = max_length + 2

        worksheet2 = workbook.create_sheet(title="Checklist June")
        current_row = 1
        worksheet2.cell(row=current_row, column=1).value = "Checklist: June"
        worksheet2.cell(row=current_row, column=1).font = header_font
        current_row += 1
        headers = ["Site", "Total of Missing & Open Checklist-Civil", "Total of Missing & Open Checklist-MEP", "TOTAL"]
        for col, header in enumerate(headers, start=1):
            cell = worksheet2.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
        current_row += 1

        summary_data = {}
        for _, row in df.iterrows():
            tower = row["Tower"]
            category = row["Category"]
            open_missing = row["Open/Missing check list"]
            site_name = f"External Development-{tower}" if "External Development" in category else f"Veridia-Tower {tower[1:]:0>2}"
            type_ = "Civil" if category in ["Civil Works", "Structure Works", "External Development"] else "MEP"
            if site_name not in summary_data:
                summary_data[site_name] = {"Civil": 0, "MEP": 0}
            summary_data[site_name][type_] += open_missing

        for site_name, counts in sorted(summary_data.items()):
            worksheet2.cell(row=current_row, column=1).value = site_name
            worksheet2.cell(row=current_row, column=2).value = counts["Civil"]
            worksheet2.cell(row=current_row, column=3).value = counts["MEP"]
            worksheet2.cell(row=current_row, column=4).value = counts["Civil"] + counts["MEP"]
            for col in range(1, 5):
                worksheet2.cell(row=current_row, column=col).border = border
                worksheet2.cell(row=current_row, column=col).alignment = center_alignment
            current_row += 1

        for col in worksheet2.columns:
            max_length = max(len(str(cell.value)) for cell in col if cell.value)
            worksheet2.column_dimensions[col[0].column_letter].width = max_length + 2

        workbook.save(output)
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"Error generating Excel: {str(e)}")
        st.error(f"❌ Error generating Excel: {str(e)}")
        return None

@function_timer()
def display_activity_count(ai_data, slab_data):
    try:
        if isinstance(ai_data, str):
            ai_data = json.loads(ai_data)
        if not isinstance(ai_data, dict) or "COS" not in ai_data or "Asite" not in ai_data:
            st.error("❌ Invalid AI data format")
            return

        slab_display_df = pd.DataFrame([
            {'Tower': 'T4A' if tower == 'T4' else 'T4B' if tower == 'T4' else tower, 'Completed': count // 2 + (count % 2) if tower == 'T4' else count // 2 if tower == 'T4' else count}
            for tower, count in slab_data.items() if tower != "Tower Name" and tower != "Total"
        ])
        
        categories = {
            "COS": {
                "MEP": ["EL-First Fix", "UP-First Fix", "CP-First Fix", "Min. count of UP-First Fix and CP-First Fix", "C-Gypsum and POP Punning", "EL-Second Fix", "Concreting", "Electrical"],
                "Interior Finishing": ["Installation of doors", "Waterproofing Works", "Wall Tiling", "Floor Tiling"],
                "ED Civil": ["Sewer Line", "Storm Line", "GSB", "WMM", "Stamp Concrete", "Saucer drain", "Kerb Stone"]
            },
            "Asite": {
                "MEP": ["Wall Conducting", "Plumbing Works", "POP & Gypsum Plaster", "Wiring & Switch Socket", "Slab Conducting", "Electrical Cable", "Concreting"],
                "Interior Finishing": ["Door/Window Frame", "Waterproofing - Sunken", "Wall Tile", "Floor Tile", "Door/Window Shutter"],
                "ED Civil": ["Sewer Line", "Rain Water/Storm Line", "Granular Sub-base", "WMM", "Saucer drain/Paver block", "Kerb Stone", "Concreting"]
            }
        }

        for source in ["COS", "Asite"]:
            st.subheader(f"{source} Activity Counts")
            source_data = ai_data.get(source, [])
            if not source_data:
                st.warning(f"No data for {source}.")
                continue
            for tower_data in source_data:
                tower_name = tower_data.get("Tower", "Unknown Tower").replace("Tower ", "T").replace("(", "").replace(")", "")
                st.write(f"#### {tower_name}")
                for category in categories[source]:
                    st.write(f"**{category}**")
                    category_data = next((cat for cat in tower_data.get("Categories", []) if cat.get("Category") == category), {"Activities": []})
                    activity_counts = [
                        {"Activity Name": activity, "Count": next((act["Total"] for act in category_data["Activities"] if act.get("Activity Name") == activity), 0)}
                        for activity in categories[source][category]
                    ]
                    df = pd.DataFrame(activity_counts)
                    if not df.empty:
                        st.table(df)
                    else:
                        st.write("No activities in this category.")
                    if source == "COS":
                        st.write("**Slab Cycle Counts**")
                        tower_slab_df = slab_display_df[slab_display_df['Tower'] == tower_name]
                        if not tower_slab_df.empty:
                            st.table(tower_slab_df)
                        else:
                            st.write("No slab cycle data for this tower.")

        total_cos = sum(
            act["Total"] for tower in ai_data.get("COS", []) for cat in tower.get("Categories", []) for act in cat.get("Activities", []) if pd.notna(act.get("Total"))
        ) + sum(slab_data.values())
        total_asite = sum(
            act["Total"] for tower in ai_data.get("Asite", []) for cat in tower.get("Categories", []) for act in cat.get("Activities", []) if pd.notna(act.get("Total"))
        )
        st.write("### Total Completed Activities")
        st.write(f"**COS Total**: {total_cos}")
        st.write(f"**Asite Total**: {total_asite}")
    except Exception as e:
        st.error(f"❌ Error displaying activity counts: {str(e)}")

@function_timer(show_args=True)
async def initialize_and_fetch_data(email, password):
    with st.spinner("Initializing and fetching data..."):
        if not email or not password:
            st.sidebar.error("Please provide both email and password!")
            return False

        if not await get_workspace_and_project_ids(email, password):
            return False

        try:
            st.session_state['data'] = await fetch_all_data(email, password)
            cos_client = initialize_cos_client()
            files = get_cos_files()  # Assuming get_cos_files is defined in veridia1
            if files:
                st.success(f"Found {len(files)} files in COS storage")
                for file in files:
                    response = cos_client.get_object(Bucket=COS_BUCKET, Key=file)
                    file_bytes = BytesIO(response['Body'].read())
                    result = process_file(file_bytes, file)  # Assuming process_file is defined
                    for df, tname in result if isinstance(result, list) else [result]:
                        if df is not None and not df.empty:
                            tower_key = f"cos_df_{tname.lower().replace(' ', '_')}"
                            st.session_state[tower_key] = df
                            st.write(f"Processed Data for {tname} - {len(df)} rows:")
                            st.write(df.head())
            else:
                st.warning("No Excel files found in COS bucket.")
            st.sidebar.success("Data fetching completed!")
            return True
        except Exception as e:
            st.sidebar.error(f"Failed to fetch data: {str(e)}")
            return False

@function_timer()
def analyze_status_manually():
    if 'data' not in st.session_state:
        st.error("❌ No data available. Please initialize and fetch data first.")
        return

    ai_response = {"COS": [], "Asite": []}
    for project_type in st.session_state['data']:
        analysis_df, total, count_table = process_data(
            st.session_state['data'][project_type]['associations'],
            st.session_state['data'][project_type]['activities'],
            st.session_state['data'][project_type]['locations'],
            project_type
        )
        result = process_manually(
            analysis_df,
            total,
            project_type,
            st.session_state['data'][project_type]['locations']
        )
        categories = {
            "finishing": "Interior Finishing",
            "structure": "Structure Work",
            "external": "ED Civil",
            "lift": "MEP",
            "common_area": "Interior Finishing"
        }
        ai_response["Asite"].append({
            "Tower": f"Tower {project_type}",
            "Categories": [{"Category": categories[project_type], "Activities": [
                {"Activity Name": k, "Total": v} for k, v in result["towers"].items()
            ]}]
        })
    st.session_state['ai_response'] = ai_response

# Streamlit UI
st.markdown(
    """
    <h1 style='font-family: "Arial Black", Gadget, sans-serif; color: red; font-size: 48px; text-align: center;'>
        CheckList - Report
    </h1>
    """,
    unsafe_allow_html=True
)

st.sidebar.title("🔒 Asite Initialization")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password", type="password", key="password_input")

if st.sidebar.button("Initialize and Fetch Data"):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        success = loop.run_until_complete(initialize_and_fetch_data(email, password))
        if success:
            st.sidebar.success("Initialization and data fetching completed!")
        else:
            st.sidebar.error("Initialization and data fetching failed!")
    finally:
        loop.close()

st.sidebar.title("📊 Status Analysis")
if st.sidebar.button("Analyze and Display Activity Counts"):
    analyze_status_manually()
    if 'ai_response' in st.session_state and 'slabreport' in st.session_state:
        display_activity_count(st.session_state['ai_response'], st.session_state.get('slabreport', {}))
        excel_file = generate_consolidated_checklist_excel(st.session_state['ai_response'], st.session_state.get('slabreport', {}))
        if excel_file:
            timestamp = pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y%m%d_%H%M')
            st.sidebar.download_button(
                label="📥 Download Checklist Excel",
                data=excel_file,
                file_name=f"Consolidated_Checklist_Veridia_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.error("❌ Missing AI response or slab report data.")

st.sidebar.title("📊 Slab Cycle")
st.session_state['ignore_year'] = st.sidebar.number_input("Ignore Year", min_value=1900, max_value=2100, value=2023, step=1)
st.session_state['ignore_month'] = st.sidebar.number_input("Ignore Month", min_value=1, max_value=12, value=3, step=1)
