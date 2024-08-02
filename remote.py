from auth.auth import BrightcoveAuth
from dotenv import load_dotenv
import requests
import json
import csv
import sys
import os
import re
import validators
from tqdm import tqdm
from colorama import Fore, Style, init

load_dotenv()

ingest_profile = os.getenv('INGEST_PROFILE')
csv_dir = os.getenv('CSV_PATH')
csv_file = 'remote_src.csv'
csv_path = os.path.join(csv_dir, csv_file)
last_processed_row_path = os.getenv('LAST_PROCESSED_PATH')
last_processed_row_file = 'last_processed_row.txt'
last_processed_row_path = os.path.join(last_processed_row_path, last_processed_row_file)

vid_url_pattern = r'^(https?://|s3://)[^/]+/(?:.+/)?[^/]+(?:\.(mp4|mov|avi|mkv|mpd|m3u8))$'

bar_format = "{l_bar}%s{bar}%s{r_bar}" % (Fore.RED, Style.RESET_ALL)

def get_container(video_url):
    if video_url.endswith('.m3u8'):
        return 'HLS'
    elif video_url.endswith('.mpd'):
        return 'DASH'
    elif video_url.endswith('.mp4'):
        return 'MP4'
    else:
        return 'Unknown'

# Function to save the last processed csv row
def save_last_processed_row(row_number):
    with open(last_processed_row_path, 'w') as file:
        file.write(str(row_number))

# Function to get the last processed csv row
def get_last_processed_row():
    if os.path.exists(last_processed_row_path):
        with open(last_processed_row_path, 'r') as file:
            content = file.read().strip()
            if content.isdigit():
                return int(content)
            else:
                return -1
    return -1

def format_tags(tags):
    valid_tag_pattern = re.compile(r'^[\w\s]+$')
    tags_list = []
    if not tags:
        return tags_list
    if isinstance(tags, list):
        tags_list = tags
    elif isinstance(tags, str):
        try:
            tags_list = json.loads(tags)
            if not isinstance(tags_list, list):
                raise ValueError
        except (json.JSONDecodeError, ValueError):
            tags_list = tags.split(',')
            tags_list = [tag.strip() for tag in tags_list]
    else:
        raise ValueError("Unsupported format for tags input")
    for tag in tags_list:
        if not valid_tag_pattern.match(tag):
            raise ValueError(f"Invalid tag value: {tag}")
    # return json.dumps(tags_list)
    return tags_list

def valid_video_url(video_url):
    if isinstance(video_url, str):
        if validators.url(video_url):
            if re.match(vid_url_pattern, video_url):
                return True, None
            else:
                return False, "Provided URL is not a valid URL path or video format"
        else:
            return False, "Provided URL is not a valid URL"
    else:
        return False, "URL is not a string or is missing"

def read_csv():
    last_processed_row = get_last_processed_row()
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        mandatory_columns = ['name', 'video_url']
        missing_columns = [col for col in mandatory_columns if col not in reader.fieldnames]
        if missing_columns:
            print(f"Error: Missing essential columns: {', '.join(missing_columns)}")
            sys.exit(1)
        
        total_rows = sum(1 for _ in reader)
        csvfile.seek(0)
        next(reader)

        with tqdm (total=total_rows, desc="Processing CSV", unit="row", ascii = "─┄┈┉┅━", bar_format=bar_format) as pbar:
            for current_row_number, row in enumerate(reader):
                if current_row_number <= last_processed_row:
                    pbar.update(1)
                    continue
                
                video_name = row.get('name')
                video_tags = row.get('tags', '')
                video_description = row.get('description', '')
                video_long_description = row.get('long_description', '')
                video_url = row.get('video_url')
                poster = row.get('poster', '')
                thumbnail = row.get('thumbnail', '')

                video_tags = format_tags(video_tags)
                is_valid, error_message = valid_video_url(video_url)
                if is_valid:
                    create_media_object(video_name, video_tags, video_description, video_long_description, video_url, poster, thumbnail)
                    save_last_processed_row(current_row_number)
                else:
                    print(f"Skipping {current_row_number}: {error_message}")
                
                pbar.update(1)
            
    if os.path.exists(last_processed_row_path):
        with open(last_processed_row_path, 'r') as file:
            contents = file.read()
            if contents:
                with open(last_processed_row_path, 'w'):
                    pass
                print(f"Removing last recorded csv row: {last_processed_row_path} -- processing is complete.")
            else:
                print(f"{last_processed_row_path} is already empty.")
        print("CSV processing has finished.")

def create_media_object(video_name, video_tags, video_description, video_long_description, video_url, poster, thumbnail):
    auth = BrightcoveAuth()
    headers = auth.get_headers()

    url = f'https://cms.api.brightcove.com/v1/accounts/{auth.account_id}/videos'
    payload = {
        "description": f"{video_description}",
        "long_description": f"{video_long_description}",
        "tags": video_tags,
        "name": f"{video_name}"
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code in [200, 201]:
        response_dict = json.loads(response.text)
        add_remote_src(response_dict['id'], video_url)
        if poster and not thumbnail:
            thumbnail = poster
            tqdm.write(f"{response_dict['id']}: No thumbnail image. Using poster image.")
        if poster:
            ingest_images(poster, thumbnail, response_dict['id'])
        else:
            tqdm.write(f"Skipping {response_dict['id']} as there are no high res images to ingest.")
    else:
        raise AttributeError(f"Failed to create object: {response.status_code}, {response.text}")

def add_remote_src(video_id, video_url):
    container = get_container(video_url)
    if container == 'MP4':
        auth = BrightcoveAuth()
        headers = auth.get_headers()
        url = f'https://cms.api.brightcove.com/v1/accounts/{auth.account_id}/videos/{video_id}/assets/renditions'
        payload = {
                "remote_url": f"{video_url}",
                "video_container": f"{container}",
                "video_codec": "h264",
                "progressive_download": True
        }
    elif container == 'HLS':
        auth = BrightcoveAuth()
        headers = auth.get_headers()
        url = f'https://cms.api.brightcove.com/v1/accounts/{auth.account_id}/videos/{video_id}/assets/hls_manifest'
        payload = {
                "remote_url": f"{video_url}"
        }
    elif container == 'DASH':
        auth = BrightcoveAuth()
        headers = auth.get_headers()
        url = f'https://cms.api.brightcove.com/v1/accounts/{auth.account_id}/videos/{video_id}/assets/dash_manifest'
        payload = {
                "remote_url": f"{video_url}"
        }
    else:
        raise ValueError("Unrecognised containers.")

    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code in [200, 201]:
        response_dict = json.loads(response.text)
        tqdm.write(response_dict['id'])
    else:
        print(f"Failed to add remote video: {response.status_code}, {response.text}")

def ingest_images(poster, thumbnail, video_id):
    auth = BrightcoveAuth()
    headers = auth.get_headers()
    url = f'https://ingest.api.brightcove.com/v1/accounts/{auth.account_id}/videos/{video_id}/ingest-requests'
    payload = {
        "profile": f"{ingest_profile}",
        "poster": {
            "url": f"{poster}"
        },
        "thumbnail": {
            "url": f"{thumbnail}"
        }
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code in [200, 201]:
        response_dict = json.loads(response.text)
        tqdm.write(f"{video_id}: {response_dict['id']} - Ingest successful.")
    else:
        print(f"Failed to ingest images: {response.status_code}, {response.text}")

def main():
    read_csv()

if __name__ == "__main__":
    main()