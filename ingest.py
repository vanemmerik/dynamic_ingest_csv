from auth.auth import BrightcoveAuth
from dotenv import load_dotenv
import requests
import json
import csv
import os
import re

load_dotenv()

ingest_profile = os.getenv('INGEST_PROFILE')
csv_dir = os.getenv('CSV_PATH')
csv_file = 'video_src.csv'
csv_path = os.path.join(csv_dir, csv_file)
last_processed_row_path = os.getenv('LAST_PROCESSED_PATH')
last_processed_row_file = 'last_processed_row.txt'
last_processed_row_path = os.path.join(last_processed_row_path, last_processed_row_file)

vid_url_pattern = r'^(https?://|s3://)[^/]+/(?:.+/)?[^/]+(?:\.(mp4|mov|avi|mkv))$'

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

def is_valid_video_url(video_url):
    # Check if video_url is a string and perform validations
    if isinstance(video_url, str):
        if re.match(vid_url_pattern, video_url):
            return True, None
        else:
            return False, "Provided URL is not a valid URL path or video format"
    else:
        return False, "URL is not a string or is missing"

def read_csv():
    last_processed_row = get_last_processed_row()
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for current_row_number, row in enumerate(reader):
            if current_row_number <= last_processed_row:
                continue

            video_name, video_tags, video_description, video_long_description, video_url = row['name'], row['tags'], row['description'], row['long_description'], row['video_url']
            video_tags = format_tags(video_tags)
            is_valid_video_url(video_url)
            create_media_object(video_name, video_tags, video_description, video_long_description, video_url)
            save_last_processed_row(current_row_number)

            
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

def create_media_object(video_name, video_tags, video_description, video_long_description, video_url):
    print(video_tags)
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
        ingest_media(response_dict['id'], video_url)
    else:
        print(f"Failed to create object: {response.status_code}, {response.text}")

def ingest_media(video_id, video_url):
    auth = BrightcoveAuth()
    headers = auth.get_headers()
    url = f'https://ingest.api.brightcove.com/v1/accounts/{auth.account_id}/videos/{video_id}/ingest-requests'
    payload = {
        "master": {
            "url": f"{video_url}"
        },
        "profile": f"{ingest_profile}"
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code in [200, 201]:
        response_dict = json.loads(response.text)

        print(response_dict['id'])
    else:
        print(f"Failed to ingest video: {response.status_code}, {response.text}")

def main():
    read_csv()

if __name__ == "__main__":
    main()