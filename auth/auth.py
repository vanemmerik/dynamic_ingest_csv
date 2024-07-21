import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

class BrightcoveAuth:
    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.account_id = os.getenv('PUB_ID')
        self.token = None
        self.token_expiry = 0

    def get_token(self):
        if not self.token or time.time() > self.token_expiry - 60:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        url = 'https://oauth.brightcove.com/v4/access_token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            token_data = response.json()
            self.token = token_data['access_token']
            self.token_expiry = time.time() + token_data['expires_in'] - 60
        else:
            raise Exception(f"Failed to refresh token: {response.status_code}, {response.text}")

    def get_headers(self):
        return {'Authorization': f'Bearer {self.get_token()}'}