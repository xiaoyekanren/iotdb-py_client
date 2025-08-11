# coding=utf-8

import json
import requests
import base64


class RestClient:
    def __init__(self, base_url: str, username='root', password='root'):
        self.base_url = base_url.rstrip('/')
        userpass = f"{username}:{password}"
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + base64.b64encode(userpass.encode()).decode()
        }
        self.api = {
            'query': 'rest/v2/query',
            'nonQuery': 'rest/v2/nonQuery',
            'insertTablet': 'rest/v2/insertTablet',
            'insertRecords': 'rest/v2/insertRecords',
            'fastLastQuery': 'rest/v2/fastLastQuery',
        }

    def query(self, sql: str):
        url = f"{self.base_url}/{self.api['query']}"
        data = {'sql': sql}
        return self._post(url, data)

    def fast_last_query(self, query_path: str):
        url = f"{self.base_url}/{self.api['fastLastQuery']}"
        data = {'prefix_paths': query_path.split('.')}
        return self._post(url, data)

    def non_query(self, sql: str):
        url = f"{self.base_url}/{self.api['nonQuery']}"
        data = {'sql': sql}
        return self._post(url, data)

    def insert_tablet(self, tablet_data: dict):
        url = f"{self.base_url}/{self.api['insertTablet']}"
        return self._post(url, tablet_data)

    def insert_records(self, records_data: dict):
        url = f"{self.base_url}/{self.api['insertRecords']}"
        return self._post(url, records_data)

    def _post(self, url, data):
        response = requests.post(url, headers=self.headers, data=json.dumps(data))
        if response.status_code == 200:
            try:
                return response.json()
            except Exception:
                return response.text
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text}")

