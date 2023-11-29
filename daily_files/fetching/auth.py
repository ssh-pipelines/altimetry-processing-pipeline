import base64
import json
import logging
import requests
import os


class Podaac_S3_Creds():
  
    def __init__(self, username: str, password: str):
        self.auth = f'{username}:{password}'
        self.creds = self.get_creds()
    
    def get_creds(self):
        '''
        Retrieve temporary Podaac S3 credentials
        '''
        temp_creds_url = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
        login_resp = requests.get(temp_creds_url, allow_redirects=False)
        login_resp.raise_for_status()

        encoded_auth  = base64.b64encode(self.auth.encode('ascii'))

        auth_redirect = requests.post(
            login_resp.headers['location'],
            data = {"credentials": encoded_auth},
            headers= { "Origin": temp_creds_url },
            allow_redirects=False
        )
        auth_redirect.raise_for_status()

        final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

        results = requests.get(temp_creds_url, cookies={'accessToken': final.cookies['accessToken']})
        results.raise_for_status()
        creds = json.loads(results.content)
        return creds