import base64
import json
import requests
from datetime import datetime, timedelta
from daily_files.utils.s3_utils import get_secret, put_secret


class PodaacS3Creds():
  
    def __init__(self, username: str, password: str):
        self.edl_auth: str = f'{username}:{password}'
        self.current_pds3_auth: dict = get_secret('podaac_direct_s3_auth')
        self.creds = self.get_creds()
    
    def get_creds(self):
        '''
        Retrieve temporary Podaac S3 credentials
        '''
        curr_expiration = datetime.strptime(self.current_pds3_auth['expiration'], '%Y-%m-%d %H:%M:%S')
        if curr_expiration < datetime.now() - timedelta(seconds=300):
            return self.current_pds3_auth
        return self.refresh_creds()  
    
    def refresh_creds(self) -> dict:
        '''
        Function to pull fresh credentials and update values in AWS secrets manager
        '''
        temp_creds_url = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
        login_resp = requests.get(temp_creds_url, allow_redirects=False)
        login_resp.raise_for_status()

        encoded_auth  = base64.b64encode(self.edl_auth.encode('ascii'))

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
        self.update_secret(creds)
        return creds
    
    def update_secret(self, creds: dict):
        '''
        Converts credentials to string and updates values on secrets manager
        '''
        secret_string = json.dumps(creds)
        put_secret('podaac_direct_s3_auth', secret_string)