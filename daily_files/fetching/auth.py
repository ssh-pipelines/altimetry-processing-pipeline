import requests

class Podaac_S3_Creds():
  
    def __init__(self):
        self.creds = self.get_creds()
        
        
    def get_creds(self):
        # Ensure netrc
        temp_creds_url = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
        creds = requests.get(temp_creds_url).json()
        return creds