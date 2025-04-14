import json
import boto3
import base64
import requests
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

session = boto3.Session()

def get_secret(secret_name: str) -> dict:
    sm_client = session.client(service_name='secretsmanager')
    try:
        secret_str = sm_client.get_secret_value(SecretId=secret_name)['SecretString']
    except ClientError as e:
        raise e
    try:
        secret = json.loads(secret_str)
    except:
        raise RuntimeError('Error converting secret string to dict')
    return secret

def put_secret(secret_name: str, secret_string: str):
    sm_client = session.client(service_name='secretsmanager')
    try:
        sm_client.put_secret_value(SecretId=secret_name, SecretString=secret_string)
    except ClientError as e:
        raise e

def refresh_creds(username, password) -> dict:
    '''
    Function to pull fresh credentials and update values in AWS secrets manager
    '''
    temp_creds_url = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
    login_resp = requests.get(temp_creds_url, allow_redirects=False)
    login_resp.raise_for_status()

    encoded_auth  = base64.b64encode(f'{username}:{password}'.encode('ascii'))

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

def update_secret(creds: dict):
    '''
    Converts credentials to string and updates values on secrets manager
    '''
    secret_string = json.dumps(creds)
    put_secret('podaac_direct_s3_auth', secret_string)

def lambda_handler(event, context):
    try:
        pda_secret = get_secret('podaac_direct_s3_auth')
        if 'expiration' in pda_secret:
            expiration = datetime.strptime(pda_secret['expiration'][:19], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            if expiration >= datetime.now(timezone.utc) + timedelta(minutes=30):
                return {'message' : 'Credentials valid for at least 30 minutes. Skipping update.'}
    except Exception as e:
        raise RuntimeError(f'Unable to get current expiration: {e}')
    try:
        edl_secret = get_secret('EDL_auth')
    except Exception as e:
        raise RuntimeError(f'Unable to obtain EDL auth from secrets manager: {e}')
        
    ed_user = edl_secret.get('user')
    ed_pass = edl_secret.get('password')
    
    try:
        creds = refresh_creds(ed_user, ed_pass)
    except Exception as e:
        raise RuntimeError(f'Unable to get refreshed PODAAC creds: {e}')
        
    try:
        update_secret(creds)
    except Exception as e:
        raise RuntimeError(f'Unable to post updated creds to secrets manager {e}')
    
    return {'message' : 'PODAAC creds updated in secrets manager'}
