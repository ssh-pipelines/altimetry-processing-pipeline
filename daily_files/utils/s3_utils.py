import boto3
from requests import request

def get_bucket(bucket: str, profile='') -> boto3.resource:
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()
    s3 = session.resource('s3')
    return s3.Bucket(bucket)


def get_objects(bucket: boto3.resource, prefix: str):
    return list(bucket.objects.filter(Prefix=prefix))


def get_object(bucket: boto3.resource, key: str):
    return bucket.Object(key).get().get('Body')


def read_object(bucket: boto3.resource, key:str) -> str:
    '''
    Reads object as a bytestring
    '''
    return get_object(bucket, key).read()


def get_uri(bucket: boto3.resource, key=''):
    return f's3://{bucket}/{key}'


def upload_s3(src: str, bucket: boto3.resource, dest:str):
    bucket.upload_file(src, dest)

def get_podaac_creds() -> dict:
    'In NGAP these credentials will allow getObject and listBucket on the configured resources.'
    url = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
    r = request.get(url)
    return r.json()

def get_podaac_objects(bucket_name, prefix=''):
    creds = get_podaac_creds()

    client = boto3.client(
        's3',
        aws_access_key_id=creds["accessKeyId"],
        aws_secret_access_key=creds["secretAccessKey"],
        aws_session_token=creds["sessionToken"]
    )
    # use the client for readonly access.
    return client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)