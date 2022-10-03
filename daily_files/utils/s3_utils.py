import boto3

def get_bucket(bucket: str, profile=''):
    if profile:
        session = boto3.session.Session(profile_name=profile)
    else:
        session = boto3.session.Session()
    s3 = session.resource('s3')
    return s3.Bucket(bucket)


def get_objects(bucket: boto3.resource, prefix: str):
    return list(bucket.objects.filter(Prefix=prefix))


def get_object(bucket, key):
    return bucket.Object(key).get().get('Body')


def read_object(bucket, key):
    return get_object(bucket, key).read()


def get_uri(bucket, key=''):
    return f's3://{bucket}/{key}'


def upload_s3(src, bucket, dest):
    bucket.upload_file(src, dest)
