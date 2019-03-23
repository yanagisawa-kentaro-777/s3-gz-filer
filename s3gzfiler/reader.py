import boto3
import gzip
import json
from io import BytesIO


KEY_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
KEY_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
KEY_BUCKET_NAME = "S3_BUCKET_NAME"
KEY_S3_REGION = "S3_REGION"

configs = {}
for line in open('./.env', 'r'):
    stripped = line.strip()
    if len(stripped) == 0:
        continue
    parts = stripped.split('=')
    configs[parts[0].strip()] = parts[1].strip()

client = boto3.client(service_name="s3",
                      region_name=configs[KEY_S3_REGION],
                      aws_access_key_id=configs[KEY_ACCESS_KEY_ID],
                      aws_secret_access_key=configs[KEY_SECRET_KEY])

print("Folder name?")
folder_name = input().strip()

print("START datetime? (YYYYMMDDHHmmSS)")
start_datetime = input().strip()

print("END datetime? (YYYYMMDDHHmmSS)")
end_datetime = input().strip()


def get_year_part(s):
    return s[:4]


def get_month_part(s):
    return s[4:6]


def get_day_part(s):
    return s[6:8]


def get_hour_part(s):
    return s[8:10]


def get_common_timeslice(start, end):
    result = "/"
    if get_year_part(start) != get_year_part(end):
        return result
    result = result + "{}/".format(get_year_part(start))
    if get_month_part(start) != get_month_part(end):
        return result
    result = result + "{}/".format(get_month_part(start))
    if get_day_part(start) != get_day_part(end):
        return result
    result = result + "{}/".format(get_day_part(start))
    if get_hour_part(start) != get_hour_part(end):
        return result
    return result + "{}/".format(get_hour_part(start))


def get_keys_for_sorting(s):
    object_name_parts = s.split('/')
    local_name_part = object_name_parts[-1]

    parts_of_local_name = local_name_part.split('_')
    tag_part = parts_of_local_name[0]
    time_part = parts_of_local_name[1]
    index = int(parts_of_local_name[2].split('.')[0])
    return tag_part, time_part, index


def cat_gz(key):
    obj = client.get_object(Bucket=configs[KEY_BUCKET_NAME], Key=key)
    body = BytesIO(obj.get('Body').read())
    gzipped_content = gzip.GzipFile(fileobj=body)
    content = gzipped_content.read()
    return content.decode('utf-8')


response = client.list_objects(
    Bucket=configs[KEY_BUCKET_NAME],
    Prefix=folder_name + get_common_timeslice(start_datetime, end_datetime)
)
keys = [content['Key'] for content in response['Contents']]
sorted_keys = sorted(keys, key=get_keys_for_sorting, reverse=False)
for k in sorted_keys:
    content = cat_gz(k)
    lines = content.split('\n')
    for line in lines:
        if len(line.strip()) == 0:
            continue
        json_obj = json.loads(line)
        print(json_obj["log"])

