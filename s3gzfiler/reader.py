import boto3
import gzip
import json
from io import BytesIO

KEY_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
KEY_SECRET_KEY = "AWS_SECRET_ACCESS_KEY"
KEY_BUCKET_NAME = "S3_BUCKET_NAME"
KEY_S3_REGION = "S3_REGION"


class FileContentReader:

    def __init__(self, _client, _bucket_name, _folder_name, _start, _end):
        self.s3_client = _client
        self.bucket_name = _bucket_name
        self.folder_name = _folder_name
        self.start_datetime = _start
        self.end_datetime = _end

    def read(self):
        result = []
        try:
            response = self.s3_client.list_objects(
                Bucket=self.bucket_name,
                Prefix=self.folder_name + self._get_common_timeslice(self.start_datetime, self.end_datetime)
            )
            keys = [content['Key'] for content in response['Contents']]
            sorted_keys = sorted(keys, key=self._get_keys_for_sorting, reverse=False)
            for k in sorted_keys:
                content = self.extract_gz_object(k)
                lines = content.split('\n')
                for line in lines:
                    if len(line.strip()) == 0:
                        continue
                    json_obj = json.loads(line)
                    result.append(json_obj["log"])
        except Exception:
            pass
        return result

    def extract_gz_object(self, key):
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        body = BytesIO(obj.get('Body').read())
        gzipped_content = gzip.GzipFile(fileobj=body)
        content = gzipped_content.read()
        return content.decode('utf-8')

    def _get_common_timeslice(self, start, end):
        def get_year_part(s):
            return s[:4]
        def get_month_part(s):
            return s[4:6]
        def get_day_part(s):
            return s[6:8]
        def get_hour_part(s):
            return s[8:10]

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

    def _get_keys_for_sorting(self, object_key):
        object_name_parts = object_key.split('/')
        local_name_part = object_name_parts[-1]

        parts_of_local_name = local_name_part.split('_')
        tag_part = parts_of_local_name[0]
        time_part = parts_of_local_name[1]
        index = int(parts_of_local_name[2].split('.')[0])
        return tag_part, time_part, index


def _load_configs(path):
    result = {}
    for each_line in open(path, 'r'):
        stripped = each_line.strip()
        if len(stripped) == 0:
            continue
        key_value = stripped.split('=')
        result[key_value[0].strip()] = key_value[1].strip()
    return result


def _ask_non_empty_string(guide):
    result = ""
    while len(result) == 0:
        print(guide)
        result = input().strip()
    return result


def _ask_datetime(guide):
    def is_well_format_datetime(s):
        return s is not None and len(s) == 14 and s.isdigit()

    result = ""
    while not is_well_format_datetime(result):
        print(guide)
        result = input().strip()
    return result


configs = _load_configs('./.env')
client = boto3.client(service_name="s3",
                      region_name=configs[KEY_S3_REGION],
                      aws_access_key_id=configs[KEY_ACCESS_KEY_ID],
                      aws_secret_access_key=configs[KEY_SECRET_KEY])

folder_name = _ask_non_empty_string("Folder name?")
start_datetime = _ask_datetime("START datetime? (YYYYMMDDHHmmSS)")
end_datetime = _ask_datetime("END datetime? (YYYYMMDDHHmmSS)")

reader = FileContentReader(client, configs[KEY_BUCKET_NAME], folder_name, start_datetime, end_datetime)
lines = reader.read()
for each in lines:
    print(each)
