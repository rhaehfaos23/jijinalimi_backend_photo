import boto3
from setting_management import GlobalSetting

JSON_CONTENT = 'application/json'
IMAGE_PNG_CONTENT = 'image/png'

JSON_PATH = 'v3/{0}'
IMAGE_PATH = 'v3/img/{0}'

g_setting: GlobalSetting
s3_resource = None


def init_aws_s3(setting):
    global g_setting
    global s3_resource
    g_setting = setting
    s3_resource = boto3.resource('s3', aws_access_key_id=g_setting.aws.aws_access_key_id,
                                 aws_secret_access_key=g_setting.aws.aws_secret_access_key,
                                 region_name=g_setting.aws.region_name)


def save_s3(data, path: str, content_type=JSON_CONTENT):
    """
    S3에 데이터를 저장하는 함수

    :param data: 저장할 데이터
    :param path: 경로 + 파일 이름
    :param content_type: 컨텐트 타입
    :return: void
    """

    global s3_resource
    s3_resource.Bucket('jijinalimi').put_object(Body=data, Key=path, ContentType=content_type)
