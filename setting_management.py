import os
import json


setting_directory_path = 'settings'
setting_file_name = 'settings.json'


class BaseSetting:
    """
    설정 파일을 관리하는 클래스가 상속해야할 기본 클래스
    """
    @property
    def setting_path(self):
        return os.path.join(setting_directory_path, setting_file_name)


class CommonSetting(BaseSetting):
    """
    KMA설정 파일과 JMA설정파일에 공통으로 존재하는 설정에 대한 클래스

    *is_first_start를 제외한 모든 변수는 읽기 전용
    """
    def __init__(self,
                 log_path,
                 log_file_name,
                 current_data_path,
                 current_data_file_name,
                 sleep_time):
        self.log_path = log_path
        self.log_file_name = log_file_name
        self.current_data_path = current_data_path
        self.current_data_file_name = current_data_file_name
        self.sleep_time = sleep_time

    @property
    def full_path(self):
        return os.path.join(self.current_data_path, self.current_data_file_name)

    @property
    def log_full_path(self):
        return os.path.join(self.log_path, self.log_file_name)


class KMASetting(CommonSetting):
    """
    KMA 설정을 관리하는 클래스
    """
    def __init__(self,
                 log_path,
                 log_file_name,
                 current_data_path,
                 current_data_file_name,
                 sleep_time):
        super().__init__(log_path, log_file_name, current_data_path, current_data_file_name, sleep_time)


class JMASetting(CommonSetting):
    """
    JMA 설정을 관리하는 클래스
    """
    def __init__(self,
                 log_path,
                 log_file_name,
                 current_data_path,
                 current_data_file_name,
                 sleep_time):
        super().__init__(log_path, log_file_name, current_data_path, current_data_file_name, sleep_time)


class MailgunSetting(BaseSetting):
    """
    Mailgun과 관련된 설정을 관리하는 클래스
    """
    def __init__(self,
                 mg_api_key,
                 sender,
                 recipient,
                 domain):
        self.mg_api_key = mg_api_key
        self.sender = sender
        self.recipient = recipient
        self.domain = domain


class AWSSetting(BaseSetting):
    """
    AWS와 관련된 설정을 관리하는 클래스
    """
    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key,
                 region_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name


class GlobalSetting(BaseSetting):
    """
    전역 설정 파일을 관리하는 클래스
    """

    def __init__(self,
                 notification_dry_run,
                 firebase_secret_key_json_file,
                 data_path,
                 log_path,
                 mailgun,
                 aws,
                 kma_setting,
                 jma_setting,
                 notification_log_file,
                 gcloud_secret_key_json_file,
                 credential_path):
        self.notification_dry_run = notification_dry_run
        self.credential_path = credential_path
        self.gcloud_secret_key_json_file = gcloud_secret_key_json_file
        self.firebase_secret_key_json_file = firebase_secret_key_json_file
        self.mailgun = MailgunSetting(**mailgun)
        self.aws = AWSSetting(**aws)
        self.data_path = data_path
        self.log_path = log_path
        self.kma_setting = KMASetting(**kma_setting, current_data_path=data_path, log_path=log_path)
        self.jma_setting = JMASetting(**jma_setting, current_data_path=data_path, log_path=log_path)
        self.notification_log_file = notification_log_file

    @property
    def gcloud_secret_key(self):
        return os.path.join(self.credential_path, self.gcloud_secret_key_json_file)

    @property
    def firebase_secret_key(self):
        return os.path.join(self.credential_path, self.firebase_secret_key_json_file)

    @staticmethod
    def create():
        full_path = os.path.join(setting_directory_path, setting_file_name)
        with open(full_path, 'r') as setting:
            j = json.load(setting)
        return GlobalSetting(**j)
