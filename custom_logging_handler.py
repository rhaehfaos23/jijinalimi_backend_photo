import logging.handlers
import traceback
import requests
import informations as i

from datetime import datetime
from setting_management import MailgunSetting


class MailgunLogHandler(logging.handlers.HTTPHandler):
    def __init__(self, subject: str, setting: MailgunSetting):
        super().__init__('', '')
        self.subject = subject
        self.setting = setting

    def emit(self, record) -> None:
        text = f'[{record.asctime}] {record.levelname}: {record.message}\n'
        if record.exc_info is not None:
            text += traceback.format_exc()

        res = requests.post(i.mg_request_url.format(self.setting.domain),
                            auth=('api', self.setting.mg_api_key),
                            data={
                                'from': self.setting.sender,
                                'to': self.setting.recipient,
                                'subject': self.subject,
                                'text': text
                            })

        print(f'[{datetime.now()}] 에러 이메일 전송. {res.status_code} : {res.reason}')

