# -*- coding: utf-8 -*-
"""
    JijinAlimi Back-end Application - notification
        version 3.0 (with Flutter application v3.0)
    Made by Junhui Lee (logo@gomgom.net)
            Gomgom (dev@gomgom.net, https://www.gomgom.net)
"""

import copy
import json
import logging.handlers
import os
import re

import firebase_admin
from firebase_admin import credentials
from firebase_admin import exceptions
from firebase_admin import messaging

import translator
from custom_logging_handler import MailgunLogHandler
from setting_management import GlobalSetting

kma_topic = '토픽이름이 설정되지 않은 상태입니다.'  # 한국 기상청 토픽 이름
jma_topic = '토픽이름이 설정되지 않은 상태입니다.'  # 일본 기상청 토픽 이름
support_language = ['ko', 'ja', 'en', 'zh_Hans', 'zh_Hant']
regex = re.compile(r'\*[\w\\.]*\*')

notify_contents = None
global_setting: GlobalSetting

COUNTRY_KMA = 'kma'
COUNTRY_JMA = 'jma'

logger: logging.Logger


class NotInitializeNotifyContents(Exception):
    def __init__(self):
        super().__init__("전역 변수인 notify_contents가 초기화 되지 않았습니다. notify_contents_init() 함수를 호출 필요")


class NotSupportCountry(Exception):
    def __init__(self):
        super().__init__("지원 되지 않는 국가의 기상청. kma나 jma를 입력요망")


def notify_contents_init(setting: GlobalSetting):
    """
    알림을 보내기 위한 초기화 진행

    :param setting: 셋팅 클래스
    :raises CannotFindTopic: notification.json에서 토픽과 관련된 데이터를 찾지 못한 경우
    """
    global notify_contents
    global kma_topic
    global jma_topic
    global global_setting
    global logger

    with open('rules/notification.json', 'rb') as f:
        notify_contents = json.load(f)
        kma_topic = notify_contents['topics']['kma']
        jma_topic = notify_contents['topics']['jma']

    global_setting = setting
    cred = credentials.Certificate(global_setting.firebase_secret_key)  # 비 공개 생성키 파일 이름
    firebase_admin.initialize_app(cred)

    os.makedirs(global_setting.log_path, exist_ok=True)
    logger = logging.getLogger('notification')
    logger.setLevel(logging.DEBUG)
    fmtter = logging.Formatter(fmt=u'[%(asctime)s] %(levelname)s: %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')

    log_hdlr = logging.handlers.RotatingFileHandler(filename=os.path.join(global_setting.log_path, global_setting.notification_log_file),
                                                    maxBytes=1024 * 1024, backupCount=1, encoding='utf8')  # 파일 저장 로그
    log_hdlr.setFormatter(fmtter)
    log_hdlr.setLevel(logging.INFO)
    logger.addHandler(log_hdlr)

    stream_hdlr = logging.StreamHandler()
    stream_hdlr.setFormatter(fmtter)  # 콘솔 출력 로그
    stream_hdlr.setLevel(logging.ERROR)
    logger.addHandler(stream_hdlr)

    mail_hdlr = MailgunLogHandler('푸쉬 알림 경고!!!!', setting.mailgun)
    mail_hdlr.setLevel(logging.WARNING)
    mail_hdlr.setFormatter(fmtter)
    logger.addHandler(mail_hdlr)


def push_notify_kma(notify_content):
    send_message(notify_content, kma_topic)


def push_notify_jma(notify_content):
    send_message(notify_content, jma_topic)


def send_message(contents, topic):
    message = messaging.Message(
        topic=topic,
        data={'json': json.dumps(contents)}
    )
    try:
        response = messaging.send(message, dry_run=global_setting.notification_dry_run)
    except exceptions.FirebaseError:
        logger.exception('알림 보내기 실패 : Firebase로 알림을 보내는 도중에 실패 했습니다')
    except ValueError:
        logger.exception('알림 보내기 실패 : 무효한 인수가 들어 왔습니다.')
    else:
        logger.info(f'<{topic}> 알림 보내기 성공 - {response}')


def get_data(data: dict, *args):
    result = data
    for arg in args:
        if arg.isdigit():
            try:
                r = result[int(arg)]
            except IndexError:
                r = 'NULL'
            else:
                result = r
        else:
            r = result.get(arg)
            if r is None:
                return 'NULL'
            result = r
    return str(result)


def replace_data(data: dict, lan, match):
    m = match.group().strip('*').split('.')
    if len(m) == 1:
        if m[0] == 'datetime':
            return translator.translate_datetime(data['datetime'], translator.SupportedLanguages(lan))
        if m[0] == 'datetime_ann':
            return translator.translate_datetime(data['datetime_ann'], translator.SupportedLanguages(lan))
        if m[0] == 'location':
            return translator.translate_location(data['location'], translator.SupportedLanguages(lan))

    return get_data(data, *m)


def change_data(data: dict, country: str, notify_content):
    copy_notify_content = copy.deepcopy(notify_content)
    for key, value in copy_notify_content.items():
        if isinstance(value, str):
            d = data if country == COUNTRY_JMA else data.get('ko')
            result = regex.sub(lambda m: replace_data(d, 'ko', m), value)
            copy_notify_content[key] = result
        elif isinstance(value, dict):
            content = copy_notify_content[key]
            d = data if country == COUNTRY_JMA else data.get(key)
            for con_key, con_value in content.items():
                result = regex.sub(lambda m: replace_data(d, key, m), con_value)
                copy_notify_content[key][con_key] = result
    return copy_notify_content


def push_notify_support(data: dict, country, notify_content):
    """
    알림 보내기 도움 함수

    :param data: 지진 데이터
    :param country: 지진 데이터를 받아온 국가 (한국 또는 일본)
    :param notify_content: 푸쉬 알림 데이터
    :return:
    """
    copy_notify_content = change_data(data, country, notify_content)
    push = push_notify_jma if country == COUNTRY_JMA else push_notify_kma
    push(copy_notify_content)


def push_notify(data: dict, notify_type=0):
    """
    지진 보내기

    :param data: 지진 데이터
    :param notify_type: 지진 알림 종류 (한국은 0으로 고정)
    :return:
    :raises NotInitializeNotifyContents:  전역 notify_contents변수가 초기화 되지 않은 경우 발생, notify_contents_init 함수 호출로 해결
    :raises NotSupportCountry: data에 어느 국가 기상청인지에 대한 정보가 없거나 한국 또는 일본 이외의 기상청 데이터임
    """
    if notify_contents is None:
        raise NotInitializeNotifyContents()

    country = data.get('country')
    if country is None or country not in [COUNTRY_KMA, COUNTRY_JMA]:
        raise NotSupportCountry()

    t = f'case_{notify_type}'
    notify_content = notify_contents[country][t]
    push_notify_support(data, country, notify_content)
