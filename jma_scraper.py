# -*- coding: utf-8 -*-
"""
    JijinAlimi Back-end Application - JMA EQ Information Scraper
        version 3.0 (with Flutter application v3.0)
    Made by Junhui Lee (logo@gomgom.net)
            Gomgom (dev@gomgom.net, https://www.gomgom.net)
"""
import ast
import copy
import datetime
import json
import logging.handlers
import math
import os
import socket
import sys
import time

import requests
from PIL import Image
from bs4 import BeautifulSoup

import aws_s3
import informations as i
import notification
from setting_management import GlobalSetting
from custom_logging_handler import MailgunLogHandler

# 일본 기상청 API 에서 관심있는 타이틀 정보
eqk_info_list = [
    '震度速報',
    '震源に関する情報',
    '震源・震度に関する情報'
]

# 일본 기상청에서 불러온 이전 데이터의 id 값
ids = ''

# 일본 기상청에서 불러온 이전 지도 이미지 url 값
jma_img_url = ''


class NotSupportedData(Exception):
    def __init__(self):
        super().__init__('지원하는 데이터 클래스가 아닙니다')


class EmptyData(Exception):
    def __init__(self):
        super().__init__('필요한 데이터가 비어 있습니다')
        

class FileDownloadFail(Exception):
    def __init__(self):
        super().__init__('파일 다운로드가 실패하였습니다')


def is_affect_korea(data: dict) -> (bool, int):
    """
    이 지진이 한국에 영향을 끼치는지 확인

    :param data: 일본 기상청 지진 정보
    :return: 영향을 주는지 여부
    """
    # 진원지가 한반도일경우 알림 0번
    hypocenter = data.get('hypocenter')
    if hypocenter is not None and hypocenter == '朝鮮半島南部':
        return True, 0
    local_max_int = data.get('local_name_and_max_int')
    if local_max_int is not None and "長崎県対馬" in local_max_int.keys():
        tsusima_maxint = local_max_int.get("長崎県対馬")
        # 대마도에 진도 2 이상인 경우에만 알림 실시
        if tsusima_maxint < 2:
            return False, -1
        else:
            fukuoka_maxint = local_max_int.get("福岡県福岡")
            # 후쿠시마에 지진이 느껴졌고 대마도 진도가 후쿠시마 진도보다 크거나 같으면 알림 1
            if fukuoka_maxint is not None and tsusima_maxint >= fukuoka_maxint:
                num = 1 if data.get('notify_type') == 2 else 3
                return True, num
            # 기타 대마도 진도가 2 이상인 경우
            num = 2 if data.get('notify_type') == 2 else 3
            return True, num
    else:
        return False, -1


def _save_data_s3(data):
    """
    s3에 지진 데이터를 저장하는 함수

    :param data: 지진데이터
    :return: 저장 성공 여부
    :raises NotSupportedData: data 파라메터가 EqkDataJma를 상속받는 클래스가 아니면 발생
    """
    logger.info('데이터 저장 시작')
    if not isinstance(data, EqkDataJma):
        raise NotSupportedData()

    dict_data = {'jijin_data': data.to_dict()}
    file_name = 'data_jma_sindo_{0}.json' if isinstance(data, EqkSindoData) else 'data_jma_singen_{0}.json'
    try:
        for language in ['ko', 'ja', 'en', 'zh_Hans', 'zh_Hant']:
            aws_s3.save_s3(json.dumps(dict_data),
                           aws_s3.JSON_PATH.format(file_name.format(language)),
                           aws_s3.JSON_CONTENT)
    except Exception as e:
        logger.warning('데이터 저장 실패')
        return False

    logger.info('데이터 저장 성공')
    return True


def _save_image_s3(data):
    """
    s3에 지진 지도 이미지를 저장하는 함수

    :param data: 지진 데이터
    :return: 저장 성고 여부
    :raises NotSupportedData: data 파라메터가 EqkDataJma를 상속받는 클래스가 아니면 발생
    :raises EmptyData: data파라메터의 img_url이나 img_name멤버가 비어있으면 발생
    """
    if not isinstance(data, EqkDataJma):
        raise NotSupportedData()
    elif data.img_name == '' or data.img_name is None or data.img_url == '' or data.img_url is None:
        raise EmptyData()

    filename = data.img_name
    url = data.img_url
    img_size = (550, 449)

    # Download earthquake image from JMA
    with open(filename, 'wb') as f:
        f.write(requests.get(url, params='bytes').content)

    # Check file size that it is right image.
    file_size = os.path.getsize(filename)
    if file_size < 1024:  # Maybe Error to download
        raise FileDownloadFail()

    try:
        img = Image.open(filename)
    except IOError:
        logger.warning('이미지 리사이징 실패')
    else:
        try:
            img_resize = img.resize(img_size, Image.ANTIALIAS)
            img_resize.save(filename, optimize=True)
        except IOError:
            logger.warning('이미지 리사이징 실패')
            return False
        except KeyError:
            logger.warning('이미지 리사이징 실패')
            return False

    try:
        with open(filename, 'rb') as f:
            aws_s3.save_s3(f, aws_s3.IMAGE_PATH.format(filename), aws_s3.IMAGE_PNG_CONTENT)
    except Exception as e:
        logger.warning('이미지 저장 실패')
        return False
    else:
        logger.info('이미지 리사이징 성공')

    # Delete file from server.
    os.remove(filename)
    return True


class EqkDataJma:
    def __init__(self, uuid, notify_type, notify_type_text):
        self.country = notification.COUNTRY_JMA
        self.id = uuid
        self.notify_type = notify_type  # 통보 종류
        self.notify_type_text = notify_type_text  # 통보 종류 이름
        self.img_url = ''
        self.img_name = ''

    def to_dict(self):
        return dict(map(lambda item: (item[0], ast.literal_eval(repr(item[1]))), vars(self).items()))

    def save(self, img_save=True):
        _save_data_s3(self)
        if img_save:
            _save_image_s3(self)


class EqkSindoData(EqkDataJma):
    """
    진도 데이터
    """
    def __init__(self, uuid, notify_type, notify_type_text):
        super().__init__(uuid, notify_type, notify_type_text)
        self.max_intensity = None
        self.datetime_ann = None
        self.local_name_and_max_int = None

    def __repr__(self):
        return str(vars(self))

    def to_dict(self):
        return dict(map(lambda item: (item[0], ast.literal_eval(repr(item[1]))), vars(self).items()))

    @staticmethod
    def create(xml_data, uuid, notify_type, notify_type_text, img_load=True):
        logger.info('진도에 관한 데이터 생성 시작')
        rv = EqkSindoData(uuid, notify_type, notify_type_text)

        # 최대 진도
        try:
            intensity = int(xml_data.select_one('Observation > MaxInt').text)
        except Exception:
            logger.warning('진도에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.max_intensity = intensity

        # 발표시각
        try:
            date_ann = datetime.datetime.fromisoformat(xml_data.select_one('ReportDateTime').text)
        except Exception:
            logger.warning('진도에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.datetime_ann = date_ann.strftime('%Y-%m-%d %H:%M:%S')

        # 지역별 최대 진도
        try:
            location_max_int = dict(
                map(lambda area: (area.find('Name').text, int(area.find('MaxInt').text)),
                    xml_data.select('Pref > Area')))
        except Exception:
            logger.warning('진도에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.local_name_and_max_int = location_max_int

        if img_load:
            img_url, img_name = img_parsing(i.jma_quake_sindo_index)
            if img_url == '':
                logger.warning('진도에 관한 데이터 생성 실패')
                return False, None
            rv.img_url = img_url
            rv.img_name = img_name

        logger.info('진도에 관한 데이터 생성 성공')
        return True, rv


class EqkSingenData(EqkDataJma):
    """
    진원 데이터
    """
    def __init__(self, uuid, notify_type, notify_type_text):
        super().__init__(uuid, notify_type, notify_type_text)
        self.datetime = None
        self.datetime_ann = None
        self.hypocenter = None
        self.coordinate = None
        self.magnitude = None

    def __repr__(self):
        return str(vars(self))

    def to_dict(self):
        return dict(map(lambda item: (item[0], ast.literal_eval(repr(item[1]))), vars(self).items()))

    @staticmethod
    def create(xml_data, uuid, notify_type, notify_type_text, img_load=True):
        logger.info('진원에 관한 데이터 생성 시작')
        rv = EqkSingenData(uuid, notify_type, notify_type_text)

        # 진앙시
        try:
            date_str = xml_data.select_one('OriginTime').text
            date = datetime.datetime.fromisoformat(date_str)
        except:
            logger.warning('진원에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.datetime = date.strftime('%Y-%m-%d %H:%M:%S')

        # 발표시각
        try:
            date_ann = datetime.datetime.fromisoformat(xml_data.select_one('ReportDateTime').text)
        except:
            logger.warning('진원에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.datetime_ann = date_ann.strftime('%Y-%m-%d %H:%M:%S')

        # 진앙지
        try:
            hypocenter = xml_data.select('Hypocenter Name')[0].text
        except:
            logger.warning('진원에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.hypocenter = hypocenter

        # 규모
        try:
            mag = float(xml_data.find('jmx_eb:Magnitude').text)
        except:
            logger.warning('진원에 관한 데이터 생성 실패')
            return False, None
        else:
            rv.magnitude = mag

        # 좌표
        try:
            coord = xml_data.find('jmx_eb:Coordinate').text
        except:
            logger.warning('진원에 관한 데이터 생성 실패')
            return False, None
        else:
            coord_info = ['', '', '']
            count = 0
            word = ''
            for c in coord:
                if c == '/':
                    coord_info[2] = word
                    break
                elif c == '+' or c == '-':
                    count += 1
                    if count > 1:
                        coord_info[count - 2] = word
                        word = ''
                    word += c
                else:
                    word += c
            loc_n, loc_e, depth = coord_info
            rv.coordinate = EqkCoordinate(float(loc_n), float(loc_e), float(depth))

        if img_load:
            img_url, img_name = img_parsing(i.jma_quake_singen_index)
            if img_url == '':
                logger.warning('진원에 관한 데이터 생성 실패')
                return False, None
            rv.img_url = img_url
            rv.img_name = img_name

        logger.info('진원에 관한 데이터 생성 성공')
        return True, rv


class EqkSingendoData(EqkDataJma):
    """
    진원, 진도 데이터
    """
    def __init__(self, uuid, notify_type, notify_type_text):
        super().__init__(uuid, notify_type, notify_type_text)
        self.sindo_data = None
        self.singen_data = None

    def __repr__(self):
        return str(vars(self))

    def to_dict(self):
        singendo = copy.deepcopy(vars(self))
        sindo = self.sindo_data.to_dict()
        singen = self.singen_data.to_dict()

        del singendo['sindo_data']
        del singendo['singen_data']

        for key, value in sindo.items():
            if key not in singendo.keys():
                singendo[key] = value

        for key, value in singen.items():
            if key not in singendo.keys():
                singendo[key] = value

        return singendo

    def save(self, img_save=True):
        self.singen_data.save(img_save=False)
        self.sindo_data.save(img_save=False)
        _save_image_s3(self)

    @staticmethod
    def create(xml_data, uuid, notify_type, notify_type_text):
        logger.info('진원 진도에 관한 데이터 생성 시작')
        rv = EqkSingendoData(uuid, notify_type, notify_type_text)

        success, data = EqkSindoData.create(xml_data, uuid, notify_type, notify_type_text, False)
        if success:
            rv.sindo_data = data
        else:
            return False, None

        success, data = EqkSingenData.create(xml_data, uuid, notify_type, notify_type_text, False)
        if success:
            rv.singen_data = data
        else:
            return False, None

        img_url, img_name = img_parsing(i.jma_quake_singendo_index)
        if img_url == '':
            logger.warning('진원 진도에 관한 데이터 생성 실패')
            return False, None

        rv.img_url = img_url
        rv.img_name = img_name
        rv.sindo_data.img_url = img_url
        rv.sindo_data.img_name = img_name
        rv.singen_data.img_url = img_url
        rv.singen_data.img_name = img_name
        logger.info('진원 진도에 관한 데이터 생성 성공')
        return True, rv


def img_parsing(jma_url, jma_access_max_count=10):
    """
    일본 기상청에서 지진 지도 불러오기

    :param jma_url: 일본 기상청 url
    :param jma_access_max_count: 지도를 불러오기위해 시도해보는 횟수
    :return: (이미지 url, 파일 이름)
    """
    logger.info('이미지 불러오기 시작')
    try:
        jma_html = requests.get(jma_url)
        jma_html.raise_for_status()
        jma_html.encoding = 'utf8'
        jma_bs = BeautifulSoup(jma_html.text, 'html.parser')
        temp_url = jma_bs.select_one('div.infotable a')['href']
        url = i.jma_url + temp_url[2:]
        count = 0
        while count < jma_access_max_count:
            try:
                req = requests.get(url=url, timeout=10)
                req.encoding = 'utf8'
                req.raise_for_status()
            except requests.exceptions.Timeout as e:
                logger.warning('이미지를 불러오는데 시간이 너무 오래 걸림')
                time.sleep(2 ** count)
                count += 1
                continue
            except socket.timeout as e:
                logger.warning('이미지를 불러오는데 시간이 너무 오래 걸림')
                time.sleep(2 ** count)
                count += 1
                continue
            except requests.exceptions.HTTPError as e:
                logger.warning('이미지를 불러오기 실패')
                return '', ''
            else:
                bs = BeautifulSoup(req.text, 'html.parser')
                img_url = bs.find('img', attrs={'usemap': '#quakemap'})['src']
                img_url = i.jma_url + img_url[2:]
                return img_url, 'jma_eq_' + img_url.split('/')[-1]
    except:
        logger.warning('이미지 불러오기 실패')
        return '', ''


class EqkCoordinate:
    """
    지진 좌표를 저장하는 클래스
    """

    def __init__(self, latitude, longitude, depth):
        self.latitude = latitude  # 위도
        self.longitude = longitude  # 경도
        self.depth = depth  # 깊이

    def __repr__(self):
        return str(vars(self))

    def to_dict(self):
        return vars(self)


def create_eqk_sindo_data(xml_data, uuid, notify_type, notify_type_text):
    return EqkSindoData.create(xml_data, uuid, notify_type, notify_type_text)


def create_eqk_singen_data(xml_data, uuid, notify_type, notify_type_text):
    return EqkSingenData.create(xml_data, uuid, notify_type, notify_type_text)


def create_eqk_singendo_data(xml_data, uuid, notify_type, notify_type_text):
    return EqkSingendoData.create(xml_data, uuid, notify_type, notify_type_text)


def create_eqk_data_support(entry, notify_type):
    uuid = entry.id.text
    notify_type = notify_type
    notify_type_text = eqk_info_list[notify_type]
    xml = requests.get(entry.link.attrs['href'])
    xml.encoding = 'utf-8'
    xml_p = BeautifulSoup(xml.text, 'lxml-xml')
    if notify_type == 0:
        return create_eqk_sindo_data(xml_p, uuid, notify_type, notify_type_text)
    elif notify_type == 1:
        return create_eqk_singen_data(xml_p, uuid, notify_type, notify_type_text)
    elif notify_type == 2:
        return create_eqk_singendo_data(xml_p, uuid, notify_type, notify_type_text)


def create_eqk_data(entry, nt_tp):
    """
    일본 기상청 API로부터 지진 데이터를 저장한 클래스를 만듦
    
    :param entry: 새로운 지진 데이터가 저장된 entry태그
    :param nt_tp: 앱에 알림 타입(?)
    :return: 지진 데이터를 저장한 클래스
    :rtype: (bool, EqkSingendoData)
    """
    return create_eqk_data_support(entry, nt_tp)


def data_save_notify(data):
    """
    일본 기상청으로부터 불러온 데이터를 가지고 한국에 영향을 주는 지진인지 판단한 후에
    영향을 주는 지진이면 알림을 보냄과 동시에 S3에 데이터를 저장
    
    :param data: 일본 기상청으로부터 불러온 데이터
    """
    affected, num_notify = is_affect_korea(data.to_dict())
    if affected:
        logger.info("한국에 영향을 주는 지진을 불러옴. 저장 및 알림 시작")
        notification.push_notify(data.to_dict(), num_notify)
        data.save()
    else:
        logger.info("한국에 영향을 주지 않는 지진을 불러옴. 저장 및 알림 없음")

    with open(setting.jma_setting.full_path, 'w') as f:
        f.write(data.id)


def crawling_start(sleep: float):
    """
    새로운 지진 정보가 있는지 확인하고 S3에 저장하는 함수

    :param sleep: 몇초에 한번씩 크롤링 할것인가
    """
    global ids
    restarted = os.path.exists(setting.jma_setting.full_path)
    if not restarted:
        print(f'[{datetime.datetime.now()}] <JMA> 처음으로 프로그램 실행')
    else:
        print(f'[{datetime.datetime.now()}] <JMA> 프로그램 재실행')

    dt = sleep - 0.1
    while True:
        t = math.fmod(dt, sleep)
        time.sleep(sleep - t)
        logger.info("지진 데이터를 불러오는 사이클 시작")
        start_time = time.time()
        # 기상청에서 xml데이터 가져오기
        xml = requests.get(i.jma_xml_url)
        xml.encoding = 'utf-8'
        # xml 파싱
        bs = BeautifulSoup(xml.text, 'lxml-xml')
        # 관심 있는 데이터만 필터링
        entrys = bs.find_all('entry')
        entrys = list(filter(lambda entry: entry.title.text in eqk_info_list, entrys))
        # 관심 있는 데이터가 한개라도 있으면
        if len(entrys) > 0:
            first = entrys[0]
            # 이전에 불러온 id와 비교해서 다르면
            if first.id.text != ids:
                logger.info("새로운 지진을 불러들임")
                success, data = create_eqk_data(first, eqk_info_list.index(first.title.text))
                # 지진 데이터를 불러오는데 성공하면
                if success:
                    if restarted:
                        logger.info("재시작 루틴")
                        restarted = False
                        with open(setting.jma_setting.full_path, 'r') as f:
                            file_uid = f.readline()
                        # 파일에 저장된 uid와 새로 불러온 uid가 같으면
                        if first.id.text == file_uid:
                            continue
                    ids = first.id.text
                    logger.info("새로운 지진을 불러들이는데 성공함")
                    data_save_notify(data)
                # 지진 데이터를 불러오는데 실패하면
                else:
                    logger.info("모종의 이유로 지진을 불러들이는데 실패함")
            # 이전에 불러온 id와 비교해서 같으면
            else:
                logger.info("새로운 지진데이터가 없음")
        # 관심 있는 데이터가 한개도 없으면
        else:
            logger.info("API에 지진과 관련된 데이터가 없음")
        end_time = time.time()
        dt = end_time - start_time
        logger.info(f"지진 데이터를 불러오는 사이클 종료. 소요시간 : {dt}")


if __name__ == "__main__":
    # 설정파일로부터 설정값을 불러옴
    setting = GlobalSetting.create()

    # 설정된 값이 0보다 이하면
    if setting.jma_setting.sleep_time <= 0:
        # 프로그램 종료
        sys.exit("Time value is zero or under zero.")

    # Print current setting value.
    print(f"JMA Scraper Service is running... Time value is {setting.jma_setting.sleep_time} second(s).")

    # 로거 초기화
    os.makedirs(setting.log_path, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fmtter = logging.Formatter(fmt=u'[%(asctime)s] %(levelname)s: %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')

    log_hdlr = logging.handlers.RotatingFileHandler(filename=setting.jma_setting.log_full_path,
                                                    maxBytes=1024 * 1024, backupCount=1, encoding='utf8')  # 파일 저장 로그
    log_hdlr.setFormatter(fmtter)
    log_hdlr.setLevel(logging.INFO)
    logger.addHandler(log_hdlr)

    stream_hdlr = logging.StreamHandler()
    stream_hdlr.setFormatter(fmtter)  # 콘솔 출력 로그
    stream_hdlr.setLevel(logging.ERROR)
    logger.addHandler(stream_hdlr)

    mail_hdlr = MailgunLogHandler('일본 기상청 크롤러로부터 경고!!!', setting.mailgun)
    mail_hdlr.setLevel(logging.WARNING)
    mail_hdlr.setFormatter(fmtter)
    logger.addHandler(mail_hdlr)

    # 설정된 데이터를 저장할 디렉토리가 존재하지 않으면
    if not os.path.exists(setting.jma_setting.current_data_path):
        # 디렉토리 생성
        os.makedirs(setting.jma_setting.current_data_path)

    # 푸쉬 알림을 위한 초기화 진행
    notification.notify_contents_init(setting)
    aws_s3.init_aws_s3(setting)
    # 크롤링 시작
    crawling_start(setting.jma_setting.sleep_time)
