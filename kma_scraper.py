# -*- coding: utf-8 -*-
"""
    JijinAlimi Back-end Application - KMA EQ Information Scraper
        version 3.0 (with Flutter application v3.0)
    Made by Junhui Lee (logo@gomgom.net)
            Gomgom (dev@gomgom.net, https://www.gomgom.net)
"""
import ast
import datetime
import json
import logging.handlers
import math
import os
import re
import socket
import sys
import time

import requests
from PIL import Image
from bs4 import BeautifulSoup
from google.cloud import translate
from retrying import retry

import aws_s3
import informations as i
import notification
from custom_logging_handler import MailgunLogHandler
from setting_management import GlobalSetting

# Initiate default values.
prev_data = None
pre_translated_data = None

EQK_TYPE_INFO = '3'
EQK_TYPE_BREAKING_INFO = '14'

code: dict

# 로마 숫자 1-10
rome_numeral_numbers = [u"\u2160", u"\u2161", u"\u2162", u"\u2163", u"\u2164", u"\u2165", u"\u2166", u"\u2167",
                        u"\u2168", u"\u2169"]

# 지진 정보
eqk_datetime = '발생시각'
eqk_magnitude = '규모(불확도)'
eqk_max_int = '계기진도'
eqk_location_coord = '발생위치(위도,경도,불확도)'
eqk_remain = '참고사항'

# 지진 속보
eqk_breaking_datetime = '발생시각'
eqk_breaking_magnitude = '추정규모'
eqk_breaking_max_int = '예상진도'
eqk_breaking_location_coord = '추정위치'
eqk_breaking_remain = '참고사항'

# 지역 슬라이싱
wide_or_north = slice(0, 1)
city = slice(1, -3)
direction = slice(-3, -2)
distance = slice(-2, -1)

push_data = {'country': 'kma'}


def _pre_translate(text, dest: str):
    """
    미리 번역된 지역명에서 번역본 찾아오기

    :param text: 지역명
    :param dest: 번역할 언어
    :return: 미리 번역된 번역본이 있는지 여부, 번역된 언어
    """
    if isinstance(text, int):
        return text

    if text in pre_translated_data:
        return pre_translated_data[text][dest]
    else:
        return text+"_"


def _translate_location_str(text: list, lan):
    """
    DataTranslateFileSave 클래스에서 사용하는 도우미 함수\n
    text가 str이면 번역을 해서 리턴을 해주고\n
    text가 str이 아니면 그대로 리턴

    :param text: value
    :type text: Any
    :param lan: 번역할 목적 언어
    :type lan: str
    """
    result = [_pre_translate(translated, lan) for translated in text]
    temp_result = [x.rstrip('_') for x in result if isinstance(x, str) and x[-1] == '_']
    if len(temp_result) > 0:
        response = client.translate_text(
            parent=parent,
            contents=temp_result,
            mime_type="text/plain",  # mime types: text/plain, text/html
            source_language_code="ko",
            target_language_code=lan,
        )

        j = 0
        for idx, txt in enumerate(result):
            if isinstance(txt, str) and txt[-1] == '_':
                result[idx] = response.translations[j].translated_text
                j += 1

    return result


class ImageFileDownloadFail(Exception):
    def __init__(self):
        super().__init__("이미지를 받아오는데 실패 했습니다.")


class EqkLocationKma:
    """
    지진 위치를 저장하는 클래스
    """

    def __init__(self, wide, city, direction, distance, depth):
        self.wide = wide
        self.city = city
        self.direction = direction
        self.distance = int(re.search(r'\d*', distance).group())
        self.depth = depth
        self.code = code[self.wide]

    def __repr__(self):
        return str(vars(self))


class EqkCoordKma:
    """
    지진 좌표 저장 클래스
    """

    def __init__(self, longitude: float, latitude: float):
        self.longitude = longitude
        self.latitude = latitude

    def __repr__(self):
        return str(vars(self))


class EqkDataKma:
    """
    지진 정보 데이터를 저장하는 클래스
    """

    def __init__(self, uid: str, datetime: datetime.datetime, datetime_ann: datetime.datetime, location: str,
                 coord: EqkCoordKma, magnitude: float, max_intensity: int, img_name: list, fctp: str, img_url: list,
                 region_intensity: dict, note: str, depth: float):
        self.country = notification.COUNTRY_KMA
        self.uid = uid  # 고유번호 (진앙시 + 발표일련번호) ex) 201001010008
        self.datetime = datetime.strftime('%Y-%m-%d %H:%M:%S')  # 진앙시
        self.datetime_ann = datetime_ann.strftime('%Y-%m-%d %H:%M:%S')  # 발표시각
        split_str = location.split()
        self.location = EqkLocationKma(
            ' '.join(split_str[wide_or_north]),
            ' '.join(split_str[city]),
            ' '.join(split_str[direction]),
            ' '.join(split_str[distance]),
            depth
        )
        self.coord = coord
        self.magnitude = magnitude  # 규모
        self.max_intensity = max_intensity  # 최대 진도
        self.img_name = img_name  # 이미지 파일 이름
        self.fctp = fctp  # 통보 종류
        self.img_url = img_url  # 이미지 url
        self.region_intensity = region_intensity
        self.note = note

    def __eq__(self, other):
        if other is None:
            return False
        return self.uid == other.uid

    def __ne__(self, other):
        return not (self == other)

    def to_dict(self):
        """
        클래스를 dictionary 형식으로 변환해주는 함수
        """
        return dict(map(lambda item: (item[0], ast.literal_eval(repr(item[1]))), vars(self).items()))


class EqkBaseData:
    """
    지진 데이터를 저장하는 기본 클래스
    """

    def __init__(self, tp, kind, tm_fc, tm_seq, data, tp_text, title_text, tm_fc_text):
        self.tp = tp
        self.kind = kind
        self.tm_fc = tm_fc
        self.tm_seq = tm_seq
        self.data = data
        self.tp_text = tp_text
        self.title_text = title_text
        self.tm_fc_text = tm_fc_text


class DataSaver:
    """
    지진 데이터를 저장하는 베이스 클래스
    """

    def __init__(self, data):
        self.data = {'jijin_data': data.to_dict()}

    def save(self):
        raise NotImplementedError("이 함수는 서브클래스에서 정의될 필요가 있습니다.")


class S3ImageSaverKma(DataSaver):
    """
    지진 이미지를 S3에 저장하게 하는 클래스
    """

    def __init__(self, data):
        super().__init__(data)
        self.error_count = 0

    def _image_resizing(self):
        img_sizes = [(450, 444), (550, 471)]
        for i in range(2):
            filename = self.data['jijin_data']['img_name'][i]
            url = self.data['jijin_data']['img_url'][i]
            img_size = img_sizes[i]
            # Download earthquake image from KMA
            with open(filename, 'wb') as f:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.warning(" 이미지 다운 로드 실패")
                    raise
                except Exception as e:
                    logger.warning("이미지 다운 로드 실패")
                    raise
                else:
                    total_size = int(response.headers.get('content-length')) if response.headers.get(
                        'content-length') is not None else 1024
                    f.write(response.content)

            # 이미지를 제대로 다운 받았는지 확인
            try:
                file_size = os.path.getsize(filename)
                if file_size < total_size:  # 파일 다운로드 실패
                    raise ImageFileDownloadFail()
            except ImageFileDownloadFail as e:
                logger.warning("이미지 다운로드 실패")
                raise

            logger.info("이미지 다운로드 성공")
            # 이미지 사이즈 변경 최적화
            try:
                logger.info("이미지 리사이징")
                img = Image.open(filename)
                img_resize = img.resize(img_size, Image.ANTIALIAS)
                img_resize.save(filename, optimize=True)
            except Exception as e:
                logger.warning("이미지 리사이징 실패")
                raise
            logger.info("이미지 리사이징 성공")

    @retry(wait_fixed=1000)
    def save(self):
        logger.info("지진 이미지 저장 시작")

        self._image_resizing()

        for idx in range(2):
            filename = self.data['jijin_data']['img_name'][idx]
            # S3에 저장
            try:
                with open(filename, 'rb') as file:
                    aws_s3.save_s3(file, aws_s3.IMAGE_PATH.format(filename), aws_s3.IMAGE_PNG_CONTENT)
            except Exception as e:
                self.error_count += 1
                logger.exception(f"이미지 S3에 저장 실패. {self.error_count}")
                raise
            else:
                logger.info("이미지 S3에 저장 성공")

            # Delete file from server.
            os.remove(filename)


class DataTranslateFileSaver(DataSaver):
    def __init__(self, data: EqkDataKma, language: str):
        super().__init__(data)
        if language not in ['ko', 'en', 'ja', 'zh_Hans', 'zh_Hant']:
            raise Exception("번역할수 없는 언어입니다.")
        if language in ['ko', 'en', 'ja']:
            self.language = language
        elif language == 'zh_Hans':
            self.language = 'zh-cn'
        elif language == 'zh_Hant':
            self.language = 'zh-tw'
        self.ori_language = language
        self.file_name = f'data_kma_{language}.json'
        self.error_count = 0

    def _translate_data(self):
        """
        데이터를 번역
        """
        logger.info(f"데이터 번역시작. 언어 : {self.language}")
        if self.language != 'ko':
            # 지진 발생지역을 각 언어로 번역
            self.data['jijin_data']['location'] = {k: v for k, v in zip(self.data['jijin_data']['location'].keys(), _translate_location_str(self.data['jijin_data']['location'].values(), self.language))}

            # 각 시/도 명을 번역
            self.data['jijin_data']['region_intensity'] = {k: v for k, v in zip(_translate_location_str(self.data['jijin_data']['region_intensity'].keys(), self.language), self.data['jijin_data']['region_intensity'].values())}

            # 참고 사항
            self.data['jijin_data']['note'] = _translate_location_str([self.data['jijin_data']['note']], self.language)[0]

            # 중국어인 경우 방향에서 '方'자를 지움
            if self.language.find('zh') != -1:
                self.data['jijin_data']['location']['direction'] = self.data['jijin_data']['location'][
                    'direction'].replace('方', '')

        push_data[self.ori_language] = self.data['jijin_data']
        logger.info(f"데이터 번역종료. 언어 : {self.language}")

    @retry(wait_fixed=1000)
    def save(self):
        self._translate_data()
        # S3에 저장
        logger.info("번역된 데이터 S3에 저장 시작")
        try:
            aws_s3.save_s3(json.dumps(self.data), aws_s3.JSON_PATH.format(self.file_name), aws_s3.JSON_CONTENT)
        except Exception as e:
            self.error_count += 1
            logger.warning(f"S3에 파일 저장 실패. 파일 이름 : {self.file_name}. 시도 횟수 : {self.error_count}")
            raise e
        else:
            logger.info("번역된 데이터 S3에 저장 성공")


class DataSaverListSaver(DataSaver):
    """
    지진 데이터를 저장하는 Saver클래스의 리스트를 받아 그 클래스의 save 함수를 호출해주는 클래스
    """

    def __init__(self, data, *savers):
        super().__init__(data)
        check = True
        for s in savers:
            check &= issubclass(type(s), DataSaver)
        if not check:
            raise ValueError('savers 인수에 DataSaver의 서브클래스가 아닌 변수가 포함되 있습니다.')
        self.savers = savers

    def save(self):
        for s in self.savers:
            s.save()


@retry(wait_fixed=1000)
def create_base_data():
    """
    기상청 지진 목록으로 부터 기초적인 정보를 만들어주는 함수입니다.

    :return: 새로운 데이터가 있는지 여부, 기초 데이터(없으면 None)
    """
    logger.info("기초 데이터 불러오기 시작")
    global prev_data
    error_count = 0
    while True:
        try:
            response = requests.get(url=i.kma_list_url, timeout=10)
            response.encoding = 'utf8'
            response.raise_for_status()
        # 기상청에서 시간안에 응답이 없으면
        except (requests.exceptions.Timeout, socket.timeout) as e:
            time.sleep(min(120, 2 ** error_count))
            error_count += 1
            if error_count > 2:
                logger.warning(f'상세데이터 파싱 중 알수 없는 이유로 파싱 실패. 시도횟수 : {error_count}')
            continue
        except requests.exceptions.HTTPError as e:
            logger.exception("기초 데이터 불러오기 실패, 프로그램 종료됨")
            exit(1)
        else:
            try:
                data = response.json()
            except ValueError:
                logger.exception("기초 데이터를 불러왔으나 JSON 형식이 아님, 프로그램 종료됨")
                exit(1)
            else:
                if len(data) == 0:
                    return False, None
                filter_data = [d for d in data if d['tp'] == EQK_TYPE_INFO or d['tp'] == EQK_TYPE_BREAKING_INFO]
                if len(filter_data) == 0:
                    return False, None
                data = filter_data[0]
                logger.info(f"이전 Data : {prev_data}, 최근 Data : {data['data']}")
                if data['data'] != prev_data:
                    prev_data = data['data']
                    ret = EqkBaseData(data['tp'],         # 통보 종류 코드
                                      data['kind'],       # 통보 종류 한글
                                      data['tmFc'],       # 발표 시각
                                      data['tmSeq'],      # 일련번호
                                      data['data'],       # 데이터
                                      data['tpText'],     # 뭐지? 전부 비어있음
                                      data['titleText'],  # 타이틀 텍스트
                                      data['tmFcText'])   # 발표시간 텍스트
                    logger.info("기초 데이터 불러오기 성공")
                    return True, ret
                else:
                    logger.info("새로운 지진 데이터 없음")
                    return False, None


def create_data():
    """
    기상청 상세 정보 페이지로부터 실제 사용할 데이터를 뽑는 함수
    
    :retrun: 새로운 정보를 불러오기 성공여부(True, False), 새로운 정보 데이터 클래스(없으면 None)
    :rtype: (bool, EqkKmaData)
    """
    success, base_data = create_base_data()

    if not success:
        logger.info("데이터 생성 실패")
        return False, None

    error_count = 0

    while True:
        try:
            detail_url_param = {'eqk': base_data.data}
            response = requests.get(i.kma_detail_url, params=detail_url_param, timeout=10)
            response.encoding = 'utf8'
            response.raise_for_status()
        except (requests.exceptions.Timeout, socket.timeout) as e:
            time.sleep(min(120, 2 ** error_count))
            error_count += 1
            if error_count > 2:
                logger.warning(f'상세데이터 파싱 중 알수 없는 이유로 파싱 실패. 시도횟수 : {error_count}')
            continue
        except requests.exceptions.HTTPError as e:
            logger.exception("HTTP에러로 상세 데이터 불러오기 실패. 프로그램 종료")
            exit(1)
        except Exception:
            logger.exception("알수 없는 이유로 상세 데이터 불러오기 실패, 프로그램 종료")
            exit(1)
        else:
            logger.info("상세 데이터 파싱 시작")
            try:
                # 지진 발표 시각
                kma_datetime_ann = datetime.datetime.strptime(base_data.tm_fc, '%Y%m%d%H%M')
                bs_detail_page = BeautifulSoup(response.text, 'html.parser')

                # 시도별 진도 데이터를 뽑아옴
                table = bs_detail_page.select_one('table.table-col.eqk-city-ins-table > tbody')
                city_max_int = {r.th.text: rome_numeral_numbers.index(r.td.img['alt'])+1 for r in table.find_all('tr')}

                # 지진 정보 테이블
                table = bs_detail_page.select_one('div.over-scroll.cont-box-eqk > table > tbody')
                regex = re.compile(r'[\r\t]|\xa0')
                eqk_info = {regex.sub('', r.th.text.strip()): regex.sub('', r.td.text.strip())
                            for r in table.find_all('tr')}

                # 테이블 키 종류(?) 뭐라 해야되지
                search_datetime = ''
                search_magnitude = ''
                search_max_int = ''
                search_location_coord = ''
                search_remain = ''
                depth = 0

                # 지진 정보
                if base_data.tp == EQK_TYPE_INFO:
                    search_datetime = eqk_datetime
                    search_magnitude = eqk_magnitude
                    search_max_int = eqk_max_int
                    search_location_coord = eqk_location_coord
                    search_remain = eqk_remain
                    try:
                        depth = int(re.match(r'\d+\.?\d*', table.find('td', text=re.compile(r'\d+\.?\d* km$')).text).group())
                    except (ValueError, AttributeError):
                        depth = 0

                # 지진 속보
                elif base_data.tp == EQK_TYPE_BREAKING_INFO:
                    search_datetime = eqk_breaking_datetime
                    search_magnitude = eqk_breaking_magnitude
                    search_max_int = eqk_breaking_max_int
                    search_location_coord = eqk_breaking_location_coord
                    search_remain = eqk_breaking_remain
                    depth = 0

                # 지진 발생 시각
                kma_datetime = datetime.datetime.strptime(eqk_info[search_datetime], '%Y년 %m월 %d일 %H시 %M분 %S초')

                # 지진 최대 진도
                pattern = u'[{}]'.format(''.join(rome_numeral_numbers))
                regex = re.compile(pattern, re.UNICODE)
                kma_max_int = rome_numeral_numbers.index(regex.search(eqk_info[search_max_int]).group()) + 1

                # 고유번호 (진앙시 + 발표일련번호) ex) 201001010008
                uid = kma_datetime.strftime('%Y%m%d') + base_data.tm_seq.rjust(4, '0')
                regex = re.compile(r'\d*\.\d*')
                result = regex.findall(eqk_info[search_location_coord])

                # 지진 규모
                try:
                    kma_magnitude = float(re.findall(r'\d*\.\d*', eqk_info[search_magnitude])[0])
                except (ValueError, Exception):
                    kma_magnitude = 0

                # 위도/경도
                try:
                    latitude = float(result[0])
                    longitude = float(result[1])
                except (ValueError, Exception):
                    latitude = 0.0
                    longitude = 0.0

                # 지진 발생 위치
                regex = re.compile(r'[가-힝\d\w\s]*(?!\()')
                eqk_location = regex.search(eqk_info[search_location_coord]).group().strip()

                # 지진 발생 위치 이미지 url
                img_url = [i.kma_base_url + url['src'].split(';')[0]
                           for url in bs_detail_page.find_all('img', {'src': re.compile(r'\.*repositary\.*')})]
                img_name = [url.split('/')[-1] for url in img_url]
                coord = EqkCoordKma(longitude, latitude)

                # 지진 참고 사항
                note = eqk_info[search_remain]
                logger.info("상세 데이터 파싱 성공")
                return True, EqkDataKma(uid,
                                        kma_datetime,
                                        kma_datetime_ann,
                                        eqk_location,
                                        coord,
                                        kma_magnitude,
                                        kma_max_int,
                                        img_name,
                                        base_data.tp,
                                        img_url,
                                        city_max_int,
                                        note,
                                        depth)
            except Exception:
                time.sleep(min(120, 2 ** error_count))
                if error_count > 2:
                    logger.warning(f'상세데이터 파싱 중 알수 없는 이유로 파싱 실패. 시도횟수 : {error_count}')
                error_count += 1
                continue


def success_crawling_kma(data: EqkDataKma):
    """
    한국 기상청으로부터 크롤링해온 데이터를 처리하는 함수

    :param data: 한국 기상청으로부터의 지진 정보
    :return:
    """

    logger.info("새로운 데이터 저장 시작")
    logger.info("새로운 데이터의 uid 파일에 저장")
    # 새 데이터의 uid를 파일에 저장
    with open(setting.kma_setting.full_path, 'w') as f:
        f.write(data.uid)
    logger.info("새로운 데이터 S3에 저장")
    # S3에 번역된 데이터와 이미지를 저장
    saver = DataSaverListSaver(data, S3ImageSaverKma(data),
                               DataTranslateFileSaver(data, 'ko'),
                               DataTranslateFileSaver(data, 'en'),
                               DataTranslateFileSaver(data, 'ja'),
                               DataTranslateFileSaver(data, 'zh_Hans'),
                               DataTranslateFileSaver(data, 'zh_Hant')
                               )
    saver.save()
    logger.info("새로운 데이터 S3에 저장 종료")
    logger.info("새로운 데이터 알림 보내기 시작")
    # 푸쉬 알림 보내기
    notification.push_notify(push_data)
    logger.info("새로운 데이터 알림 보내기 종료")


if __name__ == "__main__":
    # 미리 번역된 단어들 불러오기
    with open('rules/translate.json', 'rb') as f:
        pre_translated_data = json.load(f)

    # setting 불러오기
    setting = GlobalSetting.create()

    if setting.kma_setting.sleep_time <= 0:
        sys.exit("Time value is zero or under zero.")

    # 각총 초기화
    aws_s3.init_aws_s3(setting)
    notification.notify_contents_init(setting)
    client = translate.TranslationServiceClient.from_service_account_json(setting.gcloud_secret_key)
    parent = client.location_path("jijin-alimi", "global")

    with open('rules/codes.json', 'rb') as f:
        code = json.load(f)

    # 로거 초기화
    os.makedirs(setting.log_path, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fmtter = logging.Formatter(fmt=u'[%(asctime)s] %(levelname)s: %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')

    log_hdlr = logging.handlers.RotatingFileHandler(filename=setting.kma_setting.log_full_path,
                                                    maxBytes=1024*1024, backupCount=1, encoding='utf8')  # 파일 저장 로그
    log_hdlr.setFormatter(fmtter)
    log_hdlr.setLevel(logging.INFO)
    logger.addHandler(log_hdlr)

    stream_hdlr = logging.StreamHandler()
    stream_hdlr.setFormatter(fmtter)  # 콘솔 출력 로그
    stream_hdlr.setLevel(logging.ERROR)
    logger.addHandler(stream_hdlr)

    mail_hdlr = MailgunLogHandler('한국 기상청 크롤러로부터 경고!!!!', setting.mailgun)
    mail_hdlr.setLevel(logging.WARNING)
    mail_hdlr.setFormatter(fmtter)
    logger.addHandler(mail_hdlr)

    # Print current setting value.
    print(f"KMA Scraper Service is running... Time value is {setting.kma_setting.sleep_time} second(s).")
    # 프로그램이 처음으로 실행됬는가? 파일이 없으면 처음 실행/ 있으면 재실행됨
    restarted = os.path.exists(setting.kma_setting.full_path)
    if not restarted:
        print(f"[{datetime.datetime.now()}] <KMA> 프로그램 처음 실행")
        logger.info('프로그램 처음 실행')
    else:
        print(f"[{datetime.datetime.now()}] <KMA> 프로그램 재실행")
        logger.info('프로그램 재실행')

    if not os.path.exists(setting.kma_setting.current_data_path):
        os.makedirs(setting.kma_setting.current_data_path)

    dt = setting.kma_setting.sleep_time - 0.1
    while True:
        t = math.fmod(dt, setting.kma_setting.sleep_time)
        time.sleep(setting.kma_setting.sleep_time - t)
        logger.info("크롤링 시작")
        start_time = time.time()
        success, cur_data = create_data()
        if success:
            if restarted:
                logger.info("재시작 루틴")
                restarted = False
                with open(setting.kma_setting.full_path, 'r') as uid_file:
                    prev_uid = uid_file.readline()
                    if prev_uid == cur_data.uid:
                        logger.info("재시작 새로운 데이터 없음 다음 단계로")
                        end_time = time.time()
                        dt = end_time - start_time
                        logger.info(f"크롤링 종료. 걸린시간 : {dt}")
                        continue
            logger.info("새로운 데이터 불러오기 성공")
            success_crawling_kma(cur_data)
        end_time = time.time()
        dt = end_time - start_time
        logger.info(f"크롤링 종료. 걸린시간 : {dt}")
