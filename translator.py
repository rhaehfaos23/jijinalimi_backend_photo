from enum import Enum
from datetime import datetime

import sys
import locale


class SupportedLanguages(str, Enum):
    ko = "ko"
    ja = "ja"
    en = "en"
    zh_Hans = "zh_Hans"
    zh_Hant = "zh_Hant"


def translate_location(data, lang):
    # data = {"wide": "위치 1", "city": "위치 2", "direction": "방향", "distance": 거리(숫자)

    if lang is SupportedLanguages.ko:
        result = "%s %s %s %skm" % (data['wide'], data['city'], data['direction'],
                                    data['distance'],)
    elif lang is SupportedLanguages.ja:
        result = "%s%sから%s%skm" % (data['wide'], data['city'], data['direction'],
                                   data['distance'],)
    elif lang is SupportedLanguages.zh_Hans:
        result = "%s%s以%s%s公里" % (data['wide'], data['city'], data['direction'],
                                  data['distance'],)
    elif lang is SupportedLanguages.zh_Hant:
        result = "%s%s以%s%s公里" % (data['wide'], data['city'], data['direction'],
                                  data['distance'],)
    else:
        result = "%skm %s of %s, %s" % (data['distance'], data['direction'], data['city'], data['wide'],)

    return result


def translate_datetime(data, lang):
    # data = '2020-03-04 21:25:15'
    datetime_data = datetime.strptime(data, "%Y-%m-%d %H:%M:%S")
    current_locale = locale.getlocale()

    if lang is SupportedLanguages.ko:
        if sys.platform.find('win') != -1:
            locale.setlocale(locale.LC_ALL, "ko_kr")
        else:
            locale.setlocale(locale.LC_ALL, "ko_KR.UTF-8")
        result = datetime_data.strftime("%Y년 %m월 %d일 %p %I시 %M분")
    elif lang is SupportedLanguages.ja:
        if sys.platform.find('win') != -1:
            locale.setlocale(locale.LC_ALL, "ja_jp")
        else:
            locale.setlocale(locale.LC_ALL, "ja_JP.UTF-8")
        result = datetime_data.strftime("%Y年%m月%d日%p%I時%M分")
    elif lang is SupportedLanguages.zh_Hans:
        if sys.platform.find('win') != -1:
            locale.setlocale(locale.LC_ALL, "zh_cn")
        else:
            locale.setlocale(locale.LC_ALL, "zh_CN.UTF-8")
        result = datetime_data.strftime("%Y年%m月%d日%p%I点%M分")
    elif lang is SupportedLanguages.zh_Hant:
        if sys.platform.find('win') != -1:
            locale.setlocale(locale.LC_ALL, "zh_tw")
        else:
            locale.setlocale(locale.LC_ALL, "zh_TW.UTF-8")
        result = datetime_data.strftime("%Y年%m月%d日%p%I點%M分")
    else:
        if sys.platform.find('win') != -1:
            locale.setlocale(locale.LC_ALL, "en_us")
        else:
            locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
        result = datetime_data.strftime("%Y-%m-%d %H:%M")

    locale.setlocale(locale.LC_ALL, current_locale)

    return result
