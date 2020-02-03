from string import printable
from datetime import datetime
import re


def string(data):
    while '  ' in data:
        data = data.replace('  ', ' ')
    data = re.sub(r'[\t\n\r\f]', '', data)
    return ''.join([s for s in data if
                    s in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя' or s in printable])


def int_num(data):
    if re.findall(r'[.,]\d', data) == []:
        return int(re.sub(r'\D', '', data))
    else:
        raise Exception('wrong format')


def float_num(data):
    if len(re.findall(r'[.,]\d', data)) < 2:
        return float(''.join((ch if ch in '0123456789.-e' else ' ') for ch in data.replace(',', '.')))
    else:
        raise Exception('wrong format')


def date(data):
    day = None
    month = None
    year = None
    if len(re.findall('\d+', data)) == 3:
        for i in sorted(list(map(int, re.findall('\d+', data))), reverse=True):
            if i >= 10000:
                raise Exception('wrong format')
            elif year is None and 0 <= datetime.now().year % 100 - i <= 2 or i >= 1000:
                year = i
            elif day is None:
                day = i
            else:
                month = i
        return '%s-%s-%s' % (day, month, year)
    else:
        raise Exception('wrong format')


def date_time(data):
    first = 'date'
    if ':' in data[:len(data) // 2]:
        first = 'time'
    data = list(map(int, re.findall(r'\d+', data)))
    for i in data[-3:]:
        if i > 1000:
            first = 'time'
    b = True
    for i in data[:3]:
        if 1000 > i > 31:
            first = 'time'
        if i > 1000 or 0 <= datetime.now().year % 100 - i <= 2:
            b = False
    if b:
        first = 'time'
    stDate = date(' '.join(list(map(str,data[:3]))) if first == 'date' else ' '.join(list(map(str,data[-3:]))) )
    if len(data) == 6:
        stTime = '%s:%s:%s' % tuple(data[3:] if first == 'date' else data[:-3])
    else:
        stTime = '%s:%s' % tuple(data[3:] if first == 'date' else data[:-3])
    return stDate + ' ' + stTime if first == 'date' else stTime + ' ' + stDate


def gln(data):
    if len(re.findall(r'\d', data)) == 13:
        return ''.join(re.findall(r'\d', data))
    else:
        raise Exception('wrong format')


def phone(data):
    try:
        return '%s-%s-%s-%s-%s' % tuple(re.findall(r'\d+', data))
    except:
        raise Exception('wrong format')


def dataCleaner(data, type_key):
    if type_key == 1:
        return string(data)
    elif type_key == 2:
        return int_num(data)
    elif type_key == 3:
        return float_num(data)
    elif type_key == 4:
        return date(data)
    elif type_key == 5:
        return date_time(data)
    elif type_key == 6:
        return gln(data)
    elif type_key == 7:
        return phone(data)

