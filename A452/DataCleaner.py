from string import printable
import re

def string(data):
    while '  ' in data:
        data = data.replace('  ', ' ')
    data = re.sub('\s','',data)
    return ''.join([s for s in data if s in 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя' or s in printable])


def int_num(data):
    newstr = ''.join((ch if ch in '0123456789.-e' else ' ') for ch in data.replace(',', '.'))
    arr = []
    for i in newstr.split():
        try:
            if '.' not in i:
                arr.append(int(i))
            elif i[0] == '.':
                arr.append(int(i[1:]))
        except:
            pass
    return arr


def float_num(data):
    newstr = ''.join((ch if ch in '0123456789.-e' else ' ') for ch in data.replace(',', '.'))
    arr = []
    for i in newstr.split():
        try:
            if i[0]!= '.' and '.' in i:
                arr.append(float(i))
        except:
            pass
    return arr


def date(data):
    return re.findall('\d{4}[-/.]\d{2}[-/.]\d{2}', data)


def datetime(data):
    while '  ' in data:
        data = data.replace('  ', ' ')
    return re.findall('\d{4}[-/.]\d{2}[-/.]\d{2}\s\d{2}:\d{2}', data)


def gln(data):
    return re.findall('\d{13}',data)


def phone(data):
    return re.findall('\+?\d[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}',data)


def dataCleaner(data, type_key):
    types = {
        1: string(data),
        2: int_num(data),
        3: float_num(data),
        4: date(data),
        5: datetime(data),
        6: gln(data),
        7: phone(data)
    }

    return types.get(type_key, lambda: "wrong key")

