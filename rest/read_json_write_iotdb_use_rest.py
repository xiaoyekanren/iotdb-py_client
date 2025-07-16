# coding=utf-8
import json

import requests

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic cm9vdDpyb290',  # 使用 Base64 编码的用户名和密码
}
base_url = 'http://11.101.17.224:18080'
api = {
    'query': 'rest/v2/query',
    'nonQuery': 'rest/v2/nonQuery',
    'insertTablet': 'rest/v2/insertTablet',
    'insertRecords': 'rest/v2/insertRecords',
}


def format_json(text: str):
    # str =>> dict =>> str
    return json.dumps(
        json.loads(
            text
        ),
        indent=2
    )


def query(sql: str):
    url = base_url.rstrip('/') + '/' + str(api['query'])
    data = {
        'sql': str(sql)
    }
    return url, data


def noquery(sql: str):
    url = base_url.rstrip('/') + '/' + str(api['nonQuery'])
    data = {
        'sql': sql
    }
    return url, data


def insert_tablet():
    url = base_url.rstrip('/') + '/' + str(api['insertTablet'])
    data = {
        "timestamps": [1635232143960, 1635232153960],
        "measurements": ["s3", "s4"],
        "data_types": ["INT32", "BOOLEAN"],
        "values": [
            [65535, 65536],
            [1, False]
        ],
        "is_aligned": True,
        "device": "root.g_tablet.d1"
    }
    return url, data


def insert_records():
    url = base_url.rstrip('/') + '/' + str(api['insertRecords'])
    data = {
        "timestamps": [1635232113960, 1635232151960, 1635232143960, 1635232143960],
        "measurements_list": [
            ["s3", "s4"],
            ["s3", "s4"],
            ["s3", "s4"],
            ["s3", "s4"]
        ],
        "data_types_list": [
            ["INT32", "TEXT"],
            ["INT32", "TEXT"],
            ["INT32", "TEXT"],
            ["INT32", "TEXT"]
        ],
        "values_list": [
            [55555, "55555"],
            [66666, "66666"],
            [77777, "77777"],
            [88888, "88888"]
        ],
        "is_aligned": False,
        "devices": ["root.g_records.d1", "root.g_records.d2", "root.g_records.d3", "root.g_records.d4"]
    }
    return url, data


def re(url: str, headers: dict, data: dict, need_results=False):
    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(data)
    )

    if response.status_code == 200:
        if need_results:
            print('success.')
            print(format_json(response.text))
    else:
        print(response.status_code, response.reason)


def main(operate: str):
    # query
    if operate == 'query':
        url, data = query('show cluster')
        re(url, headers, data)

    # no query
    if operate == 'nonQuery':
        url, data = noquery('insert into root.noquery.d1(time,s1,s2,s3) values (10086,123,123.456,\'timecho\')')
        re(url, headers, data, need_results=False)
        url, data = query('select * from root.noquery.d1')
        re(url, headers, data)

    # insert records
    if operate == 'insertTablet':
        url, data = insert_tablet()
        re(url, headers, data)
        url, data = query('select * from root.g_tablet.d1')
        re(url, headers, data)

    # insert tablet
    if operate == 'insertRecords':
        url, data = insert_records()
        re(url, headers, data)
        url, data = query('select * from root.g_records.**')
        re(url, headers, data)


if __name__ == '__main__':
    for q in range(10000):
        for i in api.keys():
            main(i)
        if q % 10:
            print(q)

