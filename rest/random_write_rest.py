# coding=utf-8
import time

import numpy.random
import requests
import json
import pandas


def main(num):
    # API的URL
    url = 'http://127.0.0.1:18080/rest/v2/insertRecords'

    sensor_num = 10
    devices_num = 6

    # 要发送的数据
    data = {
        "timestamps": [i for i in range(devices_num)],
        "measurements_list": [
            ['s' + str(j) for j in range(sensor_num)] for __ in range(devices_num)
        ],
        "data_types_list": [
            ['INT32' for ___ in range(sensor_num)] for __ in range(devices_num)
        ],
        "values_list": [
            [num for ___ in range(sensor_num)] for __ in range(devices_num)
        ],
        "is_aligned": "true",
        "devices": [f"root.sg1.d{i}" for i in range(devices_num)]
    }
    # for i in data.keys():
    #     print(
    #         f'{i},{data[i]}'
    #     )
    # print()

    json_data = json.dumps(data)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic cm9vdDpyb290'
    }

    response = requests.post(url, data=json_data, headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
        # 解析响应的JSON内容
        response_data = response.json()
        print(response_data)
    else:
        print(
            'Failed to send POST request. Status code:',
            response.status_code,
        )


def main2():
    # API的URL
    url = 'http://127.0.0.1:18080/rest/v2/insertRecords'

    # 要发送的数据
    data = {
        "timestamps": [1, 2, 3, 4, 5],
        "measurements_list": [
            ['s' + str(i) for i in range(5)],
            # ['s1', 's2', 's3', 's4', 's5'],
            # ['s1', 's2', 's3', 's4', 's5'],
            # ['s1', 's2', 's3', 's4', 's5'],
            # ['s1', 's2', 's3', 's4', 's5'],
        ],
        "data_types_list": [
            ['INT32' for _ in range(5)],
            # ['INT32', 'INT32', 'INT32', 'INT32', 'INT32'],
            # ['INT32', 'INT32', 'INT32', 'INT32', 'INT32'],
            # ['INT32', 'INT32', 'INT32', 'INT32', 'INT32'],
            # ['INT32', 'INT32', 'INT32', 'INT32', 'INT32'],
        ],
        "values_list": [
            [1.5 for _ in range(5)],
            # [1, 2, 3, 4, 5],
            # [1, 2, 3, 4, 5],
            # [1, 2, 3, 4, 5],
            # [1, 2, 3, 4, 5],
        ],
        "is_aligned": "true",
        "devices": [
            'root.sg1.d1',
            # 'root.sg1.d2',
            # 'root.sg1.d3',
            # 'root.sg1.d4',
            # 'root.sg1.d5'
        ],
    }

    json_data = json.dumps(data)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic cm9vdDpyb290'
    }

    response = requests.post(url, data=json_data, headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
        response_data = response.json()  # 解析响应的JSON内容
        print(response_data)
    else:
        print(f'Failed to send POST request. Status code: {response.status_code}')


if __name__ == '__main__':
    # while True:
    #     abc = numpy.random.choice(['1', '1.1'])
    #     print(abc)
    #     try:
    #         main(abc)
    #     except Exception as e:
    #         pass
    #     time.sleep(.1)
    main2()


