# coding=utf-8
import json
import requests


def main(sql):

    sql = {'sql': f'{sql}'}

    url = 'http://172.20.31.16:18080/rest/v2/query'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic cm9vdDpyb290'
    }

    response = requests.post(url, data=json.dumps(sql), headers=headers)

    # 检查请求是否成功
    if response.status_code == 200:
        # 解析响应的JSON内容
        response_data = response.json()
        print(json.dumps(dict(response_data), indent=4))
    else:
        print(
            'Failed to send POST request. Status code:',
            response.status_code,
        )


if __name__ == '__main__':
    main('show models')




