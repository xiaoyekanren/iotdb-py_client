# coding=utf-8
import random
import time
import json

from interface import mqtt_write


def tree(conn_info: dict):
    data = {
        "device": "root.mqtt.d1",
        "measurements": ["s2", "s3"],
        "values": [
            random.randint(1000, 9999),
            random.randint(1000, 9999)
        ],  # 随机数
        "timestamp":
            int(time.time() * 1000)  # 毫秒时间戳
    }
    mqtt_write(conn_info, json.dumps(data), 'tree')


def table(conn_info: dict, count=1, one_line_has_record=1, once_client_write_rows=10000):
    """
    :param conn_info:
    :param count: 总的记录数，并非点数，而是行协议的行数
    :param one_line_has_record:  一行有几条记录，也就是一次 publish 几条记录
    :param once_client_write_rows: 开启一个客户端写的总行数
    :return:
    """
    print('Prepare Data. ')

    data_list = []
    data_buffer = []  # 缓存区
    for i in range(count):
        data = 't1,taga=abbc,tagb=45 f1=1i32,f2=9999999999999i,f3=3.1415f,f4=3.1415926,f5="123456",f6=t %s' % str(i+3600)
        data_buffer.append(data)

        if (i + 1) % one_line_has_record == 0:
            data_list.append('\n'.join(data_buffer))
            data_buffer = []

    if data_buffer:
        data_list.append('\n'.join(data_buffer))

    print('Start write.')
    mqtt_write(conn_info, data_list, model='table', once_client_write_rows=once_client_write_rows)


if __name__ == "__main__":
    table_conn = {
        'mqtt_host': '127.0.0.1',
        'mqtt_port': 1883,
        'mqtt_topic': 'testdb/timecho',  # 表模型，zzm为数据库
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'qos': 2,
    }
    tree_conn = {
        'mqtt_host': '127.0.0.1',
        'mqtt_port': 1883,
        'mqtt_topic': 'root.interface.d1',  # 树模型，
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'qos': 2,
    }
    table(table_conn, count=10000, one_line_has_record=100, once_client_write_rows=10)
    tree(tree_conn)
