# coding=utf-8
from interface import MqttClient
import random
import json


def init_client(server_info):
    return MqttClient(
        host=server_info['mqtt_host'],
        port=server_info['mqtt_port'],
        topic=server_info['mqtt_topic'],
        user=server_info['iotdb_user'],
        password=server_info['iotdb_password'],
    )


def write_tree(server_info):
    """
    定义好的 json
    """
    client = init_client(server_info)
    qos = server_info['qos']

    payload = {
        "device": "root.mqtt.d1",
        "measurements": ["s2", "s3"],
        "values": [
            random.randint(1000, 9999),
            random.randint(1000, 9999)
        ],  # 随机数
        "timestamp":
            1  # 毫秒时间戳
    }
    client.exec_write(json.dumps(payload), qos=qos)


def write_table(server_info):
    """
    行协议
    """
    client = init_client(server_info)
    qos = server_info['qos']

    payload = 't1,taga=abbc,tagb=45 f1=1i32,f2=9999999999999i,f3=3.1415f,f4=3.1415926,f5="123456",f6=t 1'
    client.exec_write(payload, qos=qos)


if __name__ == '__main__':
    table_conn = {
        'mqtt_host': '127.0.0.1',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'mqtt_topic': 'testdb/timecho',  # 表模型，topic的部分为数据库，即testdb
        'qos': 2,  # 默认 0
    }
    tree_conn = {
        'mqtt_host': '127.0.0.1',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'mqtt_topic': 'root.interface.d1',  # 树模型，topic无意义
        'qos': 2,
    }
    write_tree(tree_conn)
    # write_table(table_conn)


