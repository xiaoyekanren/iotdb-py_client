# coding=utf-8
from interface import MqttClient
import random
import json
import time
import threading
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from session.interface import TreeSessionClient


class Config:
    database = "root.mqtt"  # 数据库名称
    start_time = 1000  # 起始时间戳
    
    batch_size = 1000

    thread_num = 10  # 线程数量
    sensor_num = 1000  # 序列数量（从0开始编号，共1000个，编号0-999）
    loop = 10000  # 每个线程执行batch_size的次数


def init_client(server_info):
    return MqttClient(
        host=server_info['mqtt_host'],
        port=server_info['mqtt_port'],
        user=server_info['iotdb_user'],
        password=server_info['iotdb_password'],
    )


def gen_dataset(device: str, batch_size: int):
    measurements = [f"s_{i}" for i in range(Config.sensor_num)]
    dataset = []
    seed_str = f"{device}"
    random.seed(seed_str)
    for _ in range(batch_size):
        values = [float(f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}") for _ in range(Config.sensor_num)]
        payload = {
            "device": device,
            "measurements": measurements,
            "values": values
        }
        dataset.append(payload)
    return dataset


def write_tree_worker(server_info, device_name):
    client = init_client(server_info)
    topic = server_info['mqtt_topic']
    qos = server_info['qos']
    device = Config.database + f".{device_name}"
    start_time = Config.start_time
    print(f"[Thread {device_name}], generating dataset...")
    base_dataset: list = gen_dataset(device=device, batch_size=Config.batch_size)  # 只生成一次batch_size行数据（不含时间戳）
    
    for loop in range(Config.loop):
        once_dataset = base_dataset
        print(f"[Thread {device_name}] loop {loop+1}/{Config.loop}")
        for index, payload in enumerate(once_dataset):  # index, value
            payload["timestamp"] = start_time + index * 1000
            client.exec_write(json.dumps(payload), topic=topic, qos=qos)
        start_time += Config.batch_size * 1000


def write_tree(server_info):
    print('1. clear iotdb.')
    with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667,
                           password=tree_conn.get('iotdb_password')) as client:
        client.non_query("drop database root.**")

    print('2. start write to iotdb.')
    threads = []
    for i in range(Config.thread_num):
        device_name = f"d{i}"
        t = threading.Thread(target=write_tree_worker, args=(server_info, device_name))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print(
        f"hello world, " +
        str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
    )

    print('3. exec flush.')
    with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667,
                           password=tree_conn.get('iotdb_password')) as client:
        client.non_query("flush")  # 刷盘

        print('4. start count.')
        for loop in range(10 * 60):

            for i in range(Config.thread_num):
                device_name = f"d{i}"
                device = f"root.mqtt.{device_name}"
                sensors = ','.join([f"count(s_{j})" for j in range(Config.sensor_num)])
                sql = f'select {sensors} from {device}'
                query_result = client.query(sql)
                while query_result.has_next():
                    print(query_result.next())

            print(
                str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))) +
                str(loop + 1)
            )
            time.sleep(60)


if __name__ == '__main__':
    tree_conn = {
        'mqtt_host': '172.20.31.16',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'TimechoDB@2021',
        'mqtt_topic': 'root.mqtt.d1',  # 树模型，topic无意义
        'qos': 2,
    }
    write_tree(tree_conn)
