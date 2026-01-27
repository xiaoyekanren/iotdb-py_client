# coding=utf-8
from mqtt.interface import MqttClient
import random
import json
import time
import threading
import sys
import os
import logging
from mqtt.interface import logging_conf
log = logging.getLogger(__name__)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from session.interface import TreeSessionClient


class Config:
    database = "root.mqtt"  # 数据库名称
    start_time = 1000  # 起始时间戳
    
    batch_size = 100

    thread_num = 10  # 线程/设备 数量）（1个线程管理一个设备）
    sensor_num = 100  # 序列数量（从0开始编号，共1000个，编号0-999）
    loop = 100  # 每个线程执行batch_size的次数

    is_clear_iotdb = True

    # 用于结束后进行点数统计，
    is_count_point = False
    count_loop = 100
    count_interval = 30


def init_client(server_info):
    return MqttClient(
        host=server_info['mqtt_host'],
        port=server_info['mqtt_port'],
        user=server_info['iotdb_user'],
        password=server_info['iotdb_password'],
    )


def gen_dataset(device_str: str, batch_size: int):
    measurements_list: list[str] = [f"s_{i}" for i in range(Config.sensor_num)]
    measurements_list_str = json.dumps(measurements_list)
    dataset = []
    seed_str = f"{device_str}"
    random.seed(seed_str)
    for _ in range(batch_size):
        values = [float(f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}") for _ in range(Config.sensor_num)]
        values_list_str__ = [f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}" for _ in range(Config.sensor_num)]
        values_list_str = ('[' +
                           ','.join(values_list_str__) +
                           ']')
        payload = {
            "measurements": measurements_list_str,
            "values": values_list_str
        }
        dataset.append(payload)
    return dataset


def write_tree_worker(server_info, device_name):
    client = init_client(server_info)
    topic = server_info['mqtt_topic']
    qos = server_info['qos']
    device = Config.database + f".{device_name}"
    start_time = Config.start_time
    log.info(f"[Thread {device_name}], generating dataset...")
    base_dataset: list[dict] = gen_dataset(device_str=device, batch_size=Config.batch_size)  # 只生成一次batch_size行数据（不含时间戳）
    
    for loop in range(Config.loop):
        log.info(f"[Thread {device_name}] loop {loop+1}/{Config.loop}")
        for index, payload in enumerate(base_dataset):  # index, value
            timestamp_str = str(start_time + index * 1000)
            msg = ('{' +
                   '"device":' + device + ',' +
                   '"measurements":' + payload["measurements"] + ',' +
                   '"values":' + payload["values"] + ',' +
                   '"timestamp":' + timestamp_str +
                   '}')
            client.exec_write(msg, topic=topic, qos=qos)
        start_time += Config.batch_size * 1000


def write_tree(server_info):
    if Config.is_clear_iotdb:
        log.info('1. clear iotdb.')
        with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667,
                               password=tree_conn.get('iotdb_password')) as client:
            try:
                client.non_query("drop database root.**")
            except Exception as a:
                log.warning(a)

    log.info('2. start write to iotdb.')
    threads = []
    for i in range(Config.thread_num):
        device_name = f"d{i}"
        t = threading.Thread(target=write_tree_worker, args=(server_info, device_name))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    log.info("finish write.")

    log.info('3. exec flush.')
    with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667,
                           password=tree_conn.get('iotdb_password')) as client:
        client.non_query("flush")  # 刷盘

        if Config.is_count_point:
            log.info('4. start count.')
            for loop in range(1):
                for i in range(Config.thread_num):
                    device_name = f"d{i}"
                    device = f"root.mqtt.{device_name}"
                    sensors = ','.join([f"count(s_{j})" for j in range(Config.sensor_num)])
                    sql = f'select {sensors} from {device}'
                    query_result = client.query(sql)
                    while query_result.has_next():
                        log.info(query_result.next())

                log.info('cur loop: ' + str(loop + 1))
                time.sleep(30)


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
