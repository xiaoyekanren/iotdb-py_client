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
    database = "root.mqtt"  # 数据库/设备路径
    start_time = 1000  # 起始时间戳

    batch_size = 100

    thread_num = 10  # 线程数量
    per_thread_sensors = 1000  # 每线程负责的传感器数量
    loop = 100000  # 每个线程执行batch_size的次数

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


def gen_dataset(device_str: str, batch_size: int, sensor_start: int, sensor_count: int):
    # 预序列化 measurements 为 JSON 数组字符串，values 每行也生成为 JSON 数组字符串
    measurements_list: list[str] = [f"s_{i}" for i in range(sensor_start, sensor_start + sensor_count)]
    measurements_list_str = json.dumps(measurements_list)
    dataset = []
    seed_str = f"{device_str}_{sensor_start}"
    random.seed(seed_str)
    for _ in range(batch_size):
        # 生成数值字符串（不带引号），以便直接拼接到最终 JSON 中，减少后续序列化成本
        values_str_list__ = [f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}" for _ in range(sensor_count)]
        values_str_list = ('[' +
                           ','.join(values_str_list__) +
                           ']')
        payload = {
            "measurements": measurements_list_str,
            "values": values_str_list
        }
        dataset.append(payload)
    return dataset


def write_tree_worker(server_info, device_name, sensor_start):
    client = init_client(server_info)
    topic = server_info['mqtt_topic']
    qos = server_info['qos']
    device = Config.database
    start_time = Config.start_time
    log.info(f"[Thread {device_name}], generating dataset...")
    base_dataset: list = gen_dataset(
        device_str=device,
        batch_size=Config.batch_size,
        sensor_start=sensor_start,
        sensor_count=Config.per_thread_sensors
    )  # 只生成一次batch_size行数据（不含时间戳）
    
    for loop in range(Config.loop):
        log.info(f"[Thread {device_name}] loop {loop+1}/{Config.loop}")
        for index, payload in enumerate(base_dataset):  # index, value
            timestamp_str = str(start_time + index * 1000)
            # 直接拼接最终 JSON 字符串，避免对整个 payload 调用 json.dumps
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
        sensor_start = i * Config.per_thread_sensors
        t = threading.Thread(target=write_tree_worker, args=(server_info, device_name, sensor_start))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    log.info('3. exec flush.')
    with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667,
                           password=tree_conn.get('iotdb_password')) as client:
        client.non_query("flush")  # 刷盘

        if Config.is_count_point:
            log.info('4. start count.')
            for loop in range(1):
                for i in range(Config.thread_num):
                    sensor_start = i * Config.per_thread_sensors
                    device = Config.database
                    sensors = ','.join([f"count(s_{sensor_start + j})" for j in range(Config.per_thread_sensors)])
                    sql = f'select {sensors} from {device}'
                    query_result = client.query(sql)
                    while query_result.has_next():
                        log.info(query_result.next())

                log.info(
                    'cur loop: ' +
                    str(loop + 1)
                )
                time.sleep(30)


if __name__ == '__main__':
    tree_conn = {
        'mqtt_host': '172.20.31.18',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'mqtt_topic': 'root.mqtt',  # 树模型，topic无意义
        'qos': 1,
    }
    write_tree(tree_conn)
