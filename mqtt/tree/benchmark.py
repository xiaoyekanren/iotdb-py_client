# coding=utf-8
import random
import json
import time
import threading
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from session.interface import TreeSessionClient
from mqtt.interface import MqttClient
from mqtt.interface import logging_conf
log = logging.getLogger(__name__)


class Config:
    is_clear_iotdb = True

    database = "root.mqtt"  # 数据库名称
    start_time = 1000  # 起始时间戳

    is_one_device_write = False  # False: 多设备多序列写入，True: 单设备多序列写入

    thread_num = 5  # 线程数量
    # 序列数量：
    #   - is_one_device_write=False时，表示每个设备的序列数量（多设备模式：一共有thread_num个设备，每个设备有sensor_num个序列）
    #   - is_one_device_write=True时，表示每个线程负责的序列数量（单设备模式：只有1个设备，每个线程负责sensor_num个序列）
    sensor_num = 100
    batch_size = 1000
    loop = 10000  # 每个线程执行batch_size的次数

    device_name_prefix = 'd'  # 无需分隔符，自动_

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


def gen_dataset(device_str: str, batch_size: int, sensor_start: int = 0, sensor_count: int = None):
    if sensor_count is None:
        sensor_count = Config.sensor_num
    measurements_list: list[str] = [f"s_{i}" for i in range(sensor_start, sensor_start + sensor_count)]
    measurements_list_str = json.dumps(measurements_list)
    dataset = []
    seed_str = f"{device_str}_{sensor_start}" if Config.is_one_device_write else f"{device_str}"
    random.seed(seed_str)
    for _ in range(batch_size):
        values_list_str__ = [f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}" for _ in range(sensor_count)]
        values_list_str = ('[' +
                           ','.join(values_list_str__) +
                           ']')
        payload = {
            "measurements": measurements_list_str,
            "values": values_list_str
        }
        dataset.append(payload)
    return dataset


def write_tree_worker(server_info, device_name, sensor_start=0):
    client = init_client(server_info)
    topic = server_info['mqtt_topic']
    qos = server_info['qos']

    if Config.is_one_device_write:
        # 单设备多序列写入模式：所有线程共享同一个设备
        device = Config.database + "." + Config.device_name_prefix
    else:
        # 多设备多序列写入模式：每个线程管理一个独立设备
        device = Config.database + f".{device_name}"
    sensor_count = Config.sensor_num

    start_time = Config.start_time
    log.info(f"[Thread {device_name}], generating dataset...")
    base_dataset: list[dict] = gen_dataset(
        device_str=device,
        batch_size=Config.batch_size,
        sensor_start=sensor_start,
        sensor_count=sensor_count
    )  # 只生成一次batch_size行数据（不含时间戳）

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
        try:
            with TreeSessionClient(ip=tree_conn.get('mqtt_host'), port=6667, password=tree_conn.get('iotdb_password')) as session_client:
                session_client.non_query("drop database root.**")
        except Exception as e:
            log.error(e)
            exit()

    log.info('2. start write to iotdb.')
    threads = []
    for i in range(Config.thread_num):
        device_name = f"{Config.device_name_prefix}_{i}"
        if Config.is_one_device_write:
            # 单设备多序列写入：每个线程负责不同的传感器范围
            sensor_start = i * Config.sensor_num
        else:
            # 多设备多序列写入：每个设备从0开始
            sensor_start = 0
        t = threading.Thread(target=write_tree_worker, args=(server_info, device_name, sensor_start))
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
                if Config.is_one_device_write:
                    # 单设备多序列写入：查询不同范围的传感器
                    for i in range(Config.thread_num):
                        sensor_start = i * Config.sensor_num
                        device = Config.database
                        sensors = ','.join([f"count(s_{sensor_start + j})" for j in range(Config.sensor_num)])
                        sql = f'select {sensors} from {device}'
                        query_result = client.query(sql)
                        while query_result.has_next():
                            log.info(query_result.next())
                else:
                    # 多设备多序列写入：查询每个设备的所有传感器
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
        'mqtt_host': '11.101.17.121',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'mqtt_topic': 'root.mqtt.d1',  # 树模型，topic无意义
        'qos': 2,
    }
    write_tree(tree_conn)
