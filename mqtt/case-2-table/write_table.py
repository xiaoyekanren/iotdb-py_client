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
from iotdb.table_session import TableSession, TableSessionConfig


class Config:
    database = "root.mqtt"  # 数据库名称
    start_time = 1000  # 起始时间戳
    
    batch_size = 10

    thread_num = 10  # 线程数量 & 设备数量

    sensor_num = 10  # 序列数量（从0开始编号，共1000个，编号0-999）
    loop = 5  # 每个线程执行batch_size的次数


def init_session(server_info) -> TableSession:
    ip = server_info['mqtt_host']
    port = 6667
    username = server_info['iotdb_user']
    password = server_info['iotdb_password']
    database = server_info.get('mqtt_topic').split('/')[0]

    config = TableSessionConfig(
        node_urls=[f'{ip}:{port}'],
        username=username,
        password=password,
        database=database,
        time_zone="UTC+8",
    )
    session = TableSession(config)
    return session


def init_client(server_info):
    return MqttClient(
        host=server_info['mqtt_host'],
        port=server_info['mqtt_port'],
        user=server_info['iotdb_user'],
        password=server_info['iotdb_password'],
    )


def gen_dataset(device: str, batch_size: int, start_time):
    sensor_key = [f"s{i}" for i in range(Config.sensor_num)]
    dataset = []
    if start_time is None:
        start_time = Config.start_time
    for i in range(batch_size):
        timestamp = start_time + i * 1000

        # gener random num
        sensor_value = []
        for sensor in range(Config.sensor_num):
            random.seed(f'{device}{sensor}{timestamp}')
            # sensor_ = float(f"{random.randint(100000, 999999)}.{random.randint(0, 99999):05d}")
            sensor_ = f"{random.randint(100000, 999999)}i"
            sensor_value.append(sensor_)
        part_sensor = ','.join(
            [f'{sensor_key[i]}={sensor_value[i]}' for i in range(Config.sensor_num)]
        )

        payload = f't1,taga={device} {part_sensor} {timestamp}'
        dataset.append(payload)
    return dataset


def write_table_worker(server_info, device_name):
    client = init_client(server_info)
    topic = server_info['mqtt_topic']
    qos = server_info['qos']
    start_time = Config.start_time
    for loop_idx in range(Config.loop):
        print(f"[Thread {device_name}] loop {loop_idx+1}/{Config.loop}")
        dataset = gen_dataset(device=device_name, batch_size=Config.batch_size, start_time=start_time)
        for payload in dataset:
            client.exec_write(payload, topic=topic, qos=qos)
        start_time += Config.batch_size * 1000


def write_table(server_info):
    database = server_info.get('mqtt_topic').split('/')[0]

    session = init_session(server_info)


    print('1. clear iotdb.')
    session.execute_non_query_statement(f"drop database if exists {database}")
    session.execute_non_query_statement(f'create database if not exists {database}')
    session.close()

    print('2. start write to iotdb.')
    threads = []
    for i in range(Config.thread_num):
        device_name = f"d_{i}"
        t = threading.Thread(target=write_table_worker, args=(server_info, device_name))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print(
        f"hello world, " +
        str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
    )

    session = init_session(server_info)
    print('3. exec flush.')
    session.execute_non_query_statement('flush')

    print('4. start count.')
    for loop in range(10 * 60):

        for i in range(Config.thread_num):
            device_name = f"d{i}"
            sensors = ','.join([f"count(s_{j})" for j in range(Config.sensor_num)])
            sql = f'select count(*) from t1'
            query_result = session.execute_query_statement(sql)

            while query_result.has_next():
                print(query_result.next())

        print(
            str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))) +
            str(loop + 1)
        )
        time.sleep(60)
    session.close()


if __name__ == '__main__':
    table_conn = {
        'mqtt_host': '172.20.31.16',
        'mqtt_port': 1883,
        'iotdb_user': 'root',
        'iotdb_password': 'TimechoDB@2021',
        'mqtt_topic': 'testdb/xxx',  # 表模型，topic的部分为数据库，即testdb
        'qos': 2,  # 默认 0
    }
    # 注意，当前实现问题，一个线程一次只能写一行，最好是用 batch_write_table.py
    write_table(table_conn)

