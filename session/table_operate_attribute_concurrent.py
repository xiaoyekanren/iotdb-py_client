# coding=utf-8
import time

from iotdb.table_session import TableSession, TableSessionConfig
from concurrent.futures import ThreadPoolExecutor
# import threading


def get_session():
    config = TableSessionConfig(
        node_urls=['127.0.0.1:6667'],
        username='root',
        password='root',
        database=None,
        time_zone="UTC+8",
    )
    return TableSession(config)


def init_db():
    print('init db.')
    session = get_session()
    try:
        session.execute_non_query_statement('drop database if exists test')  # 删库
        session.execute_non_query_statement('create database if not exists test')  # 建库
        session.execute_non_query_statement(
            'create table test.table0(device_id string TAG, region_id string ATTRIBUTE, s_1 INT32 FIELD)')  # 建表
    finally:
        session.close()


def insert_device_data(device_id, point_per_device, batch_size):
    session = get_session()

    device = f"d_{device_id}"
    attr = f"attr_{device_id}"

    try:
        # 分批次插入（0-9999分100批，每批100条）
        for batch_start in range(0, point_per_device, batch_size):
            values = []
            # 生成当前批次的时间点数据
            for time_ in range(batch_start, batch_start + batch_size):
                values.append(f"({time_}, '{device}', '{attr}', {time_})")

            sql = f"INSERT INTO test.table0(time, device_id, region_id, s_1) VALUES {','.join(values)}"

            session.execute_non_query_statement(sql)
            # print(f"Device {device} batch {batch_start // batch_size} inserted")

    except Exception as e:
        print(f"Error in device {device}: {str(e)}")
    finally:
        session.close()


def concurrent_insert(device_num, point_per_device, batch_size, threads):
    print('concurrent insert.')
    start_time = time.time()

    # 使用线程池控制并发数（根据实际情况调整）
    with ThreadPoolExecutor(max_workers=threads) as executor:  # 同时处理20个设备
        # 提交100个设备的插入任务
        futures = [executor.submit(insert_device_data, device_id, point_per_device, batch_size) for device_id in range(device_num)]

        # 等待所有任务完成（可选）
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Task error: {str(e)}")

    end_time = time.time()
    print(f'elapsed {(end_time - start_time):.2f}s')


if __name__ == '__main__':
    init_db()

    concurrent_insert(
        device_num=100,
        point_per_device=10000,
        batch_size=100,
        threads=20,
    )



