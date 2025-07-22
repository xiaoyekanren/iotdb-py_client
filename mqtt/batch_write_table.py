# coding=utf-8
import time
from interface.mqtt import MqttClient


def init_client(server_info) -> MqttClient:
    return MqttClient(
        host=server_info['mqtt_host'],
        port=server_info['mqtt_port'],
        topic=server_info['mqtt_topic'],
        user=server_info['iotdb_user'],
        password=server_info['iotdb_password'],
    )


def generate_data(count, one_line_has_record):
    data_list = []
    data_buffer = []  # 缓存区
    for i in range(count):
        data = 't1,taga=abbc,tagb=45 f1=1i32,f2=9999999999999i,f3=3.1415f,f4=3.1415926,f5="123456",f6=t %s' % str(i+3600)
        data_buffer.append(data)
        if (i + 1) % one_line_has_record == 0:
            data_list.append('\n'.join(data_buffer))
            data_buffer = []
    if data_buffer:
        data_list.append('\n'.join(data_buffer))  # 最后剩下的 不能被整除的 部分
    return data_list


def table(conn_info: dict, count=1, one_line_has_record=1, once_client_write_rows=10000):
    """
    :param conn_info:
    :param count: 总的记录数，并非点数，而是行协议的行数
    :param one_line_has_record:  一行有几条记录，也就是一次 publish 几条记录
    :param once_client_write_rows: 开启一个客户端写的总行数
    :return:
    """
    client = init_client(conn_info)
    qos = conn_info['qos']

    print('Prepare Data. ')
    data_list = generate_data(count, one_line_has_record)

    print('Start write.')
    payload = [data_list[start:start + once_client_write_rows] for start in range(0, len(data_list), once_client_write_rows)]  # 拆分成多次写入，避免一次写入过多
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}, start write.')

    elapsed_time_ms = 0
    for i in payload:
        batch_start_time_ms = int(time.time() * 1000)
        client.exec_write(payload=i, qos=qos)
        batch_end_time_ms = int(time.time() * 1000)

        once_elapsed_time_ms = batch_end_time_ms - batch_start_time_ms

        elapsed_time_ms += once_elapsed_time_ms
        print(f'info: {len(i)} lines have been written, elapsed time {once_elapsed_time_ms}ms. ')

    print(f'\nTotal elapsed time is {elapsed_time_ms}ms.')


if __name__ == "__main__":
    table_conn = {
        'mqtt_host': '127.0.0.1',
        'mqtt_port': 1883,
        'mqtt_topic': 'testdb/timecho',  # 表模型，zzm为数据库
        'iotdb_user': 'root',
        'iotdb_password': 'root',
        'qos': 2,
    }
    table(table_conn, count=10000, one_line_has_record=100, once_client_write_rows=10)
