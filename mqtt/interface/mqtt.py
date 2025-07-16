# coding=utf-8
import random

import paho.mqtt.client as mqtt
import json
import time


class ConnectionState:
    allow_reconnect = True  # 全局连接状态控制


def parse_server_info(server_dict):
    host = server_dict['mqtt_host']
    port = server_dict['mqtt_port']
    topic = server_dict['mqtt_topic']
    user = server_dict['iotdb_user']
    password = server_dict['iotdb_password']
    qos = server_dict['qos'] if server_dict['qos'] else 0
    return host, port, topic, user, password, qos


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"on_connect：返回码：{rc}，连接成功.")
    else:
        if rc == 1:
            print(f"on_connect：返回码：{rc}，协议版本错误.")
        if rc == 2:
            print(f"on_connect：返回码：{rc}，无效的客户端标识.")
        if rc == 3:
            print(f"on_connect：返回码：{rc}，服务器无法使用.")
        if rc == 4:
            print(f"on_connect：返回码：{rc}，错误的用户名或密码")
        if rc == 5:
            print(f"on_connect：返回码：{rc}，未经授权")


def on_publish(client, userdata, mid):
    # print(f"on_publish: succeed, ID: {mid}")
    pass


def on_disconnect(client, userdata, rc):
    if ConnectionState.allow_reconnect:  # 判断是否允许重连
        print(f"on_disconnect：返回码：{rc}")
        while not client.is_connected() and ConnectionState.allow_reconnect:
            try:
                client.reconnect()
                time.sleep(3)
                print("Reconnected successfully.")
            except Exception as e:
                print(f"Reconnected failed. {e}")


def init_client(user: str, password: str, host: str, port: int, enable_random_client_id=False):

    if enable_random_client_id:
        client_id = str(random.randint(0, 9999))
    else:
        client_id = ""

    # 创建 MQTT 客户端
    client = mqtt.Client(
        protocol=mqtt.MQTTv31,
        client_id=client_id
    )

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.username_pw_set(user, password)
    client.connect(host, port, 60)

    client.loop_start()

    if enable_random_client_id:
        print(f'client_id is {client_id}')

    # 启动循环
    return client


def exec_write(server_dict: dict, payload):
    host, port, topic, user, password, qos = parse_server_info(server_dict)

    client = init_client(user, password, host, port)

    if isinstance(payload, str):
        payload = [payload]

    print(payload)

    try:
        while not client.is_connected():
            time.sleep(0.1)

        start_time_ms = int(time.time() * 1000)
        total_time_ms = 0

        if isinstance(payload, list):  # 判断是否为list
            results = [client.publish(topic, i, qos) for i in payload]  # 将结果丢到list里
            for result in results:  # 等待释放
                result.wait_for_publish(timeout=1)
                if result.is_published():
                    # print(f"已发布数据，rc：{result.rc}，mid：{result.mid}")
                    pass

        total_time_ms = int(time.time() * 1000) - start_time_ms

        if qos == 0:
            time.sleep(3)  # 避免丢数据（其实稳定丢）

    except Exception as e:
        print(f"发布数据失败: {e}")

    finally:  # 断开连接
        print('finally：bye,bye.')
        ConnectionState.allow_reconnect = False
        print('next, client.disconnect().')
        client.disconnect()

        print('next, client.loop_stop().')
        client.loop_stop()

        return total_time_ms


def prepare_write(server_dict: dict, data: str or list, model='tree', once_client_write_rows=10000):
    elapsed_time_ms = 0

    if model == 'tree' and isinstance(data, str):  # 树模型，且数据是 str(json)
        elapsed_time_ms = exec_write(server_dict, data)

    elif model == 'table' and isinstance(data, list):
        payload = [data[start:start + once_client_write_rows] for start in range(0, len(data), once_client_write_rows)]

        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}, start write.')
        for i in payload:
            once_elapsed_time_ms = exec_write(server_dict, i)
            print(f'info: {len(i)} lines have been written, elapsed time {once_elapsed_time_ms}ms. ')
            elapsed_time_ms += once_elapsed_time_ms
            time.sleep(1)

    print(f'\nTotal elapsed time is {elapsed_time_ms}ms.')

