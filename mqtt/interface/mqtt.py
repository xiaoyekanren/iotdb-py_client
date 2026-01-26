# coding=utf-8
import random

import paho.mqtt.client as mqtt
import json
import time
import logging

from . import logging_conf  # noqa: F401
log = logging.getLogger(__name__)


class MqttClient:
    def __init__(self, host=None, port=None, user=None, password=None, enable_random_client_id=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.enable_random_client_id = enable_random_client_id
        self.allow_reconnect = True  # 是否可以重连
        self.client: mqtt.Client = None  #
        self.max_retry = 10

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info(f"on_connect：返回码：{rc}，连接成功.")
        else:
            if rc == 1:
                log.error(f"on_connect：返回码：{rc}，协议版本错误.")
            if rc == 2:
                log.error(f"on_connect：返回码：{rc}，无效的客户端标识.")
            if rc == 3:
                log.error(f"on_connect：返回码：{rc}，服务器无法使用.")
            if rc == 4:
                log.error(f"on_connect：返回码：{rc}，错误的用户名或密码")
            if rc == 5:
                log.error(f"on_connect：返回码：{rc}，未经授权")

    def on_publish(self, client, userdata, mid):
        # log.info(f"on_publish: succeed, ID: {mid}")
        pass

    def on_disconnect(self, client, userdata, rc):
        if self.allow_reconnect:  # 判断是否允许重连
            log.warning(f"on_disconnect：返回码：{rc}")
            while not self.client.is_connected() and self.allow_reconnect:
                try:
                    self.client.reconnect()
                    time.sleep(3)
                    log.info("Reconnected successfully.")
                except Exception as e:
                    log.info(f"Reconnected failed. {e}")

    def init_client(self):
        if self.enable_random_client_id:
            client_id = str(random.randint(0, 99999999))
        else:
            client_id = ""

        # 创建 MQTT 客户端
        client = mqtt.Client(
            protocol=mqtt.MQTTv311,
            client_id=client_id
        )
        client.on_connect = self.on_connect
        client.on_publish = self.on_publish
        client.on_disconnect = self.on_disconnect

        client.username_pw_set(self.user, self.password)
        client.connect(self.host, self.port, 60)
        client.loop_start()

        if self.enable_random_client_id:
            log.info(f'client_id is {client_id}')

        self.client = client

    def exec_write(self, payload: str | list, topic: str, qos: int = 0):
        """
        payload: str -> json, list -> user define
        """
        if not self.client:
            self.init_client()
        try:
            while not self.client.is_connected():
                time.sleep(1)
                log.warning('mqtt is disconnected. wait reconnect.')

            result = self.client.publish(topic, payload, qos)

            # plan a
            result.wait_for_publish(timeout=1)  # plan-b，注释掉该行
            if result.is_published():
                # log.info(f"已发布数据，rc：{result.rc}，mid：{result.mid}")
                pass

            # plan b， 即发布失败时报错
            # hasattr(result, 'rc'): 检查 result 对象是否包含 rc 这个属性。
            # mqtt.MQTT_ERR_SUCCESS: （通常值为 0），代表 “MQTT 操作成功”；
            # try:
            #     if hasattr(result, 'rc') and result.rc != mqtt.MQTT_ERR_SUCCESS:
            #         log.warning(f"publish returned rc: {result.rc}")
            # except Exception:
            #     print("无法获取发布结果 rc。")

        except Exception as e:
            log.error(f"发布数据失败: {e}")

    def close(self):
        if self.client:
            try:
                log.info('关闭MQTT连接...')
                self.allow_reconnect = False
                # 先停止网络循环
                self.client.loop_stop()
                # 断开连接
                self.client.disconnect()
            except Exception as e:
                log.error(f"关闭MQTT客户端时出错: {e}")
            finally:
                self.client = None

    def __del__(self):  # 当对象被垃圾回收时调用，用于释放资源
        self.close()

    def __enter__(self):  # 上下文管理器的入口方法
        self.init_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # 上下文管理器的退出方法
        self.close()




