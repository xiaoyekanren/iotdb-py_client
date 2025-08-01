# coding=utf-8

from iotdb.Session import Session as TreeSession
from iotdb.table_session import Session as TableSession


class TreeSessionClient():
    def __init__(self, ip: str, port: int, user: str = 'root', password: str = 'root'):
        self.ip = ip
        self.port = port
        self.user = user
        self.password = password
        self.client: TreeSession = None

    def init_client(self):
        self.client = TreeSession(self.ip, self.port, self.user, self.password)
        self.client.open(False)  # rpc压缩 = false

    def close(self):
        if self.client:
            self.client.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        self.init_client()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def non_query(self, sql: str):
        if not self.client:
            self.init_client()
        self.client.execute_non_query_statement(sql)

    def query(self, sql: str):
        if not self.client:
            self.init_client()
        return self.client.execute_query_statement(sql)








