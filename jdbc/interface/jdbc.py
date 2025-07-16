# coding=utf-8
import jaydebeapi


class JdbcClient:
    def __init__(self, url=None, user=None, password=None, jar_path=None, model='tree'):  # 对象初始化方法
        self.driver_name = 'org.apache.iotdb.jdbc.IoTDBDriver'
        self.jar_path = jar_path
        self.password = password
        self.user = user
        self.url = url
        self.model = model
        self.conn = None
        self.cursor = None

    def connect(self):
        if not self.url or not self.user or not self.password or not self.jar_path:
            raise ValueError("url, user, password, and jar_path are required.")
        try:
            if not self.model or self.model == 'tree':
                self.url += '?sql_dialect=tree'
            elif self.model == 'table':
                self.url += '?sql_dialect=table'

            self.conn = jaydebeapi.connect(
                self.driver_name,
                self.url,
                [self.user, self.password],
                self.jar_path
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            self.conn = None
            self.cursor = None
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
        finally:
            if self.conn:
                self.conn.close()
        self.cursor = None
        self.conn = None

    def write(self, sql):
        if self.conn is None or self.cursor is None:
            self.connect()
        self.cursor.execute(sql)
        self.conn.commit()

    def query(self, sql) -> list:
        if self.conn is None or self.cursor is None:
            self.connect()
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def __del__(self):  # 当对象被垃圾回收时调用，用于释放资源
        self.close()

    def __enter__(self):  # 上下文管理器的入口方法
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # 上下文管理器的退出方法
        self.close()

