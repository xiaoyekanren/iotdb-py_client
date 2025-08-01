# coding=utf-8

from interface import RestClient


def init_client(conn_info: dict):
    conn = RestClient(
        conn_info.get('base_url'),
        conn_info.get('username'),
        conn_info.get('password'),
    )
    return conn


def query(conn: RestClient):
    sql = 'select * from root.g1.**;'
    results: dict = conn.query(sql)

    print(results)


def non_query(conn: RestClient):
    sql = 'insert into root.g1.d1(time, s1, s2) values (1, 123456, \'abcdefg\')'
    results = conn.non_query(sql)


def insert_tablet(conn: RestClient):  # 列写入
    dataset = {
        "measurements": ["s3", "s4"],
        "data_types": ["INT32", "BOOLEAN"],
        "timestamps": [1635232143960, 1635232153960],
        "values": [
            [65535, 65536],  # s3
            [1, False]  # s4
        ],
        "is_aligned": False,
        "device": "root.g1.d1"
    }
    conn.insert_tablet(dataset)


def insert_records(conn: RestClient):  # 行写入
    dataset = {
        "timestamps": [1635232113960, 1635232151960, 1635232143960, 1635232143960],
        "measurements_list": [
            ["s5", "s6"],
            ["s5", "s6"],
            ["s5", "s6"],
            ["s5", "s6"]
        ],
        "data_types_list": [
            ["INT32", "TEXT"],
            ["INT32", "TEXT"],
            ["INT32", "TEXT"],
            ["INT32", "TEXT"]
        ],
        "values_list": [
            [55555, "55555"],
            [66666, "66666"],
            [77777, "77777"],
            [88888, "88888"]
        ],
        "is_aligned": False,
        "devices": ["root.g1.d1", "root.g1.d1", "root.g1.d1", "root.g1.d1"]
    }
    conn.insert_records(dataset)


def main():
    conn = init_client(server_info)

    non_query(conn)
    insert_tablet(conn)
    insert_records(conn)
    query(conn)


if __name__ == '__main__':
    server_info = {
        'base_url': 'http://127.0.0.1:18080',
        'username': 'root',
        'password': 'root',
    }
    main()

