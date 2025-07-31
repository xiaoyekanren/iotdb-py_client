# coding=utf-8

from interface import RestClient


def init_client(conn_info: dict):
    conn = RestClient(
        conn_info.get('base_url'),
        conn_info.get('username'),
        conn_info.get('password'),
    )
    return conn


def query(conn: RestClient, sql, is_print=True):
    sql = 'select * from root.g1.**'
    results = conn.query(sql)

    if is_print:
        for i in results.keys():
            print(i, results[i], sep=': ')


def main():
    conn = init_client(server_info)

    query(conn, 'select * from root.g1.**', is_print=True)


    #
    conn.insert_tablet()

    #
    conn.insert_records()


if __name__ == '__main__':
    server_info = {
        'base_url': 'http://127.0.0.1:18080',
        'username': 'root',
        'password': 'root',
    }
    main()



