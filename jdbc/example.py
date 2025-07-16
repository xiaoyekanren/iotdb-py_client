# coding=utf-8
from interface import JdbcClient


def init_client(server_info):
    return JdbcClient(
        url=server_info['url'],
        user=server_info['user'],
        password=server_info['password'],
        jar_path=server_info['jar_path'],
        model=server_info['model'],
    )


def main(server_info: dict):
    client = init_client(server_info)

    sql = 'show databases'

    try:
        results = client.query(sql)
        for i in results:
            print(i)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


if __name__ == '__main__':
    server_info_dict = {
        'url': 'jdbc:iotdb://127.0.0.1:6667',
        'user': 'root',
        'password': 'root',
        'jar_path': 'libs/iotdb-jdbc-v2051-rc2.jar',
        'model': 'tree'
    }
    main(server_info_dict)


