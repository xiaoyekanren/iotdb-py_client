# coding=utf-8
"""
用于：测试iotdb-表模型，转授user
时间：2025-04-07
iotdb版本：v2021-rc7
"""

from iotdb.table_session import TableSession, TableSessionConfig


def init_session(user='root', password='root'):
    username_ = user
    password_ = password

    config = TableSessionConfig(
        node_urls=[f'{ip}:{port_}'],
        username=username_,
        password=password_,
        database=None,
        time_zone="UTC+8",
    )
    session = TableSession(config)

    return session


def exec_has_results(sql):
    session = init_session()

    results = session.execute_query_statement(sql)
    while results.has_next():
        result = results.next()
        # print(result)
        ts = result.get_timestamp()

        results_list = []
        for i in result.get_fields():
            results_list.append(
                i.get_data_type()
            )

        print(f'ts={ts}', f'results={results_list}')

    session.close()


def exec_no_results(count_create_user):
    session = init_session("root", "root")
    session.execute_non_query_statement('create user user_1 \'1234\'')
    session.execute_non_query_statement('grant all to user user_1 with grant option')

    for i in range(2, count_create_user):
        print(i)
        last_user = f'user_{i - 1}'
        cur_user = f'user_{i}'

        current_session = init_session(last_user, "1234")

        session.execute_non_query_statement(f'create user {cur_user} \'1234\'')
        session.execute_non_query_statement(f'grant all to user {cur_user} with grant option')

        current_session.close()

    session.close()


if __name__ == '__main__':
    ip = '127.0.0.1'
    port_ = "6667"
    exec_no_results(count_create_user=10000)





