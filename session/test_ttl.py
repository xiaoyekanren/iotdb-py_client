# coding=utf-8
from iotdb.table_session import TableSession, TableSessionConfig


def init_session(user_='root', password_='root', db_=None):
    return TableSession(
        TableSessionConfig(
            node_urls=[f'{ip}:{port_}'],
            username=user_,
            password=password_,
            database=db_,
            time_zone="UTC+8",
        )
    )


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


def exec_no_results():
    session = init_session()

    session.execute_non_query_statement('drop database if exists test')
    session.execute_non_query_statement('create database if not exists test with (ttl=86400000)')

    session = init_session(db_='test')
    session.execute_non_query_statement('create table t2(device_id string TAG, int32 INT32 FIELD) with (ttl=60000)')

    success = 0
    fail = 0

    for i in range(10000):
        if i % 100 == 0:
            print(f'cur: {i}')
        try:
            session.execute_non_query_statement("insert into t2(time, device_id, int32) values(now()-1m,'d1',2)")
            # print(f'insert success. {i}')
            success +=1
        except Exception:
            # print(f'insert fail. {i}')
            fail +=1
    session.close()

    print(
        f'success: {success}',
        f'fail: {fail}',
        sep='\n'
    )


if __name__ == '__main__':
    ip = '127.0.0.1'
    port_ = "6667"
    exec_no_results()





