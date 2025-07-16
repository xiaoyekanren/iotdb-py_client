# coding=utf-8
from iotdb.table_session import TableSession, TableSessionConfig


ip = '127.0.0.1'
port_ = "6667"
username_ = "root"
password_ = "root"
database = 'timecho'


config = TableSessionConfig(
    node_urls=[f'{ip}:{port_}'],
    username=username_,
    password=password_,
    database=database,
    time_zone="UTC+8",
)

session = TableSession(config)

def exec_has_results(sql):
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
    session.execute_non_query_statement('create database if not exists timecho')
    session.execute_non_query_statement('create table if not exists t1(tag1 tag,s1 int32)')

    for i in range(1025):
        sql = f'insert into timecho.t1(time, tag1, s1) values (%s,\'%s\',%s)' % (i, 'd_' + str(i), i)
        print(sql)
        session.execute_non_query_statement(sql)
    session.close()


if __name__ == '__main__':
    exec_no_results()

    sql_list = [
        'select first(s1) from t1 group by s1',
        'select last(s1) from t1 group by s1',
        'select sum(s1) from t1 group by s1',
        'select extreme(s1) from t1 group by s1'
    ]

    for sql in sql_list:
        try:
            exec_has_results(sql)
        except Exception as e:
            print(e)



