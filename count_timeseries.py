# coding=utf-8
from iotdb.Session import Session

ip = '11.101.17.223'
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=10000)

once_count_ts = 10


def join_sql(ts_list_):
    select_ = ''
    prefix_ = '.'.join(str(ts_list_[0]).split('.')[:-1])
    for ts in ts_list_:
        format_ts = str(ts).split('.')[-1]
        ts_ = f'count({format_ts})' + ','
        select_ += ts_
    select_ = select_.rstrip(',')
    sql = f'select {select_} from {prefix_}'
    return sql


def generate_sql_list(ts_list_):
    sql_list = []
    if len(ts_list_) < once_count_ts:
        return [join_sql(ts_list_)]
    else:
        index_ = 0
        while index_ < len(ts_list_):
            s_l = join_sql(ts_list_[index_:index_+once_count_ts])
            sql_list.append(s_l)
            index_ += once_count_ts
        return sql_list


def main():
    device_list = [i[0] for i in exec_has_results('show devices')]
    print(device_list)

    session.open(False)

    for device in device_list:
        sensor_list = [i[0] for i in exec_has_results(f'show timeseries {device}.**')]
        # print(sensor_list)
        sql_list: list = generate_sql_list(sensor_list)
        for i in sql_list:
            # print(i)
            print(
                '\t',
                exec_has_results(i)[0]
            )
    session.close()


def exec_has_results(sql):
    list_ = []
    session.open(False)
    if not sql:
        sql = 'show version'
    results = session.execute_query_statement(sql)

    while results.has_next():
        line_result = [
            str(i) for i in results.next().get_fields()
        ]
        list_.append(line_result)

    session.close()
    return list_


if __name__ == '__main__':
    main()


