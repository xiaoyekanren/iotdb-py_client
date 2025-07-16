# coding=utf-8
from iotdb.Session import Session

ip = '172.20.31.16'
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=10000)

time_partition_interval = 604800000
time_partition = 2791
delete_loop = 100000

step = 10000000  # 删除的时间间隔
loop = 10  # 执行循环的次数，


def main():
    ts_list = get_ts_list('show timeseries root.test.g_2.d_2.**')

    session.open(False)

    for i in range(loop):
        start_time = time_partition * time_partition_interval
        end_time = ((time_partition + 1) * time_partition_interval) - 1

        while start_time <= end_time:
            for ts in ts_list:
                sql = f'delete from {ts} where time > {start_time} and time <= {start_time + step}'
                print(f'{str(i)}: {sql}')
                session.execute_non_query_statement(sql)
            start_time += step

    session.close()


def get_ts_list(sql):
    list_ = []
    session.open(False)
    if not sql:
        sql = 'show timeseries root.test.g_2.d_2.**'
    results = session.execute_query_statement(sql)

    while results.has_next():
        line_result = results.next().get_fields()[0]
        # print(line_result)
        list_.append(str(line_result))

    session.close()
    return list_


if __name__ == '__main__':
    main()


