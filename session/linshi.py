# coding=utf-8
from iotdb.Session import Session


ip = '127.0.0.1'
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=10000)


def exec_has_results(sql):
    session.open(False)
    if not sql:
        sql = 'show version'
    results = session.execute_query_statement(sql)

    while results.has_next():
        rs = results.next()

        rs_ = rs.get_fields()[0]

        print(rs_)

        print(rs_.get_binary_value())

    session.close()


if __name__ == '__main__':
    exec_has_results(
        'select * from root.g1.d1'
    )

