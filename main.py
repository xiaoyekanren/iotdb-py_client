# coding=utf-8
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
#
from insert_record import insert_records
from insert_record import insert_record
from insert_tablet import insert_tablet
from insert_tablet import insert_tablets

ip = "172.20.31.16"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
data_file = "param0.json"


def main():
    print('start insert_record')
    insert_record(session, data_file)
    # # example:
    # device_ids_ = "root.sg_test_01.d_01"
    # timestamps = 1
    # measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    # data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT]
    # values_ = [False, 10, 11, 1.1, 10011.1, "test_record"]

    print('start insert_records')
    insert_records(session, data_file)
    # # example:
    # device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_01"]
    # timestamps = [2, 3]
    # measurements_list_ = [
    #     ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
    #     ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
    # ]
    # data_type_list_ = [
    #     [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT],
    #     [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT],
    # ]
    # values_list_ = [
    #     [False, 22, 33, 4.4, 55.1, "test_records01"],
    #     [True, 77, 88, 1.25, 8.125, "test_records02"],
    # ]

    print('start insert_tablet')
    insert_tablet(session, data_file)
    # # example:
    # device_ids_ = "root.sg_test_01.d_01"
    # measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    # data_types = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT]
    # values_ = [
    #     [False, 10, 11, 1.1, 10011.1, "test01"],
    #     [True, 100, 11111, 1.25, 101.0, "test02"],
    #     [False, 100, 1, 188.1, 688.25, "test03"],
    #     [True, 0, 0, 0, 6.25, "test04"],
    # ]
    # timestamps_ = [4, 5, 6, 7]

    print('start insert_tablets')
    insert_tablets(session, data_file)


if __name__ == '__main__':
    main()
