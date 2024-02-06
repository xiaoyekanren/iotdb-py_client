# coding=utf-8
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
import random
import string
import re

# iotdb连接信息
ip = "127.0.0.1"
port_ = "6669"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)

# 数据写入的参数
sg = 1
# 这里手动区分一下，依靠python做split好麻烦。。
case_device = [
    'root.sg1.01',
    "root.sg1.'02'",
    'root.sg1."03"',
    'root.sg1.a04',
    "root.sg1.'a-5'",
    # 'root.sg1.`a.1`',  # 插入失败
    "root.sg1.'a.6'",
    'root.sg1."a.7"',
    "root.sg1.'a.8'",
    "root.sg1.'a9`a9'",
    'root.sg1.`a10`',
]
case_sensor = [
    '01',
    "'02'",
    '"03"',
    'a04',
    "'a-5'",
    # '`a.6`',  #插入失败
    "'a.6'",
    '"a.7"',
    "'b.8'",
    "'a9`a9'",
    '`a10`',
]
device_num = len(case_device)  # 只能是9个，不然就重复了，```后续可以在这里做并发```
sensor_num = len(case_sensor)  # 只能是9个，不然就重复了，每个tablet都会写所有sensor
num_of_insert_per_device = 500  # 每个设备要写入的次数
insert_point_one_tablet = 20000  # 一次插入的点数，例如10000

start_time = 1672502400000  # 2023-01-01 00:00:00
time_interval = 5000  # 单位：ms，用于insert时，每个tablet的时间点的间隔，例如5000，5秒


def generate_time(last_time, quantity_generated, interval):
    time_list = []

    for i in range(quantity_generated):
        time_list.append(last_time + interval)
        last_time += interval

    return last_time, time_list


def generate_random_value(data_type):
    if data_type == TSDataType.BOOLEAN:
        return random.choice([True, False])
    elif data_type == TSDataType.INT32:  # 对于INT32类型，我们生成了一个最多9位的随机整数。
        return random.randint(-999999999, 999999999)
    elif data_type == TSDataType.INT64:  # 对于INT64类型，我们生成了一个最多10位的随机整数。
        return random.randint(-9999999999, 9999999999)
    elif data_type == TSDataType.FLOAT:  # 对于浮点数（FLOAT），我们生成了一个最多10位整数部分的随机数，并随机确定了它们小数点后的位数。
        return round(random.uniform(-9999999999, 9999999999), random.randint(0, 7))
    elif data_type == TSDataType.DOUBLE:  # 对于双精度数（DOUBLE），我们生成了一个最多10位整数部分的随机数，并随机确定了它们小数点后的位数。
        return round(random.uniform(-9999999999, 9999999999), random.randint(0, 15))
    elif data_type == TSDataType.TEXT:  # 对于文本（TEXT），我们生成了一个随机长度（1到10个字符）的字符串。
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(1, 10)))
    else:
        raise ValueError("Unknown data type")


def generate_type(length: int):
    # iotdb的类型
    iotdb_type_list = [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                       TSDataType.TEXT]
    # 计算原列表的长度
    list_length = len(iotdb_type_list)

    # 生成序列类型列表
    type_list = [
        iotdb_type_list[i % list_length] for i in range(length)  # 取余
    ]
    return type_list


def generate_value(type_list, data_line):
    data_list = []
    for line in range(data_line):
        line_list = []
        for type_ in type_list:
            line_list.append(generate_random_value(type_))
        data_list.append(line_list)
    return data_list


def generate_tablet():
    # list_device = generate_device(device_num)
    # list_sensor_: list = generate_sensor(sensor_num)  # 只需生成1次

    list_device = case_device
    list_sensor_ = case_sensor

    list_data_type_ = generate_type(sensor_num)

    last_time = start_time

    session.open(False)

    for index in range(num_of_insert_per_device):  # 按照插入数据的批次
        print(f'info: batch {index}.')
        last_time, list_insert_times_ = generate_time(last_time, insert_point_one_tablet, time_interval)
        # print([int(i / 1000) for i in list_insert_times_])
        for device_id in list_device:

            list_values_ = generate_value(list_data_type_, insert_point_one_tablet)

            # print(
            #     f'device_id: {device_id}\n'
            #     f'list_sensor_: {list_sensor_}\n'
            #     f'list_data_type_: {list_data_type_}\n'
            #     f'list_list_values_: {list_values_}\n'
            #     f'list_insert_times_: {list_insert_times_}\n'
            #     f'------------------'
            # )

            tablet_ = Tablet(
                device_id=device_id,
                measurements=list_sensor_,
                data_types=list_data_type_,
                values=list_values_,
                timestamps=list_insert_times_
            )
            session.insert_tablet(tablet_)

            # examples:
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
    session.close()


def main():
    print('start insert_tablet')
    generate_tablet()

    # print('start insert_tablets')
    # insert_tablets(session, data_file)


if __name__ == '__main__':
    main()
