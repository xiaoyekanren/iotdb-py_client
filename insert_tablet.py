# coding=utf-8
from parse_json import get_data_from_json
from parse_json import parse_data
from iotdb.utils.Tablet import Tablet
import time


def generate_final_data(data, device_prefix):
    list_insert_times_ = []
    list_list_values_ = []
    device_id, insert_time, list_measurements_, list_data_type_, list_values_ = parse_data(dict(data), device_prefix)  # 逐行取
    list_list_values_.append(list_values_)  # 把 list 放入 list
    list_insert_times_.append(insert_time)  # 把 int 放入 list
    tablet_ = Tablet(device_id, list_measurements_, list_data_type_, list_list_values_, list_insert_times_)
    print(
        f'device_id: {device_id}\n'
        f'list_measurements_: {list_measurements_}\n'
        f'list_data_type_: {list_data_type_}\n'
        f'list_list_values_: {list_list_values_}\n'
        f'list_insert_times_: {list_insert_times_}\n'
    )

    return tablet_


def insert_tablets(session, data_file):
    """
    好像适用于同一时间，同一设备，到了多个点
    """
    # print('***开始解析json')
    # 拿到数据 和 device前缀
    data, device_prefix = get_data_from_json(data_file)

    # print('***打开session')
    session.open(False)

    # 插入数据
    # print('***开始循环拿数据')
    tablets_ = []
    for line in data:  # line type: dict
        tablet_ = generate_final_data(line, device_prefix)
        tablets_.append(tablet_)
    start_time = time.time()
    session.insert_tablets(tablets_)
    end_time = time.time()
    print('耗时%s秒\n' % round((end_time - start_time), 2))

    # print('***关闭session')
    session.close()


def insert_tablet(session, data_file):
    """
    好像适用于同一时间，同一设备，到了多个点
    """
    # print('***开始解析json')
    # 拿到数据 和 device前缀
    data, device_prefix = get_data_from_json(data_file)

    # print('***打开session')
    session.open(False)

    # 插入数据
    # print('***开始循环拿数据')

    start_time = time.time()
    for line in data:  # line type: dict
        tablet_ = generate_final_data(line, device_prefix)
        session.insert_tablet(tablet_)
    end_time = time.time()
    print('耗时%s秒\n' % round((end_time - start_time), 2))

    # 输出插入的数据

    print('***关闭session')
    session.close()
