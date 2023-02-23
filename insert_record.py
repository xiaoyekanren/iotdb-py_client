# coding=utf-8
import time
from parse_json import get_data_from_json
from parse_json import parse_data


def insert_record(session, data_file):
    # print('***开始解析json')
    # 拿到数据 和 device前缀
    data, device_prefix = get_data_from_json(data_file)

    # print('***打开session')
    session.open(False)

    start_time = time.time()
    for line in data:
        device_id, insert_time, list_measurements_, list_data_type_, list_values_ = parse_data(dict(line), device_prefix)
        session.insert_record(device_id, insert_time, list_measurements_, list_data_type_, list_values_)
    end_time = time.time()
    print('耗时%s秒\n' % (end_time - start_time))

        # # 输出插入的数据
        # if device_id and insert_time and list_measurements_ and list_data_type_ and list_values_:
        #     print(
        #         f'device_id: {device_id}\n'
        #         f'insert_time: {insert_time}\n'
        #         f'list_measurements_: {list_measurements_}\n'
        #         f'list_values_: {list_values_}\n'
        #         f'list_data_type_: {list_data_type_}\n'
        #     )

    # print('***关闭session')
    session.close()


def generate_final_data(data, device_prefix):
    """
    将一行一行的数据，都放入list
    """
    device_ids_ = []  # device path， 里面是str
    insert_times_ = []  # timestamp，里面是int
    list_list_measurements_ = []  # 工况，里面是list
    list_list_data_type_ = []  # 工况类型，里面是list
    list_list_values_ = []  # 工况值，里面是list

    for line in data:  # line type: dict
        device_id, insert_time, list_measurements_, list_data_type_, list_values_ = parse_data(dict(line), device_prefix)  # 逐行取，每行都是个 dict

        device_ids_.append(device_id)  # 把 str 放入 list
        insert_times_.append(insert_time)  # 把 int 放入 list
        list_list_measurements_.append(list_measurements_)  # 把 list 放入 list
        list_list_data_type_.append(list_data_type_)  # 把 list 放入 list
        list_list_values_.append(list_values_)  # 把 list 放入 list

    return device_ids_, insert_times_, list_list_measurements_, list_list_data_type_, list_list_values_


def insert_records(session, data_file):
    # print('***开始解析json')
    # 拿到数据 和 device前缀
    data, device_prefix = get_data_from_json(data_file)

    # print('***打开session')
    session.open(False)

    # print('***开始循环拿数据')
    device_ids_, insert_times_, list_list_measurements_, list_list_data_type_, list_list_values_ = generate_final_data(data, device_prefix)

    # 插入数据
    start_time = time.time()
    session.insert_records(device_ids_, insert_times_, list_list_measurements_, list_list_data_type_, list_list_values_)
    end_time = time.time()
    print('耗时%s秒\n' % (end_time - start_time))

    # 输出插入的数据
    # print(
    #     f'device_ids_: {device_ids_}\n'
    #     f'insert_times_: {insert_times_}\n'
    #     f'list_list_measurements_: {list_list_measurements_}\n'
    #     f'list_list_values_: {list_list_values_}\n'
    #     f'list_list_data_type_: {list_list_data_type_}\n'
    #       )
    # print('***关闭session')
    session.close()
