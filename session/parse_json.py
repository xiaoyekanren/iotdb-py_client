# coding=utf-8
import json
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor


def get_data_from_json(data_file):
    """
    从 json 里面取数据，只把 listmap 返回
    """
    # 读 json
    with open(data_file, 'r') as json_file:
        json_str = json_file.read()
    # 转 字典
    json_json = dict(json.loads(json_str))
    # 去 前缀
    device_prefix = json_json.get('devicePrefix')
    # 取 数据
    listmap = json_json.get('listMap')  # list , list 里面的每一项都是一个dict
    return list(listmap), device_prefix  # 返回 listMap,前缀


def makesure_ts_datatype(key, value):
    """
    判断数据的类型
    参考 json 的 dbInFieldNames 和 types
    """
    if key == 'net':
        return str(value), TSDataType.TEXT
    elif key == 'dbm':
        return float(value), TSDataType.FLOAT
    elif key == 'solVol':
        return float(value), TSDataType.FLOAT
    elif key == 'solCur':
        return float(value), TSDataType.FLOAT
    elif key == 'batVol':
        return float(value), TSDataType.FLOAT
    elif key == 'batCharCur':
        return float(value), TSDataType.FLOAT
    elif key == 'workTemp':
        return float(value), TSDataType.FLOAT
    else:  # lat,lng
        return str(value), TSDataType.TEXT


def parse_data(i, device_prefix):
    """
    解析json，将数据一行一行的读出来
    """
    device_ids_ = str(device_prefix + i.get('id'))  # str, 完整的device_id_，拼接前缀,
    insert_time = int(i.get('ts'))  # int, insert_time
    list_measurements_ = []  # list, 工况
    list_data_type_ = []  # list, 工况的数据类型
    list_values_ = []  # list, 工况的值
    i.pop('id')  # 删除id 和 ts, 其他的开始循环添加到list
    i.pop('ts')  # id 和 ts 作为 key

    for x in i.keys():  # 生成 list_measurements_ 和 list_values_ 和 list_data_type_
        value, value_type = makesure_ts_datatype(x, i.get(x))
        list_measurements_.append(str(x))
        list_values_.append(value)
        list_data_type_.append(value_type)
    return device_ids_, insert_time, list_measurements_, list_data_type_, list_values_

