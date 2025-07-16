# coding=utf-8
import sys
import time

from iotdb.Session import Session
import random
import datetime
import string


random.seed(0)  # 设置随机种子以便重现

def generate_random_int32():
    """生成随机的int32类型数据"""
    return random.randint(10000000, 99999999)


def generate_random_float(integer_length=5, decimal_length=10):
    """生成随机的float类型数据，指定整数和小数长度"""
    # 生成整数部分（考虑符号）
    sign = random.choice([-1, 1])
    integer_min = 10 ** (integer_length - 1)
    integer_max = (10 ** integer_length) - 1
    integer_part = random.randint(integer_min, integer_max)

    # 生成小数部分
    decimal_part = random.randint(0, (10 ** decimal_length) - 1)

    # 组合并转换为浮点数
    value = sign * (integer_part + decimal_part / (10 ** decimal_length))
    return value


def generate_random_boolean():
    """生成随机的boolean类型数据"""
    return random.choice([True, False])


def generate_random_text(min_length=10, max_length=10):
    """生成随机的text类型数据"""
    length = random.randint(min_length, max_length)
    return ''.join(random.choice(string.ascii_letters + string.digits + ' ') for _ in range(length))


def generate_random_blob(size=6):
    """生成随机Blob数据，并转换为IoTDB支持的十六进制字符串格式"""
    # 生成随机字节序列
    binary_data = bytes(random.randint(0, 255) for _ in range(size))
    # 将二进制数据转换为十六进制字符串，并添加'0x'前缀
    # hex_string = '0x' + binary_data.hex()
    hex_string = 'X' + '\'' + binary_data.hex() + '\''

    return hex_string


def generate_random_date(start_year=1970, end_year=2025):
    """生成随机的date类型数据"""
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # 简化处理，避免月份天数问题
    return datetime.date(year, month, day)


def generate_random_timestamp(start_year=1970, end_year=2025):
    """生成随机的Unix时间戳（自1970年1月1日以来的秒数）"""
    # 确保开始年份不早于1970年（Unix时间戳起点）
    start_year = max(start_year, 1970)

    # 生成随机日期时间
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)

    # 创建datetime对象
    dt = datetime.datetime(year, month, day, hour, minute, second, microsecond)

    # 转换为Unix时间戳（秒数）
    timestamp = dt.timestamp()

    return int(timestamp * 1000)


def main():
    session = Session(ip, port_, username_, password_)

    try:
        session.open(False)

        metadata_sql = """
        create timeseries root.g1.d1.s1 int32;
        create timeseries root.g1.d1.s2 int64;
        create timeseries root.g1.d1.s3 float;
        create timeseries root.g1.d1.s4 double;
        create timeseries root.g1.d1.s5 boolean;
        create timeseries root.g1.d1.s6 text;
        
        create timeseries root.g1.d1.s7 blob;
        create timeseries root.g1.d1.s8 date;
        create timeseries root.g1.d1.s9 string;
        create timeseries root.g1.d1.s10 timestamp;
        """

        while True:
            before_insert = int(time.time())  # s
            sql = (f'insert into root.g1.d1(time,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values('
                   f'{before_insert * 1000},'  # time
                   f'{generate_random_int32()},'  # int32
                   f'{generate_random_int32() * 10000},'  # int64
                   f'{generate_random_float(integer_length=2,decimal_length=5)},'  # float
                   f'{generate_random_float(integer_length=5,decimal_length=10)},'  # double
                   f'{generate_random_boolean()},'  # boolean
                   f'\'{str(generate_random_text())}\','  # string
                   f'"{generate_random_blob()}",'  # blob
                   f'\'{generate_random_date()}\','  # date
                   f'\'{generate_random_text()}\','  # text
                   f'{generate_random_timestamp()})'  # timestamp
                   )
            print(sql)
            session.execute_non_query_statement(sql)

            after_insert = int(time.time())

            if after_insert - before_insert > 1:
                continue
            else:
                time.sleep( 1 - (after_insert - before_insert) )
    except Exception as e:
        print(e)
    finally:
        session.close()


if __name__ == '__main__':
    ip = "192.168.100.16"
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    main()
