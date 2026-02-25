# coding=utf-8
import requests
import json

# 配置基础信息
BASE_URL = "http://172.16.98.16:18080/rest/table/v1"
# Basic认证信息（root:root，对应curl中的cm9vdDpyb290）
USERNAME = "root"
PASSWORD = "TimechoDB@2021"

# 通用请求头
headers = {
    "Content-Type": "application/json"
}


def create_database():
    """创建 rest_table 数据库"""
    url = f"{BASE_URL}/nonQuery"
    # 请求体数据
    data = {
        "sql": "create database rest_table",
        "database": ""
    }
    try:
        response = requests.post(
            url=url,
            headers=headers,
            auth=(USERNAME, PASSWORD),
            json=data
        )
        # 打印响应结果，便于调试
        print("=== 创建数据库响应 ===")
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        response.raise_for_status()  # 抛出HTTP状态码异常（如4xx/5xx）
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"创建数据库失败: {e}")
        return None


def create_table():
    """在 rest_table 库中创建 t1 表"""
    url = f"{BASE_URL}/nonQuery"
    # 请求体数据（注意SQL语句的换行处理，提升可读性）
    create_table_sql = """
                       CREATE TABLE t1
                       (
                           time         TIMESTAMP TIME,
                           region       STRING TAG,
                           plant_id     STRING TAG,
                           device_id    STRING TAG,
                           model_id     STRING ATTRIBUTE,
                           maintenance  STRING ATTRIBUTE,
                           temperature  FLOAT FIELD,
                           humidity     FLOAT FIELD,
                           status       Boolean FIELD,
                           arrival_time TIMESTAMP FIELD
                       ) \
                       """
    data = {
        "sql": create_table_sql.strip(),  # 去除首尾空白字符
        "database": "rest_table"
    }
    try:
        response = requests.post(
            url=url,
            headers=headers,
            auth=(USERNAME, PASSWORD),
            json=data
        )
        print("\n=== 创建表响应 ===")
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"创建表失败: {e}")
        return None


def insert_data():
    """向 rest_table 库的 t1 插入数据"""
    url = f"{BASE_URL}/insertTablet"
    data = {
        "database": "rest_table",
        "table": "t1",
        "column_categories": ["TAG", "TAG", "TAG", "ATTRIBUTE", "ATTRIBUTE", "FIELD", "FIELD", "FIELD", "FIELD"],
        "timestamps": [1739702535000, 1739789055000],
        "column_names": ["region", "plant_id", "device_id", "model_id", "maintenance", "temperature", "humidity",
                         "status", "arrival_time"],
        "data_types": ["STRING", "STRING", "STRING", "STRING", "STRING", "FLOAT", "FLOAT", "BOOLEAN", "TIMESTAMP"],
        "values": [
            ["beijing","p_1","d_1","m_1","m_1",3.14,4.15, True, 1739702535000],
            ["tianjin","p_1","d_1","m_1","m_1",3.15,4.16, True, 1739789055000],
        ],
    }
    try:
        response = requests.post(
            url=url,
            headers=headers,
            auth=(USERNAME, PASSWORD),
            json=data
        )
        print("\n=== 插入数据响应 ===")
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"插入数据失败: {e}")
        return None


def query_data():
    """查询 rest_table 库中 t1 表的s1,s2,s3字段数据"""
    url = f"{BASE_URL}/query"  # 查询接口是/query（区别于非查询的nonQuery）
    data = {
        "database": "rest_table",
        "sql": "select * from t1"
    }
    try:
        response = requests.post(
            url=url,
            headers=headers,
            auth=(USERNAME, PASSWORD),
            json=data
        )
        print("\n=== 查询数据响应 ===")
        print(f"状态码: {response.status_code}")
        print(f"响应内容（格式化）: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
        response.raise_for_status()
        return response.json()  # 返回解析后的JSON数据，方便后续处理
    except requests.exceptions.RequestException as e:
        print(f"查询数据失败: {e}")
        return None


# 主执行逻辑
if __name__ == "__main__":
    # 按顺序执行：创建数据库 → 创建表 → 插入数据 → 查询数据
    create_database()
    create_table()
    insert_data()
    query_data()
