# MQTT 树模型写入测试程序

## 功能说明

本程序支持两种写入模式，通过配置参数 `is_one_device_write` 进行切换：

### 1. 多设备多序列写入模式（默认）
- **场景**：多个设备，每个设备有相同数量的序列
- **配置**：`is_one_device_write = False`
- **行为**：
  - 每个线程管理一个独立设备（`root.mqtt.d0`, `root.mqtt.d1`, ...）
  - 所有设备写入相同的序列范围（`s_0` 到 `s_{sensor_num-1}`）
  - 适用于模拟多个设备同时写入的场景

### 2. 单设备多序列写入模式
- **场景**：单个设备，多个线程并发写入不同序列范围
- **配置**：`is_one_device_write = True`
- **行为**：
  - 所有线程共享同一个设备（`root.mqtt`）
  - 每个线程负责不同的序列范围（线程i负责 `s_{i*sensor_num}` 到 `s_{(i+1)*sensor_num-1}`）
  - 适用于单设备高并发写入场景

## 配置参数说明

```python
class Config:
    database = "root.mqtt"  # 数据库名称
    start_time = 1000  # 起始时间戳

    thread_num = 10  # 线程数量
    # 序列数量：
    #   - is_one_device_write=False时，表示每个设备的序列数量
    #   - is_one_device_write=True时，表示每个线程负责的序列数量
    sensor_num = 100
    batch_size = 100  # 每批写入的数据条数
    loop = 100  # 每个线程执行batch_size的次数

    is_one_device_write = False  # False: 多设备模式，True: 单设备模式
    is_clear_iotdb = True  # 是否在写入前清空数据库

    # 用于结束后进行点数统计
    is_count_point = False  # 是否统计写入点数
    count_loop = 100
    count_interval = 30
```

## 使用示例

### 示例 1: 多设备模式（默认）
```python
Config.thread_num = 10
Config.sensor_num = 100
Config.is_one_device_write = False

# 结果：
# - 10个设备：root.mqtt.d0 ~ root.mqtt.d9
# - 每个设备100个序列：s_0 ~ s_99
# - 总共：10个设备 × 100个序列 = 1000个时间序列
```

### 示例 2: 单设备模式
```python
Config.thread_num = 10
Config.sensor_num = 1000
Config.is_one_device_write = True

# 结果：
# - 1个设备：root.mqtt
# - 10个线程并发写入
# - 线程0：s_0 ~ s_999
# - 线程1：s_1000 ~ s_1999
# - ...
# - 线程9：s_9000 ~ s_9999
# - 总共：1个设备 × 10000个序列 = 10000个时间序列
```

## 运行方式

1. 配置连接信息：
```python
tree_conn = {
    'mqtt_host': '172.20.31.16',
    'mqtt_port': 1883,
    'iotdb_user': 'root',
    'iotdb_password': 'TimechoDB@2021',
    'mqtt_topic': 'root.mqtt.d1',  # 树模型，topic无意义
    'qos': 2,
}
```

2. 运行程序：
```bash
python benchmark.py
```

## 数据写入流程

1. **清空数据库**（可选）：如果 `is_clear_iotdb=True`，执行 `drop database root.**`
2. **并发写入**：启动多个线程并发写入数据
3. **刷盘**：执行 `flush` 命令确保数据持久化
4. **统计点数**（可选）：如果 `is_count_point=True`，查询并统计写入的数据点数

## 注意事项

- MQTT 树模型中，topic 参数无实际意义，设备路径由 payload 中的 `device` 字段决定
- 单设备模式下，确保 `sensor_num × thread_num` 不超过系统限制
- 建议先在测试环境验证配置，再在生产环境使用
