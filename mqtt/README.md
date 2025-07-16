# iotdb-mqtt-test

## sync to server

```shell
rsync -avz ../iotdb-interface root@172.20.31.15:~/
```


## 表模型
1. 生成 “influx行协议标准” 的1行，将全部行加入到list  
2. 按照 once_write_rows 拆分list成多个list
3. 写入数据


## mqtt_topic
表模型：device  
树模型：database，取/前面的部分

例如：
```python
tree_mode = {
    'mqtt_topic': 'root.g1.d1',
}

table_mode = {
    'mqtt_topic': 'zzm/xxxxxxxxx',
}
```
