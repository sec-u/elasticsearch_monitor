#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# @Time    : 16/8/28 11:08
# @Author  : leon
# @Site    : 
# @File    : main.py
# @Software: PyCharm


import time
from Queue import Queue
from ConfigParser import ConfigParser
from esmonitor.mythread import MyThread
from esmonitor.get_data import EsMonitor
from esmonitor.insert_data import EsInsert


if __name__ == '__main__':
    # 配置文件读取
    cf = ConfigParser()
    cf.read('conf')

    hosts = [host for key, host in cf.items('hosts')]
    count_time = cf.getint('time', 'count_time')
    intervals = cf.getint('time', 'intervals')

    es_hosts = cf.get('elasticsearch', 'hosts')
    es_index_name = cf.get('elasticsearch', 'index_name')
    es_type_name = cf.get('elasticsearch', 'doc_type_name')
    es_bulk_num = cf.getint('elasticsearch', 'bulk_num')

    # 数据容器
    queue = Queue()

    # 实例化数据采集器
    for i in hosts:
        get_data = EsMonitor(hosts=i, queue=queue,
                             count_time=count_time, intervals=intervals)
        get_data_thread = MyThread(get_data.work)
        get_data_thread.start()
        time.sleep(5)

    # 实例化数据插入

    insert_data = EsInsert(queue=queue, es_index_name=es_index_name,
                           doc_type=es_type_name, es_ip_port=es_hosts,
                           bulk_num=es_bulk_num)
    insert_data_thread = MyThread(insert_data.run)
    insert_data_thread.start()
