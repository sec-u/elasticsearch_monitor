#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# @Time    : 16/8/28 20:23
# @Author  : leon
# @Site    : 
# @File    : get_data.py
# @Software: PyCharm


import time
import logging
from datetime import datetime
from elasticsearch import Elasticsearch

logger = logging.getLogger('EsMonitor')


class EsMonitor(object):
    def __init__(self, hosts, queue, count_time=300, intervals=60):
        # es 集群ip
        self.hosts = hosts
        # 实例化 es 链接
        self.es = Elasticsearch(hosts=self.hosts, timeout=30)
        # 取值时间段
        self.count_time = count_time
        # 数据存放地
        self.queue = queue
        # 取值时间间隔
        self.intervals = intervals

    @staticmethod
    def indices_filter(index):
        """ 判断是否以'.'开头 """
        if index[0] == '.':
            return False
        else:
            return True

    def get_indices(self):
        """ 获取集群中所有index 并删除.开头的index"""
        indices = self.es.indices.get_aliases().keys()

        indices = [i for i in indices if self.indices_filter(i)]

        return indices

    def merge_indices(self):
        """ 去除索引时间戳,删除重复索引 """
        indices = self.get_indices()

        merge_indices = []
        for i in indices:
            merge_indices.append('-'.join(i.split('-')[:-1]))

        merge_indices = set(merge_indices)
        return merge_indices

    def get_count(self):
        """ 获取数据 """
        data = {}

        # 获取 index
        merge_indices = self.merge_indices()

        # 构建 body
        count_time = time.strftime('%Mm', time.gmtime(self.count_time))
        body = ('{"query":{"filtered":{"filter":{"range":'
                '{"@timestamp":{"gt":"now-%s"}}}}}}') % count_time

        for i in merge_indices:
            # 过滤空值
            if not i:
                continue

            data[i] = {}
            index = '%s%s' % (i, '-*')

            date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')

            count_data = self.es.count(index=index, body=body)

            num = count_data['count']

            data[i]['count'] = num
            data[i]['speed'] = num // self.count_time
            data[i]['index_name'] = i

            # 插入时间戳
            data[i]['@timestamp'] = date

            # 插入集群信息
            data[i]['hosts'] = self.hosts

        return data

    def work(self):
        while True:
            try:
                t0 = time.clock()

                data = self.get_count()
                self.queue.put(data)

                # 执行间隔时间
                t = time.clock() - t0
                # 睡眠时间减去执行时间 保证间隔时间相等
                sleep_time = self.intervals - t
                if sleep_time < 0:
                    sleep_time = 0

                time.sleep(sleep_time)
            except Exception as e:
                logger.error(e)
