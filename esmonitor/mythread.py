#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# @Time    : 16/8/28 20:25
# @Author  : leon
# @Site    : 
# @File    : mythread.py
# @Software: PyCharm


from threading import Thread


class MyThread(Thread):
    def __init__(self, func):
        super(MyThread, self).__init__()
        self.func = func

    def run(self):
        self.func()
