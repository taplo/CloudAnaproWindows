# -*- coding: utf-8 -*-
"""
Created on 2019-02-03 
为了控制某些网络限制请求次数的情况，特此做了这个模块
@author: Administrator
"""

""" 多进程版，无法在Jupyter内使用
from multiprocessing import Manager, Queue, Process
from time import sleep

def auto_counter(delay, que):
    u'''
    单独进程运行的方法，用于在队列对端定时取出元素。
    '''
    while True:
        try:
            r = que.get()
            if r=='end':
                break
            sleep(delay)
        except Exception as err:
            print err.args
            sleep(1)


class rhythm():
    u'''
    定时节奏管理类\n
    初始化时设定固定时间内执行次数\n
    在每次执行操作前运行对象的checker方法，控制节奏。
    '''
    # 队列
    __q = None
    # 等待间隙
    __sleep = None
    # 独立进程
    __process = None
    # 队列数量
    qsize = 0
    # 线程方法
    __auto_counter = None
    
    def __init__(self, times=10, period=60):
        u'''times：次数\n
        period：计次周期'''
        manager = Manager()
        self.__q = manager.Queue(times)
        while not self.__q.full():
            self.__q.put('wtd')
        self.__sleep = 1.01 * period / times
        self.__auto_counter = auto_counter
        self.__process = Process(target=self.__auto_counter, args=(self.__sleep, self.__q))
        self.__process.start()
        
    def __del__(self):
        self.__process.terminate()
    
    def checker(self):
        u'''
        阻塞方法，在待定时的语句前运行，确保阻塞。
        '''
        self.__q.put('wtd')
        self.qsize = self.__q.qsize()
        
    def stop(self):
        u'''软终止独立进程的方法'''
        self.__q.put('end')
"""

#### 多线程版，可以在Jupyter中使用

from threading import Thread
from queue import Queue
from time import sleep
#from math import ceil

class Rhythm():
    '''
    定时节奏管理类
    初始化时设定固定时间内执行次数上限
    在每次循环操作前使用start方法启动，
    在循环中每次请求的操作前使用checker方法控制节奏。
    为保险起见，请求速度限制在上限的98%
    '''
    # 队列
    __q = None
    # 等待间隙
    __sleep = None
    # 独立线程
    __thread = None
    
    def __init__(self, times=10, period=60):
        '''times：次数上限
        period：计次周期（单位：秒）'''
        self.__q = Queue(1)
        self.__sleep = 1.02 * period / times
        self.__thread = self.auto_counter(self.__sleep, self.__q)
        
    class auto_counter(Thread):
        '''自动出栈的线程类'''
        __delay = None
        __que = None
        
        def __init__(self, delay, que):
            Thread.__init__(self)
            self.__delay = delay
            self.__que = que

        def run(self):
            '''
            单独进程运行的方法，用于在队列对端定时取出元素。
            '''
            while self.__que.get():
                sleep(self.__delay)

    def __del__(self):
        try:
            if self.__thread is None:
                exit()
            if self.__thread.is_alive():
                self.__q.put(False)
                del self.__thread
            else:
                del self.__thread        
        except:
            pass
    
    def start(self):
        '''启动节奏控制'''
        self.__thread.start()
        #print 'every times will pause %s'%str(self.__sleep)

    def checker(self):
        '''
        阻塞方法，在待定时的语句前运行，确保阻塞。
        '''
        if self.__thread.is_alive():
            self.__q.put(True)
        else:
            print('请先用rythm.start()启动对象计时')
        
    def stop(self):
        '''软终止独立进程的方法'''
        if self.__thread.is_alive():
            self.__q.put(False, timeout=2)
            self.__thread.join()
            del self.__thread
        else:
            del self.__thread