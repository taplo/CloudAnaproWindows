# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 22:04:28 2017
A股每日直接执行的更新任务程序
@author: 韬
"""
import pandas as pd
import dailytask
from anapro import Anapro

def run():
    '''
    执行命令的函数
    '''
    ana = Anapro(path='d:/workdir/default.db')
    cal = ana.get_calendar()

    del ana

    if cal.loc[str(pd.Timestamp.today().date()), 'is_open']:
        dailytask.daily_task()
    else:
        print('今日未开盘，任务结束。下次将于明日18:30分执行。')
  


if __name__ == '__main__':

    
    run()

