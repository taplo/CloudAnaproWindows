# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 22:04:28 2017
A股每日任务文件
@author: 韬
"""
import datetime
import win32api
import _thread
import dailytask
import os
from apscheduler.schedulers.blocking import BlockingScheduler
#import anadata as ana
from anapro import Anapro
import tushare as ts
import pandas as pd

import logging
import importlib
# logging.basicConfig()
log = logging.getLogger('apscheduler.executors.default')
# log.setLevel(logging.INFO)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)


# 处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件

# Now set our handler for CTRL_C_EVENT. Other control event
# types will chain to the next handler.

def handler(dwCtrlType, hook_sigint=_thread.interrupt_main):
    if dwCtrlType == 0:  # CTRL_C_EVENT
        hook_sigint()
        return 1  # don't chain to the next handler
    return 0  # chain to the next handler


win32api.SetConsoleCtrlHandler(handler, 1)
# 处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件


def get_china_calendar():
    '''
    获得国内市场日历
    '''
    df = ts.trade_cal()
    df.calendarDate = df.calendarDate.apply(pd.Timestamp)
    df.index = df.calendarDate
    df['Week'] = df.calendarDate.apply(lambda x: pd.Timestamp.weekday(x) + 1)
    df['isMonthEnd'] = df.calendarDate.apply(lambda x: x.is_month_end)
    df['isQuarterEnd'] = df.calendarDate.apply(lambda x: x.is_quarter_end)
    df['isYearEnd'] = df.calendarDate.apply(lambda x: x.is_year_end)
    df = df.drop('calendarDate', axis=1)

    return df


def run():
    '''
    执行命令的函数
    '''
    #cal = get_china_calendar()
    ana = Anapro(path='d:/workdir/default.db')
    #cal = ana.get_china_calendar()
    cal = ana.get_calendar()

    del ana

    if cal.loc[str(pd.Timestamp.today().date()), 'is_open']:
        importlib.reload(dailytask)
        dailytask.daily_task()
        print('下次任务将于明日18:30执行！')
        print('本次任务结束时间：%s' % str(pd.Timestamp.today()))
    else:
        print('今日未开盘，任务结束。下次将于明日18:30分执行。')
        print('本次任务结束时间：%s' % str(pd.Timestamp.today()))

    print('==========================================================')
    print('按 Ctrl+{0} 键退出！'.format('Break' if os.name == 'nt' else 'C'))


if __name__ == '__main__':

    # 程序启动的时候先执行一遍
    run()

    scheduler = BlockingScheduler()
    #scheduler.add_job(run, 'cron', day_of_week='0-4', hour='21', minute='30')
    scheduler.add_job(run, 'cron', hour='18', minute='30', timezone='Asia/Shanghai')
    print('程序启动！启动时间：%s' % str(pd.Timestamp.today()))
    print('系列程序将于每日18:30分自动开始运行，只在开盘日执行下载和选股。。。请等待。。。')
    '''
        year (int|str) – 4-digit year
        month (int|str) – month (1-12)
        day (int|str) – day of the (1-31)
        week (int|str) – ISO week (1-53)
        day_of_week (int|str) – number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour (int|str) – hour (0-23)
        minute (int|str) – minute (0-59)
        second (int|str) – second (0-59)
        
        start_date (datetime|str) – earliest possible date/time to trigger on (inclusive)
        end_date (datetime|str) – latest possible date/time to trigger on (inclusive)
        timezone (datetime.tzinfo|str) – time zone to use for the date/time calculations (defaults to scheduler timezone)
    
        *    any    Fire on every value
        */a    any    Fire every a values, starting from the minimum
        a-b    any    Fire on any value within the a-b range (a must be smaller than b)
        a-b/c    any    Fire every c values within the a-b range
        xth y    day    Fire on the x -th occurrence of weekday y within the month
        last x    day    Fire on the last occurrence of weekday x within the month
        last    day    Fire on the last day within the month
        x,y,z    any    Fire on any matching expression; can combine any number of any of the above expressions
    '''

    print('按 Ctrl+{0} 键退出！'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except KeyboardInterrupt as SystemExit:
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
        print('程序终止！终止时间：%s' % str(pd.Timestamp.today()))
