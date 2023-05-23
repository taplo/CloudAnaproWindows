# -*- coding: utf-8 -*-
"""
Created on 20160511
filename: dailyupdate.py
每日更新通联数据
@author: wangtao
ver.1.0---2016-05-15
"""
#import pandas as pd
import datetime as dt
from progressbar import ProgressBar,Percentage,SimpleProgress,Bar,ETA
import multiprocessing
import time
#import pp

#import datayes
import anadata
import constant
#import ppcluster

#from datetime import datetime
import os

from apscheduler.schedulers.blocking import BlockingScheduler
import importlib


importlib.reload(anadata)
#检查所有线程是否全部完成
def CheckResult(result):
    '检查线程是否全部完成'
    status = {}
    if type(result) == list:
        lst = []
        lst2 = []
        for res in result:
            t = res.ready()
            lst.append(t)
            if t:
                try:
                    lst2.append(res.successful())
                except:
                    lst2.append(False)

        status['all'] = len(result)
        status['finished'] = sum(lst)
        status['successful'] = sum(lst2)
    else:
        status['all'] = len(result)
        status['finished'] = len(result)
        status['successful'] = 0

    return status
    
#检查Cluster运行情况
def check_cluster(res):
    unfinished = len(res)
    for key in list(res.keys()):
        if res[key].finished:
            unfinished -= 1
    return unfinished

    
def multi_update_stock_data():
    '''
    多线程更新数据函数
    '''    
    #更新索引文件
    index = anadata.get_stock_index()
    anadata.save_stock_index(index)
    
    
    #获得需更新数据
    
    #排除新股
    index = index.loc[index['timeToMarket']>0]
    
    downlist = index.index.tolist()
    #启动下载
    #创建线程池
    #count = multiprocessing.cpu_count() // 2
    '''
    count = 2 * multiprocessing.cpu_count()
    if count < 10:
        count = 20
    elif count < 40:
        count = 40
    else:
        count = 80
    '''
    count = 20
    #server = ('*',)
    #job = pp.Server(5, server)

    pool = multiprocessing.Pool(processes=count)
    print('开始多进程股票数据下载，共计%s个文件，%s进程！'%(len(downlist), count))
    #启动线程/进程池
    result = []
    #res = {}
    #endday = anadata.last_trade_day()
    #加载线程任务
    for down in downlist:
        result.append(pool.apply_async(anadata.update_stock_data, (down,)))
    #for down in downlist:
    #    res[down] = job.submit(anadata.update_stock_data, (down,), modules=('anadata', 'constant', 'pandas as pd', 'os', 'numpy as np', 'warnings', 'tushare as ts'), depfuncs=(anadata.get_stock_data, anadata.save_stock_data, anadata.get_stock_all_data))
            
    pool.close()
    #判断状态
    pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(), ' ', SimpleProgress(), ' | ', ETA()], maxval=len(result)).start() #画进度条
    
    status = CheckResult(result)
    while status['finished'] < status['all']:
        time.sleep(1)
        pbar.update(status['finished'])
        status = CheckResult(result)
    pbar.finish()
    pool.join()
    '''
    ch = job.get_active_nodes()
    nodes = len(ch)
    count = 0
    for key in ch.keys():
        count += ch[key]
    print u'共%s条任务，%s台主机，%s个运算核心：'%(len(downlist), nodes, count)
    pbar = ProgressBar(widgets=[Percentage(), Bar(), SimpleProgress()], maxval=len(res)).start() #画进度条
    run_count = check_cluster(res)
    while run_count > 0:
        pbar.update(len(res)-run_count)
        time.sleep(1)
        run_count = check_cluster(res)
    pbar.update(len(res))
    print ''
    job.print_stats()
    '''
    constant.STATUS['downinfo'] = '已经完成%s/%s项下载任务！其中执行完毕%s个！'%(status['finished'], status['all'], status['successful'])
    #constant.STATUS['downinfo'] = u'已经完成%s/%s项下载任务！其中执行完毕%s个！'%(len(res) - run_count, len(res), len(res) - run_count)
    #汇总详细信息

    date = {}
    '''
    sucess = 0
    error = 0
    for r in res.keys():
        t = res[r]
        tr = t()
        if type(tr)==tuple:
            if not tr[0]:
                date[tr[1]] = tr[2]
                error += 1
            else:
                sucess += 1
        else:
            error += 1
    '''
    sucess = 0
    error = 0
    for res in result:
        try:
            t = res.get()
            if not t[0]:
                date[t[1]] = t[2]
                error += 1
            else:
                sucess += 1
        except Exception as err:
            print(err.message)
            error += 1
            pass
    
    constant.STATUS['errors'] = error
    constant.STATUS['sucess'] = sucess
    
    return date

def multi_update_index_data():
    '''
    多线程更新指数数据函数
    '''    
    #更新索引文件
    index = anadata.get_index_index()
    anadata.save_index_index(index)
    
    """
    #获得需更新数据
    #获得指数列表，去掉无用的指数列表（非发行、已停止和非股票指数）
    nouse = datayes.get_index_nouse()
    downlist = index.drop(nouse, errors='ignore')
    """
    downlist = index.index.tolist()
    
    #去掉有问题的指数
    blacklist = ['000170', '000171', '000188', '000863', '000865', '000867', '000869',
                 '000891', '1C0001', '1C0002', '399016', '399017', '399018', '399422',
                 '399692', '399694', '399695', '399697', '399698', '399699', '3C0001',
                 '3C0002', '3C0003']
    for s in blacklist:
        try:
            downlist.remove(s)
        except:
            continue
    
    #启动下载
    #创建线程池
    #count = multiprocessing.cpu_count() // 2
    #server = ('*',)
    #job = pp.Server(5, server)

    '''
    count = multiprocessing.cpu_count() * 2
    if count < 10:
        count = 20
    elif count < 40:
        count = 40
    else:
        count = 80
    '''
    count = 20
    pool = multiprocessing.Pool(processes=count)
    print('开始多进程指数数据下载，共计%s个文件，%s进程！'%(len(downlist), count))
    #启动线程/进程池
    result = []
    #res = {}
    #endday = anadata.last_trade_day()
    #加载线程任务
    for down in downlist:
        result.append(pool.apply_async(anadata.update_index_data, (down, )))
    #for down in downlist.index:
    #    res[down] = job.submit(anadata.update_index_data, (down,), modules=('anadata', 'constant', 'pandas as pd', 'os', 'numpy as np', 'warnings', 'tushare as ts'), depfuncs=(anadata.get_index_data, anadata.save_index_data))

    pool.close()
    pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(), ' ', SimpleProgress(), ' | ', ETA()], maxval=len(result)).start() #画进度条

    #判断状态
    status = CheckResult(result)
    while status['finished'] < status['all']:
        time.sleep(1)
        pbar.update(status['finished'])
        status = CheckResult(result)
    pbar.finish()
    pool.join()
    '''
    ch = job.get_active_nodes()
    nodes = len(ch)
    count = 0
    for key in ch.keys():
        count += ch[key]
    print u'共%s条任务，%s台主机，%s个运算核心：'%(len(downlist), nodes, count)
    pbar = ProgressBar(widgets=[Percentage(), Bar(), SimpleProgress()], maxval=len(res)).start() #画进度条
    run_count = check_cluster(res)
    while run_count > 0:
        pbar.update(len(res)-run_count)
        time.sleep(1)
        run_count = check_cluster(res)
    pbar.update(len(res))
    print ''
    job.print_stats()
    '''
    constant.STATUS['downinfo'] = '已经完成%s/%s项下载任务！其中执行完毕%s个！'%(status['finished'], status['all'], status['successful'])
    #constant.STATUS['downinfo'] = u'已经完成%s/%s项下载任务！其中执行完毕%s个！'%(len(res) - run_count, len(res), len(res) - run_count)
    #汇总详细信息
    '''
    date = {}
    sucess = 0
    error = 0
    for r in res.keys():
        t = res[r]
        tr = t()
        if type(tr)==tuple:
            if not tr[0]:
                date[tr[1]] = tr[2]
                error += 1
            else:
                sucess += 1
        else:
            error += 1

    '''
    date = {}
    sucess = 0
    error = 0
    for res in result:
        try:
            t = res.get()
            if not t[0]:
                date[t[1]] = t[2]
                error += 1
            else:
                sucess += 1
        except Exception as err:
            print(err.message)
            error += 1
            pass
    constant.STATUS['errors'] = error
    constant.STATUS['sucess'] = sucess
    
    return date

def multi_update_etf_data():
    '''
    多线程更新ETF基金数据函数
    '''    
    #更新索引文件
    index = anadata.get_etf_index()
    anadata.save_etf_index(index)
    
    """
    #获得需更新数据
    #获得指数列表，去掉无用的指数列表（非发行、已停止和非股票指数）
    nouse = datayes.get_index_nouse()
    downlist = index.drop(nouse, errors='ignore')
    """
    downlist = index.index.tolist()
    #启动下载
    #创建线程池
    #count = multiprocessing.cpu_count() // 2
    #server = ('*',)
    #job = pp.Server(5, server)
    
    '''    
    count = multiprocessing.cpu_count() * 2
    if count < 10:
        count = 20
    elif count < 40:
        count = 40
    else:
        count = 80
    '''
    count = 20
    pool = multiprocessing.Pool(processes=count)
    print('开始多进程ETF数据下载，共计%s个文件，%s进程！'%(len(downlist), count))
    #启动线程/进程池
    result = []
    #res = {}
    #endday = anadata.last_trade_day()
    #加载线程任务
    for down in downlist:
        result.append(pool.apply_async(anadata.update_etf_data, (down, )))
    #for down in downlist.index:
    #    res[down] = job.submit(anadata.update_index_data, (down,), modules=('anadata', 'constant', 'pandas as pd', 'os', 'numpy as np', 'warnings', 'tushare as ts'), depfuncs=(anadata.get_index_data, anadata.save_index_data))

    pool.close()
    pbar = ProgressBar(widgets=[Percentage(), ' ', Bar(), ' ', SimpleProgress(), ' | ', ETA()], maxval=len(result)).start() #画进度条

    #判断状态
    status = CheckResult(result)
    while status['finished'] < status['all']:
        time.sleep(1)
        pbar.update(status['finished'])
        status = CheckResult(result)
    pbar.finish()
    pool.join()
    '''
    ch = job.get_active_nodes()
    nodes = len(ch)
    count = 0
    for key in ch.keys():
        count += ch[key]
    print u'共%s条任务，%s台主机，%s个运算核心：'%(len(downlist), nodes, count)
    pbar = ProgressBar(widgets=[Percentage(), Bar(), SimpleProgress()], maxval=len(res)).start() #画进度条
    run_count = check_cluster(res)
    while run_count > 0:
        pbar.update(len(res)-run_count)
        time.sleep(1)
        run_count = check_cluster(res)
    pbar.update(len(res))
    print ''
    job.print_stats()
    '''
    constant.STATUS['downinfo'] = '已经完成%s/%s项下载任务！其中执行完毕%s个！'%(status['finished'], status['all'], status['successful'])
    #constant.STATUS['downinfo'] = u'已经完成%s/%s项下载任务！其中执行完毕%s个！'%(len(res) - run_count, len(res), len(res) - run_count)
    #汇总详细信息
    '''
    date = {}
    sucess = 0
    error = 0
    for r in res.keys():
        t = res[r]
        tr = t()
        if type(tr)==tuple:
            if not tr[0]:
                date[tr[1]] = tr[2]
                error += 1
            else:
                sucess += 1
        else:
            error += 1

    '''
    date = {}
    sucess = 0
    error = 0
    for res in result:
        try:
            t = res.get()
            if not t[0]:
                date[t[1]] = t[2]
                error += 1
            else:
                sucess += 1
        except Exception as err:
            print(err.message)
            error += 1
            pass
    constant.STATUS['errors'] = error
    constant.STATUS['sucess'] = sucess
    
    return date

def start_download():
    """
    程序主函数
    """
    constant.STATUS['starttime'] = dt.datetime.now()
    si = constant.STATUS
    
    print('更新程序开始于：', constant.STATUS['starttime'])
    
    #######################A股数据更新##########################################    
    #A股数据更新
    #update_stock_data()
    #多线程更新
    result = multi_update_stock_data()
    
    print(si['downinfo'])
    #print u'其中%s条错误信息，如下：'%(len(si['download']))
    #print si['download'], '\n'
    #print si['upinfo']
    #print u'其中%s错误信息，如下：'%(len(si['update']))
    #print si['update'], '\n'
    print('成功：%s条！'%si['sucess'])
    print('失败：%s条！'%si['errors'])
    if len(result) > 0:    
        for res in result:
            print('%s的问题：%s'%(res, result[res]))    
    
    ######################指数数据更新##########################################
    #指数数据更新
    result = multi_update_index_data()

    print(si['downinfo'])
    #print u'其中%s条错误信息，如下：'%(len(si['download']))
    #print si['download'], '\n'
    #print si['upinfo']
    #print u'其中%s错误信息，如下：'%(len(si['update']))
    #print si['update'], '\n'
    print('成功：%s条！'%si['sucess'])
    print('失败：%s条！'%si['errors'])
    if len(result) > 0:    
        for res in result:
            print('%s的问题：%s'%(res, result[res]))    
    
    ######################ETF数据更新##########################################
    #ETF数据更新
    result = multi_update_etf_data()
    
    #######################更新结束#############################################
    constant.STATUS['endtime'] = dt.datetime.now()
    pass

    si = constant.STATUS
    print(si['downinfo'])
    #print u'其中%s条错误信息，如下：'%(len(si['download']))
    #print si['download'], '\n'
    #print si['upinfo']
    #print u'其中%s错误信息，如下：'%(len(si['update']))
    #print si['update'], '\n'
    print('成功：%s条！'%si['sucess'])
    print('失败：%s条！'%si['errors'])
    #print u'开始时间：', si['starttime']
    print('结束时间：', si['endtime'])
    print('共用时：', si['endtime'] - si['starttime'])    
    if len(result) > 0:    
        for res in result:
            s = result[res]
            print('%s的问题：%s'%(res, s.decode('gb18030')))    



if __name__ == '__main__':
    
    scheduler = BlockingScheduler()
    scheduler.add_job(start_download, 'cron', day_of_week='0-4', hour='21', minute='30')
    print('下载程序将于每个工作日夜间21:30分自动开始运行。。。请等待。。。')
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
    
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()    #采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
        print('Exit The Job!')
    
