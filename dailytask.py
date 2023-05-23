# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 22:14:01 2017
A股每日任务列表
更新此文件可以变更任务内容
@author: 韬
"""
#import newupdate
#import updateprodata
# import redispro2sqlite

#import dailyupdate
#import dailychooseA as da
#import chooselong as cl
#import chooseshort as cs
#import maillist

import datetime as dt
import importlib
import os

"""
#处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件
import thread
import win32api

# Now set our handler for CTRL_C_EVENT. Other control event 
# types will chain to the next handler.
def handler(dwCtrlType, hook_sigint=thread.interrupt_main):
    if dwCtrlType == 0: # CTRL_C_EVENT
        hook_sigint()
        return 1 # don't chain to the next handler
    return 0 # chain to the next handler

win32api.SetConsoleCtrlHandler(handler, 1)
#处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件
"""


def daily_task():
    ''''
    每日执行任务列表
    '''
    today = str(dt.date.today())
    
    print('每日数据更新....')
    os.system("python updateprodata.py")

    print('上传数据文件....')
    os.system("python upload.py")

    '''
    importlib.reload(redispro2sqlite)
    redispro2sqlite.trans()
    
    reload(dailyupdate)
    dailyupdate.start_download()
    
    reload(newupdate)
    newupdate.newupdate()
    ''' 

    print('%s数据更新完成！'%today)


    """
    
    reload(da)
    reload(cl)
    reload(cs)
    reload(maillist)
    results1 = cl.choose()
    results2 = cs.choose_short()
    
    mail_result = {}
   
    if len(results1) > 0:
        mail_result[u'长期选股信号'] = results1
    else:
        mail_result[u'长期选股信号'] = 'None'
         
    if len(results2) > 0:
        mail_result[u'短线选股信号'] = results2
    else:
        mail_result[u'短线选股信号'] = 'None'
        
        
    maillist.send_mail(mail_result)
    
    '''
    results = da.simple_choose()
    if len(results) > 0:
        maillist.send_mail(u'简单选股信号' ,results)
    
    resultg = da.guppy_countback_choose()
    if len(resultg) > 0:
        maillist.send_mail(u'顾比选股信号' ,resultg)
     
    if len(results) + len(resultg) ==0:
        maillist.send_mail(u'当日任务提示', u'今日完成运行任务！')
    '''
    #result = da.special_choose()
    #if len(result) > 0:
    #    print result
    #    maillist.send_mail(u'当日选股信号', result)
    #else:
    #    maillist.send_mail(u'当日任务提示', u'今日完成运行任务！')
    """
    print('%s任务清单完成，请等待下次任务执行！'%today)

if __name__ == '__main__':
    
    daily_task()