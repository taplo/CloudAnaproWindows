# -*- coding: utf-8 -*-
"""
2019-02-01 首次编辑
更新redis_pro的数据
@author: Administrator
"""

import pandas as pd
import numpy as np
#from wttools.wtpickle import dumps, loads

#from wttools.commontools import split_list
#from math import ceil
#import tushare as ts

import sys
import time


from anapro import Anapro


'''
#处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件
import _thread
import win32api

# Now set our handler for CTRL_C_EVENT. Other control event 
# types will chain to the next handler.
def handler(dwCtrlType, hook_sigint=_thread.interrupt_main):
    if dwCtrlType == 0: # CTRL_C_EVENT
        hook_sigint()
        return 1 # don't chain to the next handler
    return 0 # chain to the next handler

win32api.SetConsoleCtrlHandler(handler, 1)
#处理Ctrl+C时发生的“forrtl: error (200): program aborting due to control-C event”错误事件
'''

def update(mode):

    try:

        if mode=='redis':
            redis_dict = {
                'host':'MyRedis',
                'port':'6379',
            }

            ana = Anapro(redis=redis_dict)
        elif mode=='sqlite':
            ana = Anapro(path="D:/workdir/default.db")
        else:
            print("存储模式选择错误！")
            return


        print('更新交易日历...')
        result = ana.update_calendar()
        if isinstance(result, bool):
            if result:
                print('交易日历更新成功！')
            else:
                print('交易日历更新存在问题！' + ' ' + str(result))
        else:
            print('交易日历更新错误：%s'%str(result))


        print('更新指数列表...')
        result = ana.update_index_index()
        if isinstance(result, int):
            if result==0:
                print('指数列表更新成功！')
            else:
                print('指数列表新增成功！')

        print('更新指数数据...')
        index_table = ana.read_index_index()
        lst = index_table.index.tolist()
        result = {}
        for code in lst:
            result[code] = ana.update_index_data(code)
            sys.stdout.flush()
            sys.stdout.write('%s\tfinished, \t%d/%d             \r'%(code, lst.index(code), len(lst)))
            
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        
        try:
            passed = rg.get_group('int')
        except:
            passed = rg.get_group('bool')
        errors = rg.get_group('tuple')
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))
        
        print('更新个股列表...')
        result = ana.updata_stock_index()
        if isinstance(result, int):
            if result==0:
                print('个股列表更新成功！')
            else:
                print('个股列表新增成功！')

        print('更新个股数据...')
        slst = ana.read_stock_index().index.tolist()
        sresult = {}
        for code in slst:
            sresult[code] = ana.updata_stock_data(ts_code=code)
            
            sys.stdout.flush()
            sys.stdout.write('%s\tfinished, \t%d/%d        \r'%(code, slst.index(code), len(slst)))
            
        result = pd.DataFrame(sresult,  index=['cq', 'qfq', 'adj']).T
        print('更新完成复权系数、除权、前复权数据共计%d条！'%(3*len(result)))

        print('更新指数成分和权重数据...')
        tmp_lst = ana.read_index_keys()
        lst = []
        for key in tmp_lst:
            s = key[-3:]
            if (s=='.SZ') or (s=='.SH'):
                lst.append(key)
        result = {}
        for key in lst:
            result[key] = ana.updata_index_weight(key)
            sys.stdout.flush()
            sys.stdout.write('%s\tfinished, \t%d/%d        \r'%(key, lst.index(key)+1, len(lst)))
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        
        try:
            passed = rg.get_group('int')
        except:
            passed = rg.get_group('bool')
            #passed = pd.Series(dtype=float)
            
        try:
            errors = rg.get_group('tuple')
        except:
            errors = pd.Series(dtype=float)
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))

        print('更新大盘每日指标...')
        lst = ana.get_top_index_index()
        result = {}
        for code in lst:
            result[code] = ana.updata_top_index_data(code)
            sys.stdout.flush()
            sys.stdout.write('%s\tfinished, \t%d/%d        \r'%(code, lst.index(code)+1, len(lst)))
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        try:
            passed = rg.get_group('int')
        except:
            passed = rg.get_group('bool')
        if len(passed)<len(res):
            errors = rg.get_group('tuple')
        else:
            errors = []
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))
        del ana
        print('已全部完成。')
        return
    except KeyboardInterrupt as SystemExit:
        del ana
        print('已终止。')
        return
    except Exception as err:
        print(err.args)



if __name__ == '__main__':

    print(pd.Timestamp.now())




    update('sqlite')
    print(pd.Timestamp.now())
    
    
    '''
    try:

        ana = Anapro()

        print('更新交易日历...')
        result = ana.update_calendar()
        if isinstance(result, bool):
            if result:
                print('交易日历更新成功！')
            else:
                print('交易日历更新存在问题！')
        else:
            print('交易日历更新错误：%s'%str(result))

        print('更新指数列表...')
        result = ana.update_index_index()
        if isinstance(result, int):
            if result==0:
                print('指数列表更新成功！')
            else:
                print('指数列表新增成功！')

        print('更新指数数据...')
        lst = ana.read_index_index().index.tolist()
        result = {}
        for code in lst:
            result[code] = ana.update_index_data(code)
            sys.stdout.flush()
            sys.stdout.write('%s finished, %d/%d        \r'%(code, lst.index(code), len(lst)))
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        
        passed = rg.get_group('int')
        errors = rg.get_group('tuple')
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))
        
        
        print('更新个股列表...')
        result = ana.updata_stock_index()
        if isinstance(result, int):
            if result==0:
                print('个股列表更新成功！')
            else:
                print('个股列表新增成功！')

        print('更新个股数据...')
        slst = ana.read_stock_index().index.tolist()
        sresult = {}
        for code in slst:
            sresult[code] = ana.updata_stock_data(ts_code=code)
            sys.stdout.flush()
            sys.stdout.write('%s finished, %d/%d       \r'%(code, slst.index(code), len(slst)))
        result = pd.DataFrame(sresult,  index=['cq', 'qfq', 'adj']).T
        print('更新完成复权系数、除权、前复权数据共计%d条！'%(3*len(result)))

        print('更新指数成分和权重数据...')
        tmp_lst = ana.read_index_keys()
        lst = []
        for key in tmp_lst:
            s = key[-3:]
            if (s==b'.SZ') or (s==b'.SH'):
                lst.append(key)
        result = {}
        for key in lst:
            result[key] = ana.updata_index_weight(key)
            sys.stdout.flush()
            sys.stdout.write('%s finished, %d/%d       \r'%(key, lst.index(key)+1, len(lst)))
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        
        try:
            passed = rg.get_group('int')
        except:
            passed = pd.Series(dtype=float)
            
        try:
            errors = rg.get_group('tuple')
        except:
            errors = pd.Series(dtype=float)
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))

        print('更新大盘每日指标...')
        lst = ana.get_top_index_index()
        result = {}
        for code in lst:
            result[code] = ana.updata_top_index_data(code)
            sys.stdout.flush()
            sys.stdout.write('%s finished, %d/%d      \r'%(code, lst.index(code)+1, len(lst)))
        res = pd.Series(result)
        #rg = res.groupby(res.map(type))
        rg = res.groupby(res.map(lambda x : type(x).__name__))
        passed = rg.get_group('int')
        if len(passed)<len(res):
            errors = rg.get_group('tuple')
        else:
            errors = []
        print("""\n更新完成：
            成功%d条，其中更新%d条，新增%d条。
            错误%d条，无数据%d条！"""%(len(passed), len(passed.loc[passed==0]), len(passed.loc[passed>0]),
                            len(errors), len(errors)))
        del ana
        print('已全部完成。')
        exit()
    except KeyboardInterrupt as SystemExit:
        del ana
        print('已终止。')
        exit()
    '''
