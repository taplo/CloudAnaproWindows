# -*- coding: utf-8 -*-
"""
2019-02-01 首次编辑
制作基于TuShare Pro 版接口的历史数据下载更新等接口包
功能类似于原来anadata数据包，简化封装TuShare Pro和Redis
@author: Administrator
"""

import pandas as pd
import numpy as np

import redis as red
import sqlite2kv as sqlkv
from wtpickle import dumps, loads
from rhythm import Rhythm

from commontools import split_list
#from math import ceil
import tushare as ts

#import sys
import time

class Anapro:
    '''
    数据接口包！
    网络连接使用TuShare Pro接口
    本地使用Redis数据库接口
    '''
    # 属性区
    __ts_api = None
    __rpool = None
    __rhy = None
    __calendar = None
    
    __ConnectionPool = None
    __StrictRedis = None
    
    __time_table = [
            {'start_date':'19901219', 'end_date':'20070523'},
            {'start_date':'20070524'}
        ]

    # 类基本区
    def __init__(self, tushare=None, redis=None, path=None):
        '''
        参数：
        tushare：字符串，TuShare的Token字符串
        redis：字典
            host：字符串，Redis服务器地址
            port：字符串，Redis服务器端口
            password：字符串，Redis服务器密码
            db：整数，Redis数据库编号（通常为[0~15]）
        path：字符串，SQLite的数据库文件目录
        '''

        if path is None:
            if redis is None:
                redis = {
                    'host': 'localhost',
                    'port': '6379',
                    'db': 0
                }
            self.__StrictRedis = red.StrictRedis
            # 建立redis连接池
            self.__rpool = red.ConnectionPool(**redis)
        else:
            self.__StrictRedis = sqlkv.StrictRedis
            # 建立sqlite假连接池
            self.__rpool = sqlkv.ConnectionPool(path=path)
        

        # 设置TuShare的Token和接口
        self.__ts_api = ts.pro_api(tushare)

        # 设置节奏控制器
        self.__rhy = Rhythm(495, 60)
        self.__rhy.start()

        # 设置日历
        try:
            self.__calendar = self.read_calender()
            if type(self.__calendar)!=pd.DataFrame:
                self.__calendar = self.get_calendar()
        except:
            self.__calendar = self.get_calendar()


    def __del__(self):
        #print 'stoping rhythm'
        self.__rhy.stop()
        #print 'deleting ts_api'
        del self.__ts_api
        '''
        #print 'disconnecting redis'
        try:
            self.__rpool.disconnect()
        finally:
            #print 'cleaning up'
            del self.__rpool
        '''
    # 方法区

    # 通用方法
    def __transe_date2tu(self, d):
        '''输入Timestamp，输出TuShare需要的时间格式'''
        #assert isinstance(d, pd.Timestamp), u'日期格式错误'
        s = str(d)[:10]
        return s.replace('-', '')

    def __transe_date2pd(self, s):
        '''输入TuShare日期格式， 输出pandas的Timestamp时间格式'''
        return pd.Timestamp(s)

    # 基础数据获取方法

    def _query_data(self, key, args):
        '''
        通用获取数据接口
        key ：接口名称
        args为字典，其中为各个参数的指定
        '''
        assert isinstance(args, dict)
        try:
            self.__rhy.checker()
            return self.__ts_api.query(key, **args)
        except Exception as err:
            print(type(err), err.args, "query with:", key, args)
            # 作为关键方法，出错后向上传递错误信息，终止执行
            raise err
            #return type(err), err.args

    def _bar_data(self, args):
        '''
        通用行情数据获取接口
        args:（字典结构）
            ts_code：代码（必有）
            start_date：开始日期
            end_date：结束日期
            asset：资产类别，默认为E
                E：股票
                I：指数
                C：数字货币
                FT：期货
                FD：基金
                O：期权
            adj：复权类型，默认为None
                None：未复权
                qfq：前复权
                hfq：后复权
            freq：数据频度，默认D
                1MIN：1分钟
                5MIN：5分钟
                15MIN：15分钟
                30MIN：30分钟
                60MIN：分钟
                D：日线
            factors：股票因子
                只对E（股票有效），支持tor换手率，vr量比
        '''
        assert isinstance(args, dict), u'参数错误'
        try:
            self.__rhy.checker()
            data = ts.pro_bar(api=self.__ts_api, **args)
            data.index = data.trade_date.map(self.__transe_date2pd)
            data = data.drop(['ts_code', 'trade_date'], axis=1)
            return data.sort_index()
        except Exception as err:
            print(type(err), err.args, "pro_bar with:", args)
            # 作为关键方法，出错后向上传递错误信息，终止执行
            raise err
            #return type(err), err.args

    def _save_string(self, name, data):
        '''通过redis接口以string类型保存数据'''
        try:
            r = self.__StrictRedis(connection_pool=self.__rpool)
            result = r.set(name, dumps(data))
            return result
        except Exception as err:
            print(type(err), err.args, "save_string with:", name)
            return type(err), err.args

    def _read_string(self, name):
        '''通过redis接口获得以string类型保存的数据'''
        try:
            r = self.__StrictRedis(connection_pool=self.__rpool)
            data = loads(r.get(name))
            return data
        except Exception as err:
            print(name, type(err), err.args, "read_string with:", name)
            #raise err
            return type(err), err.args

    def _save_hash(self, args):
        '''
        通过redis接口以hash类型保存数据
        批量保存：name, data两个参数，data类型为dict
        单条保存：name, key, data三个参数，data类型为非dict
        '''
        try:
            r = self.__StrictRedis(connection_pool=self.__rpool)
            if len(args) == 2 and isinstance(args, dict):
                data = {}
                for key in args['data'].keys():
                    data[key] = dumps(args['data'][key])
                return r.hmset(args['name'], data)
            elif len(args) == 3:
                return r.hset(args['name'], args['key'],
                              dumps(args['data']))
            else:
                return 'Args Error!', 'Args type or struct error!'
        except Exception as err:
            print("saving hash error:", type(err), err.args)
            return type(err), err.args

    def _read_hash(self, name, key):
        '''
        通过redis接口读取以hash类型保存的数据
        只能单条读取，暂时不支持批量读取。
        '''
        try:
            r = self.__StrictRedis(connection_pool=self.__rpool)
            return loads(r.hget(name, key))
        except Exception as err:
            print(type(err), err.args)
            return type(err), err.args
    
    def _read_hash_keys(self, name):
        '''
        通过redis接口，获得name下的所有keys值列表
        '''
        try:
            r = self.__StrictRedis(connection_pool=self.__rpool)
            def to_decode(var):
                if isinstance(var, bytes):
                    return var.decode()
                else:
                    return var
            return list(map(to_decode, r.hkeys(name)))
        except Exception as err:
            print(type(err), err.args)
            return type(err), err.args

        
    def _cq2qfq(self, data, adj_factor):
        '''
        将除权数据，使用复权因子计算出前复权数据
        '''
        assert isinstance(data, pd.DataFrame), 'data 数据格式错误'
        assert isinstance(adj_factor, pd.DataFrame), 'adj_factor 数据格式错误'
        
        temp = data.copy()
        temp['adj'] = adj_factor.adj_factor
        temp['last_adj'] = adj_factor.adj_factor[-1]
        temp['factor'] = temp.adj / temp.last_adj
        cols = ['open', 'high', 'low', 'close', 'pre_close']
        for col in cols:
            try:
                temp[col] = np.round(temp[col] * temp.factor, 4)
            except:
                continue
        return temp.drop(['adj', 'last_adj', 'factor'], axis=1) 

    # 交易日历相关
    def get_calendar(self):
        '''获得交易所日历'''
        cals = []
        key = 'trade_cal'
        args = {'start_date': '19901219'}
        while self.__transe_date2pd(args['start_date']) < pd.Timestamp.now():
            cal_p = self._query_data(key, args)
            cal_p.index = cal_p.cal_date.apply(pd.Timestamp)
            cals.append(cal_p)
            args['start_date'] = cal_p.cal_date.max()

        if len(cals)==1:
            return cals[0]
        elif len(cals)>1:
            return pd.concat(cals, axis=0).drop_duplicates()# [['is_open']]
        else:
            return pd.DataFrame()

    def show_index_info(self):
        print('''
            名称	类型	默认显示	描述
            ts_code	str	Y	TS代码
            symbol	str	Y	股票代码
            name	str	Y	股票名称
            area	str	Y	地域
            industry	str	Y	所属行业
            fullname	str	N	股票全称
            enname	str	N	英文全称
            cnspell	str	N	拼音缩写
            market	str	Y	市场类型（主板/创业板/科创板/CDR）
            exchange	str	N	交易所代码
            curr_type	str	N	交易货币
            list_status	str	N	上市状态 L上市 D退市 P暂停上市
            list_date	str	Y	上市日期
            delist_date	str	N	退市日期
            is_hs	str	N	是否沪深港通标的，N否 H沪股通 S深股通
            ''')



    def update_calendar(self):
        '''更新交易所日历'''
        cal = self.read_calender()
        if not isinstance(cal, pd.DataFrame):
            return self._save_string('calendar', self.__calendar)
        
        if cal.fillna('').eq(self.__calendar.fillna('')).all().all():
            return True
        else:
            return self._save_string('calendar', self.__calendar)

    def read_calender(self):
        '''读取交易所日历'''
        return self._read_string('calendar')

    # 股票相关
    def get_stock_index(self):
        '''获取股票列表'''
        key = 'stock_basic'
        args = {
            'fields':
            'ts_code, symbol, name, area, industry, fullname, enname, market, exchange,\
            curr_type, list_status, list_date, delist_date, is_hs'
        }
        
        stock_index = self._query_data(key, args).set_index(
            'ts_code', drop=True)
        return stock_index

    def updata_stock_index(self):
        '''更新股票列表'''
        remote_data = self.get_stock_index()
        local_data = self.read_stock_index()
        if not isinstance(local_data, pd.DataFrame):
            args = {'name': 'list', 'key': 'stock', 'data': remote_data}
            result = self._save_hash(args)
        else:
            if local_data.fillna('').eq(remote_data.fillna('')).all().all():
                result = 0
            else:
                args = {'name': 'list', 'key': 'stock', 'data': remote_data}
                result = self._save_hash(args)
        return result

    def read_stock_index(self):
        '''读取股票列表'''
        return self._read_hash('list', 'stock')
    
    def read_stock_keys(self):
        '''读取本地已保存股票数据的keys列表'''
        return self._read_hash_keys('adj')

    def get_stock_data(self,
                       ts_code,
                       adj=None,
                       freq='D'):
        '''下载股票除权数据、复权因子数据，计算前复权数据，并保存'''
        try:
            result = []
            for period in self.__time_table:
                args = {
                    'ts_code': ts_code,
                    'asset': 'E',
                    'adj': adj,
                    'freq': freq
                }
                args.update(period)
                res = self._bar_data(args)
                
                if (len(res) > 0) and (isinstance(res, pd.DataFrame)):
                    result.append(res)

            if len(result) > 1:
                target = pd.concat(result, axis=0).drop_duplicates()
            else:
                target = result[0]

            if len(target)>0:
                target.trade_date = target.trade_date.map(pd.Timestamp)
                target = target.set_index('trade_date', drop=True).sort_index()
                target = target.drop('ts_code', axis=1)
            return target
        except Exception as err:
            print("getting stock data:", type(err), err.args)
            return type(err), err.args

    def updata_stock_data(self,
                          ts_code,
                          adj=None,
                          freq='D',
                          factors=['tor', 'vr']):
        '''更新股票数据'''
        # 先更新复权因子（复权因子是一次性返回数据，不限制4000，以此为索引下载除权数据

        # 获得复权因子，并保存
        adj_data = self.get_adj_factor(ts_code)
        if len(adj_data)>0 and type(adj_data)==pd.DataFrame:
            aargs = {
                'name': 'adj',
                'key': ts_code,
                'data': adj_data
            }
            adj_result = self._save_hash(aargs)
        else:
            if len(adj_data)>0:
                print(adj_data)
            return ts_code, 'No data', '%s is no data download.' % ts_code

        # 分拆索引
        '''
        index = adj_data.index.tolist()
        p = ceil(1.0 * len(index) / 4000)
        parts = []
        for i in range(p):
            part = index[i*4000:i*4000+4000]
            parts.append([part[0], part[-1]])
        '''
        parts = []
        lst = split_list(adj_data.index.tolist(), 6000)
        for l in lst:
            parts.append([l[0],l[-1]])
        # 分段下载除权数据
        result = []
        for pa in parts:
            pargs = {
                'ts_code': ts_code,
                'asset': 'E',
                'adj': adj,
                'freq': freq,
                'start_date': self.__transe_date2tu(pa[0]),
                'end_date': self.__transe_date2tu(pa[1])
            }
            while True:
                try:
                    res = self._bar_data(pargs)
                    if isinstance(res, pd.DataFrame):
                        result.append(res)
                        break
                    else:
                        print(res)
                        continue
                except:
                    continue
        data = pd.concat(result)
        args = {
            'name': 'cq' if adj == None else adj,
            'key': ts_code,
            'data': data
        }
        cq_result = self._save_hash(args)

        # 计算并写入前复权数据
        qargs = {
            'name': 'qfq',
            'key': ts_code,
            'data': self._cq2qfq(data, adj_data)
        }
        qfq_result = self._save_hash(qargs)

        return cq_result, qfq_result, adj_result

    def read_stock_data(self, ts_code, adj=None):
        '''读取股票数据'''
        return self._read_hash('cq' if adj == None else adj, ts_code)
    
    def get_adj_factor(self, ts_code):
        '''下载复权因子数据'''
        key = 'adj_factor'
        result = []
        args = {}
        for period in self.__time_table:
            args = {'ts_code':ts_code}
            args.update(period)
            res = self._query_data(key, args)
            if len(res) > 0:
                result.append(res)

        if len(result) > 1:
            target = pd.concat(result, axis=0).drop_duplicates()
        elif len(result) == 1:
            target = result[0]
        else:
            target = pd.DataFrame()

        if len(target)>0:
            target.trade_date = target.trade_date.map(pd.Timestamp)
            target = target.set_index('trade_date', drop=True).sort_index()
            target = target.drop('ts_code', axis=1)
        return target
    
    def read_adj_factor(self, ts_code):
        '''读取复权因子数据'''
        return self._read_hash('adj', ts_code)

    # 指数相关
    def get_index_index(self):
        '''
        获取指数列表
        市场代码 	说明
        MSCI 	MSCI指数
        CSI 	中证指数
        SSE 	上交所指数
        SZSE 	深交所指数
        CICC 	中金所指数
        SW 	申万指数
        CNI 	国证指数
        OTH 	其他指数
        '''
        try:
            indexes = []
            # 目前不是所有的市场都有数据，暂时先选择有数据的市场，今后再进行考虑
            #market_list = ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'CNI', 'OTH']
            # market_list = ['SSE', 'CSI', 'SZSE', 'CNI']
            market_list = ['CSI', 'CNI', 'SZSE', 'SSE', 'MSCI']
            for mark in market_list:
                key = 'index_basic'
                args = {
                    'market':mark,
                    'fields':[
                        "ts_code",
                        "name",
                        "market",
                        "publisher",
                        "category",
                        "base_date",
                        "base_point",
                        "list_date",
                        "fullname",
                        "index_type",
                        "weight_rule",
                        "desc",
                        "exp_date"
                        ]
                }
                indexes.append(self._query_data(key, args))
            index = pd.concat(indexes, axis=0).set_index('ts_code', drop=True)
            return index
        except Exception as err:
            print("getting index's index:", type(err), err.args)
            return type(err), err.args

    def update_index_index(self):
        '''更新指数列表'''
        remote_index = self.get_index_index()

        if len(remote_index)>0:
            local_index = self.read_index_index()
            if not isinstance(local_index, pd.DataFrame):
                args = {'name': 'list', 'key': 'index', 'data': remote_index}
                result = self._save_hash(args)
            else:
                if remote_index.fillna('').eq(local_index.fillna('')).all().all():
                    result = 0
                else:
                    args = {'name': 'list', 'key': 'index', 'data': remote_index}
                    result = self._save_hash(args)
        else:
            result = 0
        return result

    def read_index_index(self):
        '''读取指数列表'''
        return self._read_hash('list', 'index')

    def read_index_keys(self):
        '''读取本地实际保存的指数编码列表'''
        #r = redis.StrictRedis(connection_pool=self.__rpool)
        #return r.hkeys('index')
        return self._read_hash_keys('index')

    def get_index_data(self, ts_code):
        '''获得指数数据'''
        try:
            key = 'index_daily'
            args = {
                'ts_code':ts_code
            }
            data = self._query_data(key, args)
            if len(data)>0:
                data.index = data.trade_date.map(self.__transe_date2pd)
                return data.drop(['ts_code', 'trade_date'], axis=1).sort_index()
            else:
                return pd.DataFrame()
        except Exception as err:
            print("with get_index_data", type(err), err.args)
            return type(err), err.args

    def update_index_data(self, ts_code):
        '''更新指数数据'''
        data = self.get_index_data(ts_code=ts_code)
        if len(data) > 0:
            args = {'name': 'index', 'key': ts_code, 'data': data}
            return self._save_hash(args)
        else:
            return 'No data', '%s is no data download.' % ts_code

    def read_index_data(self, ts_code):
        '''读取指数数据'''
        return self._read_hash('index', ts_code)
    
    def get_index_weight(self, index_code):
        '''获取指数的月度成分和权重，目测只支持深圳和上海的指数。'''
        try:
            key = 'index_weight'
            args = {
                'index_code':index_code
            }
            # 此接口的频率要求每分钟低于70
            time.sleep(1)
            data = self._query_data(key, args)
            return data[['con_code', 'trade_date', 'weight']]
        except Exception as err:
            print("with get_index_weight", type(err), err.args)
            return type(err), err.args
        
    def updata_index_weight(self, index_code):
        '''更新指数的月度成分和权重'''
        data = self.get_index_weight(index_code=index_code)
        assert isinstance(data, pd.DataFrame), u'返回数据格式不正确'
        if len(data) > 0:
            args = {'name':'index_weight', 'key':index_code, 'data':data}
            return self._save_hash(args)
        else:
            return 'No data', '%s is no data download.'%index_code
        
    def read_index_weight_keys(self):
        '''获得本地已经保存的成分权重代码'''
        return self._read_hash_keys('index_weight')
    
    def read_index_weight_data(self, index_code):
        '''从本地读取指数成分和权重数据'''
        return self._read_hash('index_weight', index_code)
    
    def get_top_index_index(self):
        '''获得大盘指数列表'''
        try:
            key = 'index_dailybasic'
            args = {
                'trade_date':'20190211'
            }
            data = self._query_data(key, args)
            return data['ts_code'].tolist()
        except Exception as err:
            print(type(err), err.args)
            return type(err), err.args
    
    def get_top_index_data(self, ts_code):
        '''获得大盘指数每日指标'''
        try:
            key = 'index_dailybasic'
            args = {
                'ts_code':ts_code
            }
            data = self._query_data(key, args)
            data = data.set_index('trade_date', drop=True)
            data.index = data.index.map(self.__transe_date2pd)
            return data.drop('ts_code', axis=1)
        except Exception as err:
            print(type(err), err.args)
            return type(err), err.args

    def updata_top_index_data(self, ts_code):
        '''更新大盘指数每日指标'''
        data = self.get_top_index_data(ts_code)
        assert isinstance(data, pd.DataFrame), u'返回数据格式不正确'
        if len(data) > 0:
            args = {'name':'index_dailybasic', 'key':ts_code, 'data':data}
            return self._save_hash(args)
        else:
            return 'No data', '%s is no data download.'%ts_code
        
    def read_top_index_data(self, ts_code):
        '''从本地读取大盘指数每日指标'''
        return self._read_hash('index_dailybasic', ts_code)
    
    def read_top_index_keys(self):
        '''从本地读取大盘指数每日指标数据key值列表'''
        return self._read_hash_keys('index_dailybasic')
    
    # 公募基金相关
    
