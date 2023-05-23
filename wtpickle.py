# -*- coding: utf-8 -*-
"""
Created on 2019-02-03 
为了在redis中更节省空间的保存数据，自行用cloudpickle和zlib封装了一个Pickle类
@author: Administrator
"""

""" 旧版内容
from cloudpickle import dumps as du
from pickle import loads as lo
from zlib import compress, decompress

def dumps(data, level=3):
    '''持久化压缩函数'''
    return compress(du(data), level)

def loads(zdata):
    '''持久化解压缩函数'''
    try:
        return lo(decompress(zdata))
    except:
        return lo(decompress(zdata), encoding='iso-8859-1')


class WtPickle():
    '''
    利用pickle、cloudpickle和zlib进行对象/数据压缩持久化的类\n
    默认level=3，为性能优先的压缩比选择
    '''
    __level = 3
    def __init__(self, level=3):
        self.__level = level
        pass
    
    def dumps(self, data):
        '''持久化压缩函数'''
        return compress(du(data), self.__level)
    
    def loads(self, zdata):
        '''持久化解压缩函数'''
        return lo(decompress(zdata))
"""
# 新版压缩解压方法
import pickle
import zlib
import lzma

def dumps(data, level=3):
    return lzma.compress(pickle.dumps(data, pickle.HIGHEST_PROTOCOL), preset=level)

def loads(data):
    # 判定是否为新格式
    check_bytes = b'\xfd7zXZ\x00\x00\x04\xe6\xd6\xb4F\x02\x00!'
    if check_bytes==data[:15]: # 新格式
        result = pickle.loads(lzma.decompress(data))
    else:
        try:
            result = pickle.loads(zlib.decompress(data))
        except:
            result = pickle.loads(zlib.decompress(data), encoding='iso-8859-1')
    return result
