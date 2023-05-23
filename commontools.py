# -*- coding: utf-8 -*-
"""
Created on Mon Feb 04 11:24:18 2019

@author: Administrator
"""

from numpy import array

from io import StringIO
from gzip import GzipFile



def split_list(lst, lenth):
    '''按照固定长度拆分列表，返回分段索引。'''
    if lenth>len(lst):
        return [lst]
    else:
        start = array(list(range(0, len(lst), lenth)))
        end = start + lenth - 1
        end[-1] = len(lst) - 1
        pos = list(zip(start, end))
        result = []
        for p in pos:
            result.append(lst[p[0]:p[1]+1])
            
        return result
    
    
def gzip_compress(buf):
    out = StringIO()
    with GzipFile(fileobj=out, mode='w') as f:
        f.write(buf)
    return out.getvalue()

def gzip_decompress(buf):
    obj = StringIO(buf)
    with GzipFile(fileobj=obj) as f:
        result = f.read()
    return result
