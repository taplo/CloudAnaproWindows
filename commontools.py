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

"""
调整字符串长度的方法。
输入字符串和一个整数，将输入字符串长度调整为整数指定的长度，用空格补全。
如果字符串长度超过指定长度则截断，并在末尾加入...字符。
"""
def adjust_string_length(string, length):
    if len(string) < length:
        string += ' ' * (length - len(string))
    elif len(string) > length:
        string = string[:length-3] + '...'
    return string
