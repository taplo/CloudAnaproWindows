# -*- coding: utf-8 -*-
"""
2019-02-16 首次编辑
因为X1的笔记本只有4G内存，因此在使用Redis的时候发生过多次的内存溢出。
而且，随着pro版数据的使用，数据库内存储的数据也越来越多，必然导致最终数据库内容超过4G，而在笔记本上无法使用。
在比较了多种Key-Value数据库后一直找不到理想的基于磁盘存储的KV数据库。因为我对数据库的要求就是简单，越简单越好。
所以，尝试用SQLite数据库（因为小巧轻便）为基础，使用类进行封装，将使用过程封装到和Redis一样的接口，从而实现无差别、无缝的使用。

整体来说，redis是未来的方向，所以总体上还是要以redis为主，将来的电脑内存够大的话，必然还是要继续使用redis的，这一方案只是临时方案。当然，将来也可能会遇到需要用这个方法的地方，那么这次努力就赚了！

---------------------------------------
2023-4-24
在使用了几年、停用了几年之后，反复权衡，还是认为这个程序的方案是最合适的。
本次更新的目的：
1、因为引入了docker技术，将来要部署到云上去，所以代码中的很多细节将向linux主机调整。
2、因为学习了sqlite中的内存数据库技术，思考是否可以工作时使用内存数据库，最终写入文件数据库。


@author: Administrator
"""

import sqlite3 as sql
from sqlite3 import Error
# from wttools.wtpickle import dumps, loads

# 建立连接函数
def ConnectionPool(path='/workdir/default.db'):
    '''
    仿照redis的操作，建立连接池的方法
    返回的是数据库连接句柄
    '''
    try:
        return sql.connect(path)
    except Error as err:
        print((type(err), err.args))

# 建立操作实例函数
def StrictRedis(connection_pool):
    '''
    仿照redis的操作，建立数据库操作句柄
    实际返回的是sqlite3的cursor对象
    '''
    try:
        return LiteKv(connection_pool)
    except Error as err:
        print((type(err), err.args))

class LiteKv():
    '''
    使用SQLite实现一个key-value数据库，使用方法与Redis相同
    '''
    # 私有属性
    __path = None
    __connection = None
    __cursor = None
    __string_table = 'list'
    __hash_table = 'hash'
    __tables = []

    @property
    def __total_changes(self):
        return self.__connection.total_changes
    
    # 公有属性
    @property
    def tables(self):
        return self.__tables
    
    # 用于模拟连接池的断开操作
    class cp():
        connection_pool = None
        def __init__(self, connectionpool):
            self.connection_pool = connectionpool
        def disconnect(self):
            if self.connection_pool.total_changes>0:
                self.connection_pool.commit()
            self.connection_pool.close()
    
    @property
    def connection_pool(self):
        return litekv.cp(self.__connection)
        

    # 类方法
    #def __init__(self, datafile='c:\\sqlite\\default.db'):
    def __init__(self, connection):
        '''
        connection:sqlite的连接
        '''
        self.__connection = connection
        self.__cursor = self.__connection.cursor()
        self.__tables = self.__exist_tables()
        if len(self.__tables)==0:
            self.__prepare_tables()
        self.__tables = self.__exist_tables()

    def __del__(self):
        try:
            self.__connection.commit()
            # self.__connection.close()
        except:
            return

    # 私有方法
    def __run(self, cmd):
        '''
        执行命令函数
        cmd为提交给execute函数的tuple
        元素0为指令，元素1为参数tuple
        '''
        try:
            res = self.__cursor.execute(*cmd)
            # self.save() # 在这里我很纠结。。。会很大程度的降低效率
            # 但是如果中间不保存的话，可能导致缓存占用大量内存！
            if self.__connection.total_changes>5:
                self.save()

            '''
            # 判定是操作语句还是查询语句（语句以select开头），如果是查询语句则直接返回代码运行结果；
            # 如果不是查询语句，如果返回空列表，则返回True，否则直接返回返回值。
            check_str = cmd[0]
            #if "select" in check_str.lower():
            if check_str.lower().find('select')==0:
                return res.fetchall()
            else:
                result = res.fetchall()
                if isinstance(result,list) and len(result)==0:
                    return True
                else:
                    return result
            '''
            return res.fetchall()
        except Error as err:
            print(type(err), err, cmd)
            raise err

    '''
        except Exception as err:
            print((type(err), err.args, cmd))
    '''

    def __prepare_tables(self):
        '''
        准备为了封装而使用的数据表
        list为string类型数据表
        hash为hash类型的数据表
        '''
        cmd = ("""CREATE TABLE
        IF NOT EXISTS
        list (name char(50) PRIMARY KEY NOT NULL,
        data blob);""", )
        list_res = self.__run(cmd)
        
        cmd = ("CREATE UNIQUE INDEX IF NOT EXISTS listindex ON list(name);",)
        list_index_res = self.__run(cmd)
        
        cmd = ("""CREATE TABLE
        IF NOT EXISTS
        hash (name char(50) not null,
        key char(50) not null,
        data blob, 
        primary key (name, key));""",)
        hash_res = self.__run(cmd)
        
        cmd = ("""CREATE UNIQUE INDEX
        IF NOT EXISTS
        hashindex ON 
        hash(
        name asc,
        key asc);""",)
        hash_index_res = self.__run(cmd)
        
        self.save()

    def __exist_tables(self):
        cmd = ('select name from sqlite_master where type="table"',)
        return self.__run(cmd)
    
    # 公有方法
    def save(self):
        self.__connection.commit()
    
    def keys(self):
        '''
        返回所有的key值列表
        '''
        list_cmd = ("select distinct name from list;",)
        list_keys = [l[0] for l in self.__run(list_cmd)]
        
        hash_cmd = ("select distinct name from hash;",)
        hash_keys = [h[0] for h in self.__run(hash_cmd)]
        
        return "list_type:\n" + str(list_keys) + "\nhash_type:\n" + str(hash_keys)
    
    # string类型方法
    def get(self, name):
        '''
        string类型读取
        '''
        cmd = ("select data from list where name=?;",(name,))
        res = self.__run(cmd)
        if len(res)>0:
            return res[0][0]
        else:
            return res
    
    def set(self, name, data):
        '''
        string类型保存
        '''
        ''' 使用此方案速度过慢
        cmd = ("""insert or replace into
        list (name, data)
        values (?, ?);
        """, (name, buffer(data)))
        return self.__run(cmd)
        '''
        '''
        redis中，list类型的添加和更新成功都返回True
        sqlite3中，添加和更新成功都返回空

        '''
        # 先查看是否有记录
        result = False
        check = self.get(name)
        if len(check)>0:
            # 更新
            cmd = ("""update list
            set data=?
            where name=?
            """, (memoryview(data), name))
            res = self.__run(cmd)
            if res == []:
                results = True
            else:
                result = res
        else:
            # 插入
            cmd = ("""insert into
            list (name, data)
            values (?, ?);
            """, (name, memoryview(data)))
            res = self.__run(cmd)
            if res == []:
                result = True
            else:
                result = res

        return result
    
    # hash类型方法
    def hkeys(self, name):
        '''
        返回hash的name下的key值列表
        '''
        cmd = ("""select key from hash
        where name=?;""", (name,))
        return [l[0] for l in self.__run(cmd)]
    
    
    def hget(self, name, key):
        '''
        hash类型读取
        '''
        cmd = ("""select data
        from hash
        where name=?
        and key=?""", (name, key))
        res = self.__run(cmd)
        if len(res)>0:
            return res[0][0]
        else:
            return res
    
    def hset(self, name, key, data):
        '''
        hash类型单条写入
        '''
        ''' 使用此方案速度过慢
        cmd = ("""insert or replace into
        hash (name, key, data)
        values (?, ?, ?)
        """, (name, key, buffer(data)))
        return self.__run(cmd)
    
        '''
        '''
        redis中，hash的加入成功返回1，更新成功返回0
        sqlite中，加入和更新成功都返回空列表
        '''
        # 先检查是否存在
        check = self.hget(name, key)
        if len(check)>0:
            # 更新
            cmd = ("""update hash
            set data=?
            where name=?
            and key=?
            """, (memoryview(data), name, key))
            res = self.__run(cmd)
            if res == []:
                result = 0
            else:
                result = res
        else:
            # 插入
            cmd = ("""insert into 
            hash (name, key, data)
            values (?, ?, ?)
            """, (name, key, memoryview(data)))
            res = self.__run(cmd)
            if res == []:
                result = 1
            else:
                result = res

        return result
        
    def hmset(self, name, args):
        '''
        hash类型批量写入
        args为{key,data}对应字典格式
        '''
        # 循环处理
        result = []
        for key, data in args.items():
            result.append(self.hset(name, key, data))
        return result
