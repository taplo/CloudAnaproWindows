# -*- coding: utf-8 -*-
"""
Created on 2023-05-19 09:10:00
通过阿里云上传数据库文件
@author: Administrator
"""
import os
import datetime
import configparser
from aligo import Aligo, EMailConfig


def upload_file():
    conf = configparser.ConfigParser()
    conf.read('../config.ini',encoding="utf-8-sig")
    
    target_email = conf.get('anapro', 'target_email')
    username = conf.get('anapro', 'user')
    password = conf.get('anapro', 'password')
    host = conf.get('anapro', 'host')
    port = int(conf.get('anapro', 'port'))
    
    email_config = EMailConfig(email=target_email, user=username, password=password,
                           host=host, port=port)
    ali = Aligo(email=email_config)
    remote_folder = ali.get_folder_by_path('数据')
    
    # 按照日期复制文件
    filename = datetime.datetime.now().isoformat().split('.')[0] + '.db'
    filename = filename.replace(':', '-')
    cmd = "copy d:\\workdir\\default.db d:\\workdir\\" + filename
    print("复制文件副本...")
    result = os.system(cmd)
    print(result)
    result = ali.upload_file(file_path='d:\\workdir\\'+filename, parent_file_id=remote_folder.file_id)
    print(result)
    result = os.system("del d:\\workdir\\" + filename)
    print(result)

if __name__ == '__main__':

    upload_file()