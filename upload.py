# -*- coding: utf-8 -*-
"""
Created on 2023-05-19 09:10:00
通过阿里云上传数据库文件
@author: Administrator
"""
import os
import datetime
from aligo import Aligo, EMailConfig


def upload_file():
    email_config = EMailConfig(email='taplo@qq.com', user='wtdwang@189.cn', password='Re=2Pd)6x#0Xj!8D',
                           host='smtp.189.cn', port=25)
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