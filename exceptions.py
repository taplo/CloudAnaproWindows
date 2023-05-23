# -*- coding: utf-8 -*-
"""
Created on Sat Jul 9 23:41:46 2022
异常处理工具
@author: Administrator
"""

import traceback
import colorama

tb = False
#logger = None


def try_run(func):
    global logger

    def real_run(*args, **keyargs):
        global logger
        try:
            return func(*args, **keyargs)
        except Exception as err:
            print(colorama.Fore.RED, 'Error execute: %s, %s' %
                  (func.__name__, ' '.join(map(str, err.args))), colorama.Fore.RESET)

            if tb == True:
                traceback.print_exc()

    return real_run
