#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-07-05 10:49:00
import os
import sys
import time
from functools import wraps

def fn_timer(function):  
    @wraps(function)  
    def function_timer(self,*args, **kwargs):  
        t0 = time.time()  
        result = function(self,*args, **kwargs)  
        t1 = time.time()  
        print ("Total time running %s: %s seconds" %  
               (function.func_name, str(t1-t0))  
               )  
        return result  
    return function_timer
