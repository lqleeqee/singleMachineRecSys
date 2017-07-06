#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-07-05 11:06:02

import os
import sys
import inspect
import json
import time
import shutil
import multiprocessing
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from tools.time_dec import fn_timer
from ConfigParser import SafeConfigParser
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class AssemblyUser(luigi.Task):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.user_history = '%s/%s' % (self.root,parser.get("basic","user_history"))
        self.book_resource = '%s/%s' % (self.root,parser.get("basic","book_resource"))
        self.outfile = '%s/user.txt' % os.path.dirname(self.user_history)
        

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def requires(self):
        return []

    def run(self):
        self.assembly(self.book_resource,self.user_history,self.outfile)

    
    @fn_timer
    def assembly(self,bookfile=None,userhistory=None,outfn=None):
        if not bookfile and not userhistory and not outfn:
            return None
        book_resource = self.read_resource_file(bookfile)
        user_history = self.read_user_fn(userhistory)
        with open(outfn,'w')as ofn,open('%s.err' % outfn,'w')as err:
            for u in user_history:
                descriptions = []
                for r in user_history[u]:
                    descriptions.append(book_resource.get(r,' '))
                descriptions = [i.strip() for i in descriptions if not i.isspace()]
                if descriptions:
                    ofn.write(u+'\t'+' '.join(descriptions)+'\n')
                else:
                    err.write(u+'\t')
                    continue

    @fn_timer
    def read_user_fn(self,user_fn=None):
        '''读取用户日志，按时间排序，取最近的50条日志'''
        if not user_fn:
            return None
        user_history = {}
        with open(user_fn,'r')as ufn,open('%s.err' % user_fn,'w')as err:
            for line in ufn:
                line = line.strip()
                items = [i.strip() for i in line.split('\t')]
                if len(items) != 3:
                    err.write(line+'\n')
                    continue
                user_history.setdefault(items[0],[]).append((items[1],items[2]))
        for u in user_history:
            user_history[u] = [h[0] for h in (sorted(user_history[u],key=lambda x:x[1])[:50])]
        with open('%s.result' % user_fn,'w')as ofn:
            json.dump(user_history,ofn)
        return user_history

    @fn_timer
    def read_resource_file(self,infilename=None):
        '''读取文章等资源，供用户日志组合为用户的文本描述'''
        if not infilename:
            return None
        out_dict = {}
        with open(infilename,'r')as ifd:
            for line in ifd:
                line = line.strip()
                items = [i.strip() for i in line.split('\t')]
                if len(items) != 2:continue
                out_dict[items[0]] = items[1]
        return out_dict



if __name__ == "__main__":
    luigi.run()