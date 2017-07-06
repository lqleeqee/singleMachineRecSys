#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-16 09:13:13
import os
import sys
import inspect
import csv
import uuid
import json
from shutil import copyfile
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from tools.recommend import recommend
from decomposit.decomposit_book import DecompositBook
from decomposit.decomposit_user import DecompositUser
from index.index_book import IndexBook
from index.index_user import IndexUser

from ConfigParser import SafeConfigParser
import luigi
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


class UserRecBooks(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
            luigi.Task.__init__(self, *args, **kwargs)
            parser = SafeConfigParser()
            parser.read(self.conf)
            root = parser.get("basic", "root")
            self.batch = parser.getint("rec", "batch")
            self.threshold = parser.getfloat("rec", "threshold")
            self.thread_num = parser.getint("rec", "cpu_core_num")
            self.n_components = parser.getint('svd', 'n_components')
            self.topk = parser.getint("rec", "topk")
            self.rec = '%s/data/rec/userRecBooks.rec' % root
            #self.uid_rec = '%s/data/user/user2users.rec.uid' % root

        def requires(self):
            return [DecompositUser(self.conf), IndexBook(self.conf)]

        def output(self):
            return luigi.LocalTarget(self.rec)

        def run(self):
            rec_dir = os.path.dirname(self.output().fn)
            if not os.path.isdir(rec_dir):
                os.makedirs(rec_dir)
            index_dir = os.path.dirname(self.input()[1]['index'].fn)
            with self.output().open('w') as out_fd:
                recommend(out_fd, 
                    self.input()[0].fn,
                    self.input()[1]['ids'].fn, 
                    self.input()[1]['index'].fn,
                    self.n_components,
                    self.topk, 
                    self.batch, 
                    self.threshold,
                    self.thread_num,
                    isUser=True,
                    isRecUser=True)

if __name__ == "__main__":
    luigi.run()
