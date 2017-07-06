#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-06-06 09:46:50
import os
import sys
import inspect
import json
import time
import commands
import shutil
import multiprocessing
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from ConfigParser import SafeConfigParser
from segment_user import SegmentUser
import jieba
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class SegmentBook(SegmentUser):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        SegmentUser.__init__(self, *args, **kwargs)
        self.infile = '%s/data/segmentation/%s' % (self.root,'500W.book.txt')
        self.outfile = '%s/data/segmentation/%s' % (self.root,'500W.book.seg')
        self.errfile = '%s/data/segmentation/%s' % (self.root,'500W.book.err')

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def requires(self):
        return []

    def run(self):
        super(SegmentBook,self).handle_core(self.infile,self.outfile,self.errfile)

if __name__ == "__main__":
    luigi.run()
