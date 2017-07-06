#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-17 14:38:04
import os
import sys
import inspect
import json
import multiprocessing
import time
import shutil
import commands
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from ConfigParser import SafeConfigParser
from infer_user import InferenceUser
from preprocess.filter_with_elect_words import FilterWithElectWords
from preprocess.elect_words_book import ElectWordsBook

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class InferenceBook(InferenceUser):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        InferenceUser.__init__(self,*args,**kwargs)
        self.book_outfn = '%s/data/infer/%s' % (self.root,'book.topic')

    def output(self):
        return luigi.LocalTarget(self.book_outfn)

    def requires(self):
        return [ElectWordsBook(self.conf)]

    def run(self):
        super(InferenceBook,self).split_multiprocess(self.input()[0].fn,self.output().fn)

if __name__ == "__main__":
    luigi.run()
