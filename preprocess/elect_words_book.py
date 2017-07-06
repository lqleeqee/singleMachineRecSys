#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-18 10:02:55
import os
import sys
import inspect
import json
import time
import commands
import multiprocessing
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from ConfigParser import SafeConfigParser

from tools.wordset import WordSet
from models.checkDict import CheckDict
from elect_words_user import ElectWordsUser
from segment_book import SegmentBook
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class ElectWordsBook(ElectWordsUser):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        ElectWordsUser.__init__(self,*args,**kwargs)
        self.book_infn = '%s/data/segmentation/%s' % (self.root,'500W.book.seg')

    def output(self):
        book_outfn = '%s/data/electWords/%s' % (self.root,os.path.basename(self.book_infn))
        return luigi.LocalTarget(book_outfn)

    def requires(self):
        return [SegmentBook(self.conf)]

    def run(self):
        super(ElectWordsBook,self).split_to_multi(self.book_infn,self.output().fn,self.wordset)

if __name__ == "__main__":
    luigi.run()
