#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-15 15:21:20
import os
import sys
import inspect
import shutil
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
import luigi
from decomposit.decomposit_user import DecompositUser
from annoy import AnnoyIndex
from tools.corpus import FeaCorpus
from gensim import corpora, models, similarities
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


from index_book import IndexBook

class IndexUser(IndexBook):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        IndexBook.__init__(self,*args,**kwargs)
        self.index = '%s/data/index/user/user.topic.index' % self.root
        self.ids = '%s/data/index/user/user.id' % self.root

    def requires(self):
        return [DecompositUser(self.conf)]

    def run(self):
        super(IndexUser,self).run()

if __name__ == "__main__":
    luigi.run()

