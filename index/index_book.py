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
from decomposit.decomposit_book import DecompositBook
from annoy import AnnoyIndex
from tools.corpus import FeaCorpus
from gensim import corpora, models, similarities
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class IndexBook(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.topic_num = parser.getint('plda', 'topic_num')
        self.n_components = parser.getint('svd', 'n_components')
        self.index = '%s/data/index/book/book.topic.index' % self.root
        self.ids = '%s/data/index/book/book.id' % self.root

    def requires(self):
        return [DecompositBook(self.conf)]

    def output(self):
        return {"index" : luigi.LocalTarget(self.index),
                "ids" : luigi.LocalTarget(self.ids)}

    def run(self):
        index_dir = os.path.dirname(self.index)
        if not os.path.isdir(index_dir):os.makedirs(index_dir)
        #get ids
        with self.output()['ids'].open('w') as ids_fd:
            corpus = FeaCorpus(self.input()[0].fn, onlyID=True)
            for id in corpus:
                print >> ids_fd, id
        corpus = FeaCorpus(self.input()[0].fn, sparse=False)
        t = AnnoyIndex(self.n_components, metric='angular')
        i = 0
        for v in corpus:
            t.add_item(i, v)
            i += 1
        t.build(int(self.n_components / 2))
        t.save(self.output()['index'].fn)

if __name__ == "__main__":
    luigi.run()

