#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-09 11:17:25
import os
import sys
import inspect
import json
import time
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


class FilterWithElectWords(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.user_infn = '%s/data/segmentation/%s' % (self.root,'user.seg')
        self.book_infn = '%s/data/segmentation/%s' % (self.root,'book.seg')

    def output(self):
        user_outfn = '%s/data/electWords/%s' % (self.root,os.path.basename(self.user_infn))
        book_outfn = '%s/data/electWords/%s' % (self.root,os.path.basename(self.book_infn))
        return {'user':luigi.LocalTarget(user_outfn),
                'book':luigi.LocalTarget(book_outfn)}

    def requires(self):
        return []
        #return [CheckDict(self.conf)]

    def run(self):
        local_dict = '%s/models/data/paper.sampled.dict' % self.root
        self.wordset = WordSet(local_dict)
        p1 = multiprocessing.Process(target = self.elect_words, args = (self.user_infn,self.output()['user'].fn,))
        p2 = multiprocessing.Process(target = self.elect_words, args = (self.book_infn,self.output()['book'].fn,))
        p1.start()
        p2.start()
        p1.join()
        p2.join()
        #self.elect_words(self.user_infn,self.output()['user'].fn)
        #self.elect_words(self.book_infn,self.output()['book'].fn)

    def elect_words(self,infn,outfn):
        outf = open(outfn,'w')
        with open(infn,'r') as inf:
            for line in inf:
                items = [item.strip() for item in line.split('\t')]
                bows = self.wordset.filter_bows(items[1])
                bows = bows.strip()
                uid = items[0]
                if len(items) != 2 or len(bows.strip()) == 0 or len(uid.strip()) == 0:
                    print(line)
                    #time.sleep(1)
                    continue
                outf.write(uid+'\t'+bows+'\n')
        outf.close()

if __name__ == "__main__":
    luigi.run()	

