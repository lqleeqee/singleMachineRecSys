#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-15 11:11:56
import os
import sys
import inspect
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
import luigi
from tools.corpus import FeaCorpus
from sklearn.externals import joblib
from infer.infer_user import InferenceUser
from models.svdModel import ILibSVD
from tools.matrix import *

class DecompositUser(luigi.Task):
    conf = luigi.Parameter()
        
    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        root = parser.get("basic", "root")
        self.topic_num = parser.getint("plda", "topic_num")
        self.user_decompisit = '%s/data/decomposit/user/user.dec' % root  #books`s

    def requires(self):
        return [InferenceUser(self.conf), ILibSVD(self.conf)]   

    def output(self):
        return luigi.LocalTarget(self.user_decompisit) 

    def run(self):
        if not os.path.exists(os.path.dirname(self.output().fn)):
             os.makedirs(os.path.dirname(self.output().fn))
        model = joblib.load(self.input()[1].fn)
        fea_corpus = FeaCorpus(self.input()[0].fn,sparse=True)
        ids = [id for id in FeaCorpus(self.input()[0].fn, onlyID=True)]
        #X = load_dense_matrix(fea_corpus,len(ids),self.topic_num)
        X = load_csr_matrix(fea_corpus,self.topic_num)
        Y = model.transform(X)
        with self.output().open('w') as out_fd:
            print_dense_matrix(out_fd, ids, Y)  
        
if __name__ == "__main__":
    luigi.run()
