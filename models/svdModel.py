#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-15 09:08:45
import os
import sys
import inspect
import shutil
import csv
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from infer.infer import Inference
from tools.corpus import FeaCorpus
from sklearn.decomposition import TruncatedSVD
from sklearn.externals import joblib
import luigi

class ILibSVD(luigi.Task):
    conf = luigi.Parameter()
        
    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.svd_model = '%s/models/data/paper.svd.model/svd.model/svd.model' % self.root
        self.sample_fraction = parser.getfloat('svd', 'sample_fraction')
        self.n_components = parser.getint('svd', 'n_components')
        self.sampled_doc = '%s/data/svd.tmp/paper.topic.sampled' % self.root
        self.topic_num = parser.getint('plda', 'topic_num')
        

    def requires(self):
        return [Inference(self.conf)]

    def output(self):
        return luigi.LocalTarget(self.svd_model)
    
    def run(self):
        pass
        # model_dir = os.path.dirname(self.svd_model)
        # if os.path.exists(model_dir):
        #     shutil.rmtree(model_dir)
        # os.mkdir(model_dir)

        # df = sf.SFrame.read_csv(self.input()[0]['book'].fn,
        #     column_type_hints=[str, str],
        #     delimiter='\t', header=False)
        # df = df.sample(self.sample_fraction)    
        # df.export_csv(self.sampled_doc, delimiter="\t", quote_level=csv.QUOTE_NONE, header=False)

        # try:
        #     fea_corpus = FeaCorpus(self.sampled_doc)    
        #     X = load_csr_matrix(fea_corpus, self.topic_num)
        #     model = TruncatedSVD(n_components=self.n_components)    
        #     model.fit(X)
        #     joblib.dump(model, self.output().fn)
        # except ValueError,e:
        #     print e
        #     pass
        # #os.remove(self.sampled_doc)

if __name__ == "__main__":
    luigi.run()
