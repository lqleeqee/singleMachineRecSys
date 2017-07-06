#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-10 09:45:51

import os
import sys
import inspect
import json
import multiprocessing
import time
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from ConfigParser import SafeConfigParser
from preprocess.filter_with_elect_words import FilterWithElectWords


class Inference(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.user_outfn = '%s/data/infer/%s' % (self.root,'user.infer')
        self.book_outfn = '%s/data/infer/%s' % (self.root,'book.infer')
        self.inffn = '%s/models/plda/infer' % self.root
        self.mfn = '%s/models/data/%s' % (self.root,parser.get('plda','model_fn'))
        self.topic_num = parser.getint('plda', 'topic_num')
        self.alp = 50.0 /self.topic_num
        self.biit = parser.getint('plda', 'infer_burn_in_iter')
        self.tit = parser.getint('plda', 'infer_total_iter')

    def output(self):
        return {'user':luigi.LocalTarget(self.user_outfn),
                'book':luigi.LocalTarget(self.book_outfn)}

    def requires(self):
        return [FilterWithElectWords(self.conf)]

    def run(self):
        import shutil
        try:
            if not os.path.exists(os.path.dirname(self.user_outfn)): 
                os.makedirs(os.path.dirname(self.user_outfn))
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(os.path.dirname(self.book_outfn)):
                pass
            else: raise

        user_infn = self.input()[0]['user'].fn
        book_infn = self.input()[0]['book'].fn

        p1 = multiprocessing.Process(target = self.inference, args = (user_infn,self.user_outfn,))
        p2 = multiprocessing.Process(target = self.inference, args = (book_infn,self.book_outfn,))
        p1.start()
        p2.start()
        p1.join()
        p2.join()

        #self.inference(in_fn=user_infn,out_fn=self.user_outfn)
        #self.inference(in_fn=book_infn,out_fn=self.book_outfn)

        

    def inference(self,in_fn=None,out_fn=None):
        #inference doc and user
        inf = self.inffn
        aph = self.alp
        m_fn = self.mfn
        bii = self.biit
        ti = self.tit

        if in_fn is None or out_fn is None:
            raise Exception("Input or output not specified:!",'Inference')
        id_fn = '%s.id' % in_fn
        cont_fn = '%s.cont' % in_fn
        cmd_0 = "awk -F '\t' '{print $1}' %s > %s" % (in_fn,id_fn)
        cmd_1 = "awk -F '\t' '{print $2}' %s > %s" % (in_fn,cont_fn)
        os.system(cmd_0)
        os.system(cmd_1)

        cmd = '''%s --alpha %s --beta 0.01 \
        --inference_data_file %s \
        --inference_result_file %s \
        --model_file %s \
        --total_iterations %s \
        --burn_in_iterations %s\
        '''

        tmp_outfn = '%s.tmp' % out_fn
        cmd = cmd % (inf,aph,cont_fn,tmp_outfn,m_fn,ti,bii)
        os.system(cmd)

        cmd_2 = "paste %s %s > %s" % (id_fn,tmp_outfn,out_fn)
        os.system(cmd_2)


if __name__ == "__main__":
    luigi.run()	
