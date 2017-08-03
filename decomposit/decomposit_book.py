#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-15 11:11:56
import os
import sys
import inspect
import multiprocessing
import commands
import shutil
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
import luigi
from tools.corpus import FeaCorpus
from sklearn.externals import joblib
from infer.infer import Inference
from infer.infer_book import InferenceBook
from models.svdModel import ILibSVD
from tools.matrix import *


def decomposit_split(in_fn,out_fn,model_fn,topic_num):
    model = model_fn
    fea_corpus = FeaCorpus(in_fn,sparse=True)
    ids = [id for id in FeaCorpus(in_fn, onlyID=True)]
    X = load_csr_matrix(fea_corpus,topic_num)
    Y = model.transform(X)
    with open(out_fn,'w') as out_fd:
        print_dense_matrix(out_fd, ids, Y)

class DecompositBook(luigi.Task):
    conf = luigi.Parameter()
        
    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        root = parser.get("basic", "root")
        self.topic_num = parser.getint("plda", "topic_num")
        self.books_decompisit = '%s/data/decomposit/book/book.dec' % root  #books`s

    def requires(self):
        return [InferenceBook(self.conf), ILibSVD(self.conf)]   

    def output(self):
        return luigi.LocalTarget(self.books_decompisit) 

    def run(self):
        model = joblib.load(self.input()[1].fn)
        self.split_multiprocess(self.input()[0].fn,self.output().fn,model,self.topic_num)
        # fea_corpus = FeaCorpus(self.input()[0].fn,sparse=True)
        # ids = [id for id in FeaCorpus(self.input()[0].fn, onlyID=True)]
        # X = load_csr_matrix(fea_corpus,self.topic_num)
        # Y = model.transform(X)
        # with self.output().open('w') as out_fd:
        #     print_dense_matrix(out_fd, ids, Y) 


    def split_multiprocess(self,in_fn=None,out_fn=None,model_fn=None,topic_num=None):
        '''把in_fn切分为几个块，多进程执行……
            64位系统，行数不超过9223372036854775807,
            32位系统，行数不超过2147483647'''
        if in_fn is None or out_fn is None:
            raise Exception("没有指定输入或输出文件:!",'DecompositBook')
        if model_fn is None:
            raise Exception("没有指定SVD的模型文件:!",'DecompositBook')
        if topic_num is None:
            raise Exception("没有指定topic个数:!",'DecompositBook')
        infn,outfn = in_fn,out_fn
        try:
            if not os.path.exists(os.path.dirname(outfn)): 
                os.makedirs(os.path.dirname(outfn))
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(os.path.dirname(outfn)):
                pass
            else: raise
        #分割文件？$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        cpu_num = int(multiprocessing.cpu_count()-1) #获取CPU个数,保留一个CPU
        #获取输入文件行数
        cmd_get_line = 'wc -l %s'
        cmd_get_line_num = cmd_get_line % infn  
        line_num = int(commands.getstatusoutput(cmd_get_line_num)[1].split(' ')[0])
        #创建临时存储分片输入输出的文件夹
        infn_splits = '%s/split' % os.path.dirname(infn)
        outfn_splits = '%s/split' % os.path.dirname(outfn)
        if os.path.exists(infn_splits):
            shutil.rmtree(infn_splits)
            os.mkdir(infn_splits)
        else:os.mkdir(infn_splits)
        if os.path.exists(outfn_splits):
            shutil.rmtree(outfn_splits)
            os.mkdir(outfn_splits)
        else:os.mkdir(outfn_splits)
        #分割即将被infer的文件
        num_signle_file = 10**6    #单个文件的行数不超过100W
        file_num =  line_num/num_signle_file+1
        cmd_split = '''cd %s && cat %s | awk \'{print $0 > (\"%s.part_\"int(NR%%%s))}\' && cd -'''
        cmd_split_file = cmd_split % (infn_splits,infn,os.path.basename(infn),str(file_num))
        #分割用户文件
        if line_num <= num_signle_file:
            #100w
            shutil.copy2(user_infn,infn_splits)
        else:
            print('split user eletc seg:%s' % cmd_split_file)
            return_code = commands.getstatusoutput(cmd_split_file)[0]
            if return_code != 0:
                raise Exception('failed to split user elect seg:','Inference')
        #获取分片后的文件名
        splits_fns = ['%s/%s' % (infn_splits,bfn) for bfn in os.listdir(infn_splits)]
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        record = []
        pool = multiprocessing.Pool(processes=cpu_num)
        for bfn in splits_fns:
            ofn = '%s/%s.%s' % (outfn_splits,os.path.basename(bfn),'dec')
            pool.apply(decomposit_split, (bfn,ofn,model_fn,topic_num, ))
        
        pool.close()
        pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束

        cmd_merge = "cd %s && cat %s > %s && cd -"
        cmd_merge_infer = cmd_merge % (outfn_splits,'./*.dec',outfn)
        os.system(cmd_merge_infer)
        #删除中间结果文件
        shutil.rmtree(infn_splits)
        shutil.rmtree(outfn_splits) 

        
if __name__ == "__main__":
    luigi.run()
