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
import shutil
import commands
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from ConfigParser import SafeConfigParser
from preprocess.filter_with_elect_words import FilterWithElectWords

import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class Inference(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.user_outfn = '%s/data/infer/%s' % (self.root,'user.topic')
        self.book_outfn = '%s/data/infer/%s' % (self.root,'book.topic')
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
        #import shutil
        try:
            if not os.path.exists(os.path.dirname(self.user_outfn)): 
                os.makedirs(os.path.dirname(self.user_outfn))
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(os.path.dirname(self.book_outfn)):
                pass
            else: raise
        
        user_infn = self.input()[0]['user'].fn
        book_infn = self.input()[0]['book'].fn
        #分割文件？$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        cpu_num = multiprocessing.cpu_count() #获取CPU个数

        #获取用户和文件个数，并按比例分配CPU计算资源，如果某个类型没有分配到CPU，则强制分配一个CPU
        cmd_get_line_num = 'wc -l %s'
        #import commands
        cmd_get_user = cmd_get_line_num % user_infn 
        cmd_get_book = cmd_get_line_num % book_infn 
        user_line_num = float(commands.getstatusoutput(cmd_get_user)[1].split(' ')[0])
        book_line_num = float(commands.getstatusoutput(cmd_get_book)[1].split(' ')[0])
        #按文件大小比例分配CPU
        cpu_user = int((cpu_num-1) * user_line_num/(user_line_num + book_line_num))
        cpu_book = int((cpu_num-1) * book_line_num/(user_line_num + book_line_num))
        #强制分配CPU
        if cpu_user <= 0 :
            cpu_user = 1
            cpu_book = cpu_num - 2
        if cpu_book <=0 :
            cpu_book = 1
            cpu_user = cpu_num - 2

        #创建临时存储分片输入输出的文件夹
        user_infn_splits = '%s/user' % os.path.dirname(user_infn)
        book_infn_splits = '%s/book' % os.path.dirname(book_infn)
        user_outfn_splits = '%s/user' % os.path.dirname(self.user_outfn)
        book_outfn_splits = '%s/book' % os.path.dirname(self.book_outfn)
        if os.path.exists(user_infn_splits):
            shutil.rmtree(user_infn_splits)
            print('Creat folder:\t%s' % user_infn_splits)
            os.mkdir(user_infn_splits)
        else:os.mkdir(user_infn_splits)

        if os.path.exists(book_infn_splits):
            shutil.rmtree(book_infn_splits)
            os.mkdir(book_infn_splits)
        else:os.mkdir(book_infn_splits)

        if os.path.exists(user_outfn_splits):
            shutil.rmtree(user_outfn_splits)
            os.mkdir(user_outfn_splits)
        else:os.mkdir(user_outfn_splits)

        if os.path.exists(book_outfn_splits):
            shutil.rmtree(book_outfn_splits)
            os.mkdir(book_outfn_splits)
        else:os.mkdir(book_outfn_splits)
        
        #分割即将被infer的文件
        cmd_split = '''cd %s && cat %s | awk \'{print $0 > (\"./%s.part_\"int(NR%%%s))}\' && cd -'''
        cmd_split_user = cmd_split % (user_infn_splits,user_infn,os.path.basename(user_infn),str(cpu_user))
        cmd_split_book = cmd_split % (book_infn_splits,book_infn,os.path.basename(book_infn),str(cpu_book))
        #分割用户文件
        
        if cpu_user == 1:
            shutil.copy2(user_infn,user_infn_splits)
        else:
            ##import subproces
            #print('split user eletc seg:\n%s' % cmd_split_user)
            return_code = commands.getstatusoutput(cmd_split_user)
            if return_code[0] != 0:
                print(return_code[1])
                raise Exception('failed to split user elect seg:','Inference')

        #分割图书资源文件
        if cpu_book == 1:
            shutil.copy2(book_infn,book_infn_splits)
        else:
            #print('split book eletc seg:\n%s' % cmd_split_book)
            return_code = commands.getstatusoutput(cmd_split_book)[0]
            if return_code != 0:
                raise Exception('failed to split book elect seg:','Inference')
        time.sleep(30)
        print('Split files finished!')
        #获取分片后的文件名
        user_splits_fns = os.listdir(user_infn_splits)
        print(user_splits_fns)
        book_splits_fns = os.listdir(book_infn_splits)
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        record = []
        for ufn in user_splits_fns:
            ufn = '%s/%s' % (user_infn_splits,ufn)
            ofn = '%s/%s.%s' % (user_outfn_splits,os.path.basename(ufn),'infer')
            process = multiprocessing.Process(target=self.inference,args=(ufn,ofn))
            process.start()
            record.append(process)

        for bfn in book_splits_fns:
            bfn = '%s/%s' % (book_infn_splits,bfn)
            ofn = '%s/%s.%s' % (book_outfn_splits,os.path.basename(bfn),'infer')
            process = multiprocessing.Process(target=self.inference,args=(bfn,ofn))
            process.start()
            record.append(process)

        for process in record:
            process.join()

        #test
        #user_outfn_splits = '%s/user' % os.path.dirname(self.user_outfn)
        #book_outfn_splits = '%s/book' % os.path.dirname(self.book_outfn)

        cmd_merge = "cd %s && cat %s > %s && cd -"
        cmd_merge_user = cmd_merge % (user_outfn_splits,'./*.infer',self.user_outfn)
        cmd_merge_book = cmd_merge % (book_outfn_splits,'./*.infer',self.book_outfn)
        #print(cmd_merge_user)
        os.system(cmd_merge_user)
        os.system(cmd_merge_book)

        # #按类型分两类两个进程并行模式
        # p_user = multiprocessing.Process(target = self.inference, args = (user_infn,self.user_outfn,))
        # p_book = multiprocessing.Process(target = self.inference, args = (book_infn,self.book_outfn,))
        # p_user.start()
        # p_book.start()
        # p_user.join()
        # p_book.join()

        # #顺序执行模式
        # self.inference(in_fn=user_infn,out_fn=self.user_outfn)
        # self.inference(in_fn=book_infn,out_fn=self.book_outfn)

        

    def inference(self,in_fn=None,out_fn=None):
        #inference doc and user
        inf,aph = self.inffn,self.alp
        m_fn,bii,ti = self.mfn,self.biit,self.tit

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
        #删除中间文件
        # os.remove(id_fn)
        # os.remove(cont_fn)
        # os.remove(tmp_outfn)


if __name__ == "__main__":
    luigi.run()	
