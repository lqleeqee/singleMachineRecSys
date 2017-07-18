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
from preprocess.elect_words_user import ElectWordsUser
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


class InferenceUser(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.user_outfn = '%s/data/infer/%s' % (self.root,'user.topic')
       # self.book_outfn = '%s/data/infer/%s' % (self.root,'book.topic')
        self.inffn = '%s/models/plda/infer' % self.root
        self.mfn = '%s/models/data/%s' % (self.root,parser.get('plda','model_fn'))
        self.topic_num = parser.getint('plda', 'topic_num')
        self.alp = 50.0 /self.topic_num
        self.biit = parser.getint('plda', 'infer_burn_in_iter')
        self.tit = parser.getint('plda', 'infer_total_iter')

    def output(self):
        return luigi.LocalTarget(self.user_outfn)

    def requires(self):
        #return [FilterWithElectWords(self.conf)]
        return [ElectWordsUser(self.conf)]

    def run(self):
        self.split_multiprocess(self.input()[0].fn,self.output().fn)

    def split_multiprocess(self,in_fn=None,out_fn=None):
        '''把in_fn切分为几个块，多进程执行……
            64位系统，行数不超过9223372036854775807,
            32位系统，行数不超过2147483647'''
        if in_fn is None or out_fn is None:
            raise Exception("没有指定输入或输出文件:!",'Inference')
        infn,outfn = in_fn,out_fn
        try:
            if not os.path.exists(os.path.dirname(outfn)): 
                os.makedirs(os.path.dirname(outfn))
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(os.path.dirname(outfn)):
                pass
            else: raise
        #分割文件？$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        cpu_num = multiprocessing.cpu_count()-1 #获取CPU个数,保留一个CPU
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
        cmd_split = '''cd %s && cat %s | awk \'{print $0 > (\"%s.part_\"int(NR%%%s))}\' && cd -'''
        cmd_split_file = cmd_split % (infn_splits,infn,os.path.basename(infn),str(cpu_num))
        #分割用户文件
        if line_num <= 10**3:
            shutil.copy2(user_infn,infn_splits)
        else:
            print('split user eletc seg:%s' % cmd_split_file)
            return_code = commands.getstatusoutput(cmd_split_file)[0]
            if return_code != 0:
                raise Exception('failed to split user elect seg:','Inference')
        #获取分片后的文件名
        splits_fns = ['%s/%s' % (infn_splits,ufn) for ufn in os.listdir(infn_splits)]
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        record = []
        for ufn in splits_fns:
            ofn = '%s/%s.%s' % (outfn_splits,os.path.basename(ufn),'infer')
            process = multiprocessing.Process(target=self.inference,args=(ufn,ofn))
            process.start()
            record.append(process)
        for process in record:
            process.join()

        cmd_merge = "cd %s && cat %s > %s && cd -"
        cmd_merge_infer = cmd_merge % (outfn_splits,'./*.infer',outfn)
        os.system(cmd_merge_infer)
        #删除中间结果文件
        shutil.rmtree(infn_splits)
        shutil.rmtree(outfn_splits)

    def inference(self,in_fn=None,out_fn=None):
        #inference doc and user
        inf,aph = self.inffn,self.alp
        m_fn,bii,ti = self.mfn,self.biit,self.tit

        if in_fn is None or out_fn is None:
            raise Exception("Input or output not specified:!",'Inference')
        id_fn = '%s.id' % in_fn
        cont_fn = '%s.cont' % in_fn
        print('infer %s...... outfile:%s\n' % (in_fn,out_fn))
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
        #输出到稠密矩阵文件
        tmp_dense = '%s.dense' % out_fn
        cmd_2 = "paste %s %s > %s" % (id_fn,tmp_outfn,tmp_dense)
        os.system(cmd_2)
        #转换为稀疏表示
        self.den_spa(tmp_dense,out_fn)
        #删除中间文件)
        os.remove(id_fn)
        os.remove(cont_fn)
        os.remove(tmp_outfn)
        os.remove(tmp_dense)

    def den_spa(self,infilename,outfilename):
        in_fn = infilename
        out_fn = outfilename
        with open(in_fn,'r')as infn,open(out_fn,'w')as outfn:
            for line in infn:
                line = line.strip()
                items = [i.strip() for i in line.split('\t') if not i.isspace()]
                if len(items) != 2:
                    #output to error log file
                    continue
                i_id = items[0]
                cont = items[1]
                out_cont = []
                content = cont.split(' ')
                total_i = len(content)
                for i in range(total_i):
                    if content[i] != '0':
                        out_cont.append('%s:%s'%(str(i),content[i]))
                if len(out_cont) > 0:
                    ot_string = ' '.join(out_cont)
                    ot_string = ot_string.strip()
                    outfn.write('%s\t%s\n' % (i_id,ot_string))
                else:
                    #output to error log file
                    continue

if __name__ == "__main__":
    luigi.run()	
