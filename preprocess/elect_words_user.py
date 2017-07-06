#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-18 09:27:58
import os
import sys
import inspect
import json
import time
import shutil
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
from segment_user import SegmentUser
from tools.wordset import WordSet
from models.checkDict import CheckDict
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


class ElectWordsUser(luigi.Task):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        #self.user_infn = '%s/data/segmentation/%s' % (self.root,'user.seg')
        local_dict = '%s/models/data/paper.sampled.dict' % self.root
        self.wordset = WordSet(local_dict)

    def requires(self):
        return [SegmentUser(self.conf)]

    def output(self):
        self.user_infn = self.input()[0].fn
        user_outfn = '%s/data/electWords/%s' % (self.root,os.path.basename(self.user_infn))
        return luigi.LocalTarget(user_outfn)

    def run(self):
        #self.user_infn = self.input()[0].fn
        self.split_to_multi(self.user_infn,self.output().fn,self.wordset)

    def split_to_multi(self,infn=None,outfn=None,wordset=None):
        '''
        拆分输入文件，进行并行清洗......
        64位系统，行数不超过9223372036854775807,
        32位系统，行数不超过2147483647
        '''
        if infn is None or outfn is None:
            raise Exception("没有指定输入或输出文件:!",'elect_words_user')
        if wordset is None:
            raise Exception("没有指定筛选词典:!","elect_words_user")
        try:
            if not os.path.exists(os.path.dirname(outfn)): 
                os.makedirs(os.path.dirname(outfn))
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(os.path.dirname(outfn)):
                pass
            else: raise
        #分割文件？->$$$$
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
            ofn = '%s/%s.%s' % (outfn_splits,os.path.basename(ufn),'tmp')
            process = multiprocessing.Process(target=self.elect_words,args=(ufn,ofn,wordset,))
            process.start()
            record.append(process)
        for process in record:
            process.join()

        cmd_merge = "cd %s && cat %s > %s && cd -"
        cmd_merge_infer = cmd_merge % (outfn_splits,'./*.tmp',outfn)
        os.system(cmd_merge_infer)
        shutil.rmtree(infn_splits)
        shutil.rmtree(outfn_splits)

    def elect_words(self,infn,outfn,wordset):
        outf = open(outfn,'w')
        with open(infn,'r') as inf:
            for line in inf:
                items = [item.strip() for item in line.split('\t')]
                bows = wordset.filter_bows(items[1])
                bows = bows.strip()
                uid = items[0]
                if len(items) != 2 or len(bows.strip()) == 0 or len(uid.strip()) == 0:
                    print(line)
                    time.sleep(0.5)
                    continue
                outf.write(uid+'\t'+bows+'\n')
        outf.close()

if __name__ == "__main__":
    luigi.run() 
