#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-06-06 09:46:50
import os
import sys
import inspect
import json
import time
import commands
import shutil
import multiprocessing
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from ConfigParser import SafeConfigParser
import jieba
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class SegmentUser(luigi.Task):
    conf = luigi.Parameter()
    def __init__(self, *args, **kwargs):
        luigi.Task.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.seg_dict = '%s/adjunct/SmartChineseTokenizerFactory.dict' % self.root
        self.infile = '%s/data/segmentation/%s' % (self.root,'500W.user.txt')
        self.outfile = '%s/data/segmentation/%s' % (self.root,'500W.user.seg')
        self.errfile = '%s/data/segmentation/%s' % (self.root,'500W.user.err')
        #jieba.load_userdict(self.seg_dict)

    def output(self):
        return luigi.LocalTarget(self.outfile)

    def requires(self):
        return []

    def run(self):
        jieba.load_userdict(self.seg_dict)
        self.handle_core(self.infile,self.outfile,self.errfile)

    def handle_core(self,in_filename,out_filename,err_filename):
        '''in_filename 格式为 id   string。
        id为编号，string为文本描述,id与string之间用制表符间隔（下同）
        out_filename 格式为 id    string。
        id为编号，string为<word1,count><word2,count>......
        eg:123  a 3 b 4 c 5 u 1 ......'''
        #jieba.load_userdict(self.seg_dict) #自定义分词词典
        with open(in_filename,'r')as infn,open(out_filename,'w')as outfn,open(err_filename,'w')as err:
            for line in infn:
                line = line.strip()
                items = [it.strip() for it in line.split('\t') if not it.isspace()]
                if len(items) != 2:err.write(line+'\n')
                txt_id,txt_str = items[0],items[1]
                seg_word_counts = self.segment(txt_str)
                outfn.write(txt_id+'\t'+seg_word_counts+'\n')
                
    def segment(self,txt_string):
        '''txt_string 为需要分词的字符串'''
        seg_words = jieba.cut(txt_string,cut_all=False) #精确模式
        seg_list = [w.strip() for w in seg_words if not w.isspace()]
        rest = " ".join(seg_list)
        result = self.word_count(rest)
        return result

    def word_count(self,seged_txt_string):
        '''seged_txt_string 的格式为分词后以空格分割的“词”    '''
        res = []
        txt = seged_txt_string.strip()
        words = [i.strip() for i in txt.split(" ")]
        uniq_words = set(words)
        for w in uniq_words:
            res.append(w)
            res.append(str(words.count(w)))
        txt = " ".join(res)
        return txt

if __name__ == "__main__":
    luigi.run()
