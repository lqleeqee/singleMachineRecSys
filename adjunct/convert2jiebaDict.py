#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-09 08:58:49
import os
import sys


in_fn = './coredict.dict'
out_fn = './SmartChineseTokenizerFactory.dict'

outf = open(out_fn,'wb')

i = 0
with open(in_fn,'r') as inf:
    for line in inf:
        line = line.strip()
        if len(line) <= 0 :continue
        
        #i = i+1
        #print(line)
        word = line[:line.index('[')]
        freq = line[line.index('=')+1:line.index(',')]
        post = line[line.rindex('=')+1:line.index(']')]#Part-of-speech tagging 
        #print(word+'\t'+freq+'\t'+post+'\n')
        #if i > 10:break
        outf.write(word+'\t'+freq+'\t'+post+'\n')

outf.close()
