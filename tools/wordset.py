#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-08 14:54:11

import os
import re
import sys
import logging
import time
import inspect
import math
pfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

class WordSet(object):
    def __init__(self, vocab_fn):
        with open(vocab_fn) as vocab_fd:
            self.wordset = set([line.split('\t')[1] for line in vocab_fd])

    def filter_bows(self, line):
        bows = line.split(' ')
        filtered = []
        for i in xrange(len(bows) / 2):
            word = bows[i * 2]
            freq = bows[i * 2 + 1]
            if word in self.wordset:
                filtered.append(word)
                filtered.append(freq)
        return " ".join(filtered)
