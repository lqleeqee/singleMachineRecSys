#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-08 13:47:55

import os
import sys
import inspect
import errno
from shutil import copyfile
from ConfigParser import SafeConfigParser
pfolder=os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0],"..")))
if pfolder not in sys.path:
    sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi


class CheckModel(luigi.ExternalTask):
    conf = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        luigi.ExternalTask.__init__(self, *args, **kwargs)
        parser = SafeConfigParser()
        parser.read(self.conf)
        self.root = parser.get("basic", "root")
        self.model_fn = parser.get("plda", "model_fn")

    def output(self):
        local_model_fn = './data/%s' % self.model_fn
        return luigi.LocalTarget(local_model_fn)


if __name__ == "__main__":
    luigi.run()
