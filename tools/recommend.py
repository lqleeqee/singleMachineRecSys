#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: lizujun
# @email: lizujun2008@gmail.com
# @cratetime : 2017-05-16 09:57:07
import os
import sys
import inspect
import csv
import json
import time
import Queue
import threading
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
from shutil import copyfile
from gensim import corpora, models, similarities
from annoy import AnnoyIndex
from tools.corpus import FeaCorpus, BatchFeaCorpus

class RecThread(threading.Thread):
	def __init__(self, queue, index, uids, docids, topk, threshold, out_fd, lock,isUser=True,isRecUser=True):
		threading.Thread.__init__(self)
		self.queue = queue
		self.index = index
		self.uids = uids
		self.docids = docids
		self.topk = topk
		self.threshold = threshold
		self.out_fd = out_fd
		self.lock = lock
		self.isRecUser = isRecUser
		self.isUser = isUser

	def run(self):
		while True:
			batch = self.queue.get()
			base_id = batch[0]
			s = time.time()
			self.batch_rec(base_id, batch[1],self.isUser,self.isRecUser)
			e = time.time()
			t = (e - s)
			print 'recommend %d users, cost %.2fs/user' % (len(batch[1]), 1.0 * t / len(batch[1]))
			sys.stdout.flush()
			self.queue.task_done()

	def batch_rec(self, base_id, batch,isUser,isRecUser):
		idx = 0
		uid = None
		for v in batch:
			if isUser:
				uid = self.uids[base_id + idx]
			else:
				uid = self.uids[base_id + idx][:-2]
			#v = normalize([v], norm='l2', copy=False)[0]
                        rec = self.index.get_nns_by_vector(v, n = self.topk, include_distances=True)
                        jrlist = []
			if isRecUser:
				for i in xrange(self.topk):
	                                userid = self.docids[rec[0][i]]
        	                        s = round(1 - rec[1][i], 2)
                	                if s > self.threshold:
                        	                jrlist.append({"id" : userid, "s" : s})
			else:
				for i in xrange(self.topk):
					docid = self.docids[rec[0][i]]
					doc_type = docid[-1:]
					docid = docid[6:-2]
					s = round(1 - rec[1][i], 2)
					if s > self.threshold:
						jrlist.append({"id" : docid, "s" : s,'type':int(doc_type)})
			if len(jrlist) > 0:
				line = uid + '\t' + json.dumps(jrlist)
				self.lock.acquire()
				print >> self.out_fd, line
				self.lock.release()
			idx += 1

def recommend(out_fd, user_fn, docid_fn, index_fn, n_components, topk, batch_size, threshold, thread_num,isUser=True,isRecUser=True):	
	uids = [uid.strip() for uid in FeaCorpus(user_fn, onlyID=True)]
	docids = [docid.strip() for docid in open(docid_fn)]
	user_batch = BatchFeaCorpus(user_fn, batch_size, sparse=False)
	index = AnnoyIndex(n_components, metric='angular')
	index.load(index_fn)
	queue = Queue.Queue()
	lock = threading.Lock()
	total_user = 0
	s = time.time()
	for batch in user_batch:
		queue.put(batch)
		total_user += len(batch[1])
	for i in range(thread_num):
		t = RecThread(queue, index, uids, docids, topk, threshold, out_fd, lock,isUser,isRecUser)
		t.setDaemon(True)
		t.start()
	queue.join()
	e = time.time()
	t = (e - s)
	print 'recommend %d users, %.2fs/user' % (total_user, 1.0 * t / total_user)
