#!/usr/bin/env python
# Authored by Ilya Goldberg (igg at cathilya dot org), Nov., 2013
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time # for sleep
import datetime
import ConfigParser
import io
import socket # for socket.gethostbyaddr
import subprocess
import sys
import os
import types
import multiprocessing
from multiprocessing import Process
import signal

has_beanstalkc = False
try:
	import beanstalkc
	has_beanstalkc = True
except:
	pass

conf_path = '/etc/wndchrm/wndchrm-queue.conf'


class DepQueue (object):
	def __init__(self):
		self.conf = None
		self.read_config ()

		self.beanstalk = None

		self.process_name = ''
		if 'main' not in multiprocessing.current_process().name.lower():
			self.process_name += multiprocessing.current_process().name
		else:
			self.process_name += 'Main'

		self.process_name += '['+str(multiprocessing.current_process().pid)+']'
		self.process_name += '@'+self.conf['worker_host']+':'


	def read_config(self):
		global conf_path
		config = ConfigParser.SafeConfigParser()
		config.readfp(io.BytesIO('[conf]\n'+open(conf_path, 'r').read()))
		self.conf = {
			'wndchrm_executable'    : None,
			'num_workers'           : None,
			'beanstalkd_host'       : None,
			'beanstalkd_port'       : None,
			'beanstalkd_tube'       : None,
			'beanstalkd_wait_retry' : None,
			'beanstalkd_retries'    : None,
			'worker_PID_dir'        : None,
			'worker_log'            : None,
			'worker_host'           : None,
			}
		conf_ints = {
			'num_workers'           : None,
			'beanstalkd_port'       : None,
			'beanstalkd_wait_retry' : None,
			'beanstalkd_retries'    : None,
			}
		for k in self.conf.keys():
			if (k in conf_ints):
				self.conf[k] = int (config.get("conf", k))
			else:
				self.conf[k] = config.get("conf", k)
		self.max_retries = self.conf['beanstalkd_retries']
		self.wait_retry = self.conf['beanstalkd_wait_retry']

	def write_log (self, *args):
		with open(self.conf['worker_log'], "a") as f:
			f.write(
				str(datetime.datetime.now().replace(microsecond=0))+' '+
				self.process_name+' '+
				' '.join (map(str,args))+
				"\n"
				)

	def connect (self):
		self.beanstalk = beanstalkc.Connection(host=self.conf['beanstalkd_host'], port=self.conf['beanstalkd_port'])
		# Tube for incoming jobs with dependencies.
		# These will be broken up and put into their own job-specific tube
		# with the name <self.deps_tube>-<job_with_deps.id>
		# The original job with dependencies will then be buried
		self.jobs_tube = self.conf['beanstalkd_tube']
		self.beanstalk.watch (self.jobs_tube)

		# This tube gets jobs describing the dependencies (not the actual dependency jobs)
		# The jobs here have the form <job_with_deps.id><tab><tube for this job's dependencies>
		# Workers either watch this tube or a job-specific dependency tube
		# This tube serves to alert workers of pending jobs in job-specific dependency tube
		# Jobs are released back into this tube when they are picked up, and then this tube is ignored so that
		# the worker now watches to the job-specific tube
		self.deps_tube = self.conf['beanstalkd_tube']+'-deps'
		self.beanstalk.watch (self.deps_tube)

		# When all job dependencies are satisfied, the original buried job still in the root tube
		# is deleted, and its body is placed in the jobs-ready tube
		self.jobs_ready_tube = self.conf['beanstalkd_tube']+'-ready'
		self.beanstalk.watch (self.jobs_ready_tube)
		
		# Might as well ignore 'default' tube
		self.beanstalk.ignore ('default')


		self.write_log ("Watching tubes", self.beanstalk.watching())


	def add_job_deps (self, job, job_dep_tube):
		ndeps = 0
		old_tube = self.beanstalk.using()

		self.beanstalk.use (job_dep_tube)
		ndeps = self.add_job_deps_callback (job, job_dep_tube)
		# ...
		# beanstalk.put (dep_job)
		# ndeps += 1
		# ...
		self.beanstalk.use (old_tube)
		return ndeps


	def run_job (self, job):
		# ...
		self.run_job_callback (job)
		job.delete()


	def run(self, run_job_callback, add_job_deps_callback):
		if not type(run_job_callback) == types.FunctionType or not type(add_job_deps_callback) == types.FunctionType:
			raise ValueError ("both run_job_callback and add_job_deps_callback must be functions")
		self.add_job_deps_callback = add_job_deps_callback
		self.run_job_callback = run_job_callback

		keepalive = True
		while (keepalive):
			job = self.beanstalk.reserve()
			job_tube = job.stats()['tube']
			if (job_tube == deps_tube):
				(job_id, job_dep_tube) = job.body.split("\t")
				if (self.beanstalk.stats_tube(job_dep_tube)['current-jobs-ready'] > 0):
					job.release()
					if (job_dep_tube not in self.beanstalk.watching()): self.beanstalk.watch(job_dep_tube)
					self.beanstalk.ignore (deps_tube)
				else:
					self.beanstalk.watch(deps_tube)
					self.beanstalk.ignore(job_dep_tube)
					if (not self.beanstalk.stats_tube(job_dep_tube)['current-jobs-reserved'] > 0):
						ready_job = self.beanstalk.peek (job_id)
						if (ready_job):
							self.beanstalk.use (jobs_ready_tube)
							self.beanstalk.put (ready_job.body)
			elif (job_tube == jobs_tube):
				job_id = job.stats()['id']
				job_dep_tube = deps_tube+'-'+str(job_id)
				ndeps = add_job_deps (job, job_dep_tube)
				if (ndeps):
					self.beanstalk.use (deps_tube)
					self.beanstalk.put (job_id+"\t"+job_dep_tube)
					job.bury()
			else:
				# job_tube is a job_dep_tube
				run_job (job)
				tube_stats = self.beanstalk.stats_tube(job_tube)
				if (not tube_stats['current-jobs-reserved'] + tube_stats['current-jobs-ready'] > 0):
					self.beanstalk.watch(deps_tube)
					self.beanstalk.ignore(job_tube)
					job_id = job_tube[job_tube.rfind('-')+1:]
					ready_job = self.beanstalk.peek (job_id)
					if (ready_job):
						self.beanstalk.use (jobs_ready_tube)
						self.beanstalk.put (ready_job.body)
					self.beanstalk.use (deps_tube)


def run_job_callback (job):
	pass

def add_job_deps_callback (job, dep_tube):
	pass


workers = {}
terminating = False
# signals handled by the sighandler, and reset to default in children
signals_handled = {
	signal.SIGTERM: 'SIGTERM', 
	signal.SIGINT:  'SIGINT', 
	signal.SIGCHLD: 'SIGCHLD', 
	signal.SIGHUP:  'SIGHUP', 
	}

def work (worker_name):
	global signals_handled

	# set signals back to default
	for signum in signals_handled.keys():
		signal.signal(signum,signal.SIG_DFL)

	queue = DepQueue()
	queue.write_log ("Starting worker", worker_name)
	queue.connect()
	try:
		if worker_name.endswith('3'):
			time.sleep(5)
			queue.write_log ("Raising exception from worker", worker_name)
			raise Exception (worker_name+": I can't go on!")
		elif worker_name.endswith('2'):
			time.sleep(10)
			queue.write_log ("Returning from worker", worker_name)
			return
		else:
			queue.run (run_job_callback, add_job_deps_callback)
	except Exception as e:
		queue.write_log ("Exception: ", e)
		sys.exit(1)

def sighandler(signum, frame):
	global terminating
	global workers
	global signals_handled

	print 'Signal handler called with signal', signals_handled[signum]
	if signum == signal.SIGINT or signum == signal.SIGTERM:
		terminating = True
		print "terminating workers"
		for worker in workers:
			workers[worker]['process'].terminate()

	elif signum == signal.SIGCHLD:
		for worker in workers.keys():
			process = workers[worker]['process']
			w_name = workers[worker]['name']
			if (process and not process.is_alive()):
				print "worker", w_name, "died with exitcode", process.exitcode
				workers[worker]['process'] = None
				if (not terminating):
					print "restarting worker",w_name
					process = Process (target = work, name = w_name, args = (w_name,))
					process.start()
					workers[worker]['process'] = process

	elif signum == signal.SIGHUP:
		print "restarting workers"
		# They will send SIGCHLD when they terminate, which will restart them
		# when terminate is False
		for worker in workers:
			workers[worker]['process'].terminate()


def main():
	global workers
	global signals_handled
	global terminating

	if not has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)

	queue = DepQueue()
	num_workers = queue.conf['num_workers']
	for i in range (1,num_workers+1):
		worker_name = 'W'+str(i).zfill(len(str(num_workers)))
		worker = Process (target = work, name = worker_name, args = (worker_name,))
		worker.daemon = True
		workers[worker_name] = {'name': worker_name, 'process': worker}

	queue.write_log ("Main starting",num_workers,"workers")

	for worker in workers:
		workers[worker]['process'].start()

	# We register the signal handlers after starting workers
	# workers have to reset their own signal handlers
	for signum in signals_handled.keys():
		signal.signal(signum,sighandler)


	# Nothing to do here.
	# Don't want to join any process, since we'd block on only one process
	# Take nice naps.
	while (not terminating):
		time.sleep (100)
	queue.write_log ("Main terminating")


if __name__ == "__main__":
    main()

