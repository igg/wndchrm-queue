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

class DepQueue (object):
	conf_path = '/etc/wndchrm/wndchrm-queue.conf'

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
		config = ConfigParser.SafeConfigParser()
		config.readfp(io.BytesIO('[conf]\n'+open(DepQueue.conf_path, 'r').read()))
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

	def wait_connect (self):
		first = True
		# This will poll the server every conf['beanstalkd_wait_retry'] seconds
		# until a connection is made
		while (not self.beanstalk):
			try:
				self.connect()
				first = True
			except beanstalkc.BeanstalkcException:
				if first:
					self.write_log ("Cannot connect to beanstalk server on",
						self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])
					first = False
				self.beanstalk = None
				time.sleep (self.conf['beanstalkd_wait_retry'])


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

		self.write_log ("Watching tubes", self.beanstalk.watching(),
			"on",self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])


	def add_job_deps (self, job, job_dep_tube):
		ndeps = 0
		old_tube = self.beanstalk.using()

		ndeps = self.add_job_deps_callback (self, job, job_dep_tube)
			# ...
			# beanstalk.put (dep_job)
			# ndeps += 1
			# ...
		return ndeps

	def add_dependent_job (self, job_dep_tube, body):
		self.beanstalk.use (job_dep_tube)
		self.beanstalk.put (body)
				

	def run_job (self, job):
		# ...
		self.run_job_callback (self, job)
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



# A class for main to use to maintain some scope without globals
# Intent is to have one of these per worker pool on a given server.
class QueueMain (object):
	# signals handled by the sighandler, and reset to default in children
	signals_handled = {
		signal.SIGTERM: 'SIGTERM', 
		signal.SIGINT:  'SIGINT', 
		signal.SIGCHLD: 'SIGCHLD', 
		signal.SIGHUP:  'SIGHUP', 
		}
	terminating = False

	def __init__(self):
		self.queue = DepQueue()
		self.workers = {}

	def __del__(self):
		self.queue.write_log ("Main terminating")
	
	def launch_workers (self):
		num_workers = self.queue.conf['num_workers']
		for i in range (1,num_workers+1):
			worker_name = 'W'+str(i).zfill(len(str(num_workers)))
			worker = Process (target = self.work, name = worker_name, args = (worker_name,))
			# FIXME: make it a daemon?
			worker.daemon = False
			self.workers[worker_name] = {'name': worker_name, 'process': worker}

		self.queue.write_log ("Main starting",num_workers,"workers")

		for worker in self.workers:
			self.workers[worker]['process'].start()

	def work (self, worker_name):
		# subprocess entry point
		#
		# set signals back to default
		for signum in QueueMain.signals_handled.keys():
			signal.signal(signum,signal.SIG_DFL)

		# and make our own queue
		queue = DepQueue()
		queue.write_log ("Starting worker", worker_name)

		try:
			queue.wait_connect()
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
		except beanstalkc.SocketError:
			queue.write_log ("Lost connection to beanstalk server on", self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])
			sys.exit(1)
		except Exception as e:
			queue.write_log ("Exception: ", e)
			sys.exit(1)


	def sighandler(self, signum, frame):
		print 'Signal handler called with signal', QueueMain.signals_handled[signum]
		workers = self.workers
		if signum == signal.SIGINT or signum == signal.SIGTERM:
			QueueMain.terminating = True
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
					if (not QueueMain.terminating):
						print "restarting worker",w_name
						process = Process (target = self.work, name = w_name, args = (w_name,))
						process.start()
						workers[worker]['process'] = process

		elif signum == signal.SIGHUP:
			print "restarting workers"
			# They will send SIGCHLD when they terminate, which will restart them
			# when terminate is False
			# Main should re-read the config as well...
			for worker in workers:
				workers[worker]['process'].terminate()



def get_wndchrm_command (job_body):
	# a Job body may begin with the key word "wndchrm" followed by "train", "test", "classify".
	# if the job body begins with wndchrm and has at least two other parameters, return
	# 'wndchrm', sub-command, params tuple.
	# If not, return a (None, None, job_body) tuple.

	jobRE = re.compile ('^\s*(wndchrm)\s+(\S+)\s+(.+)$')
	jobREres = jobRE.search (job.body)
	if (jobREres):
		return (jobREres.groups())
	else:
		return (None, None, job_body)

def run_job_callback (queue, job):
	# This job has already been looked at by add_job_deps_callback, which generated more sub-jobs
	# So, this particular job is ready to execute as is.
	(cmd, sub_cmd, params) = get_wndchrm_command (job.body)
	if cmd:
		# We probably need the shell to parse the params
		(out, err) = subprocess.Popen(["sh", "-c", queue.conf['wndchrm_executable']+' '+sub_cmd+' '+params],
			stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()


def add_job_deps_callback (queue, job, dep_tube):
	# This job has just entered the job queue, so we have to see if it can be broken into sub-jobs
	# The breaking into subjobs is all that we do here - we do not "run" anything.
	# The number of subjobs the main job can be broken into is returned
	# If there are no subjobs, then 0 is returned.
	# maybe this should be a generator returning job bodies?
	#   Do generators end in None, an exception, some other way?
	# Either way isolate this from direct beanstalkc calls.
	# use queue.add_dependent_job (dep_tube, job, body) for now.

	(cmd, sub_cmd, params) = get_wndchrm_command (job.body)
	if cmd:
		# We probably need the shell to parse the params
		# wndchrm commands that are not 'check' are turned into check commands.
		# We probably need the shell to parse the params
		if sub_cmd not == 'check':
			(out, err) = subprocess.Popen(["sh", "-c", queue.conf['wndchrm_executable']+' check '+params],
				stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
		# how to do this one line at a time?

def main():
	if not has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)


	main_scope = QueueMain()
	main_scope.launch_workers()

	# We register the signal handlers after starting workers
	# workers have to reset their own signal handlers
	for signum in QueueMain.signals_handled.keys():
		signal.signal(signum,main_scope.sighandler)


	# Nothing to do here.
	# Don't want to join any process, since we'd block on only one process
	# Take nice naps.
	while (not QueueMain.terminating):
		time.sleep (100)


if __name__ == "__main__":
    main()

