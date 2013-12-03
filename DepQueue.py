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
import sys
import os
import types
import multiprocessing
from multiprocessing import Process
import signal
import logging
import traceback


class DepQueue (object):
	conf_path = '/etc/wndchrm/wndchrm-queue.conf'
	job_time = 300
	try:
		import beanstalkc
		has_beanstalkc = True
	except:
		has_beanstalkc = False
		pass

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
# 		logging.basicConfig(filename='example.log',level=logging.DEBUG)
# 		logging.getLogger(name)
# 		logging.debug('This message should go to the log file')
# 		logging.info('So should this')
# 		logging.warning('And this, too')
		time_stamp = str(datetime.datetime.now().replace(microsecond=0))
		with open(self.conf['worker_log'], "a") as f:
			f.write(
				time_stamp+' '+
				self.process_name+' '+
				' '.join (map(str,args))+
				"\n"
				)

	def get_stats (self):
		if (not self.beanstalk):
			raise ValueError ("queue_stats() called with no beanstalk connection.  Call queue.connect() or queue.wait_connect() first.")

		job_stats = {}
		total_workers = 0
		busy_workers = 0
		jobs_with_deps = 0
		jobs_left = 0
		for tube in self.beanstalk.tubes():
			# Workers always listen to the jobs_tube as well the jobs_ready_tube.
			# Workers switch from listening to the general "deps_tube" to the job-specific dep tube (deps_tube-<job_id>)
			# total number of workers are those listening to the jobs_ready_tube.
			# Total busy workers are the total of reserved jobs
			# Total idle workers is total workers - reserved jobs.
			if tube == self.jobs_tube:
				tube_stats = self.beanstalk.stats_tube(tube)
				busy_workers += tube_stats['current-jobs-reserved']
				jobs_with_deps += tube_stats['current-jobs-buried']
				jobs_left += jobs_with_deps
				jobs_left += tube_stats['current-jobs-ready']
				total_workers = tube_stats['current-watching']
			elif tube == self.jobs_ready_tube:
				tube_stats = self.beanstalk.stats_tube(tube)
				jobs_left += tube_stats['current-jobs-ready']
				busy_workers = tube_stats['current-jobs-reserved']
			elif tube.startswith (self.deps_tube+'-'):
				tube_stats = self.beanstalk.stats_tube(tube)
				job_id = tube[tube.rfind('-')+1:]
				job_stats[job_id] = {}
				job_stats[job_id]['job'] = self.beanstalk.peek (int(job_id))
				job_stats[job_id]['working_workers'] = tube_stats['current-jobs-reserved']
				job_stats[job_id]['idle_workers'] = tube_stats['current-waiting']
				job_stats[job_id]['n_deps'] = tube_stats['total-jobs']
				job_stats[job_id]['n_deps_left'] = tube_stats['current-jobs-ready']
				jobs_left += tube_stats['current-jobs-ready']
				busy_workers += tube_stats['current-jobs-reserved']
		self.job_stats = job_stats
		self.total_workers = total_workers
		self.idle_workers = total_workers - busy_workers
		self.busy_workers = busy_workers
		self.jobs_with_deps = jobs_with_deps
		self.jobs_left = jobs_left



	def wait_connect (self):
		first = True
		# This will poll the server every conf['beanstalkd_wait_retry'] seconds
		# until a connection is made
		while (not self.beanstalk):
			try:
				self.connect()
				first = True
			except DepQueue.beanstalkc.BeanstalkcException:
				if first:
					self.write_log ("Cannot connect to beanstalk server on",
						self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])
					first = False
				self.beanstalk = None
				time.sleep (self.conf['beanstalkd_wait_retry'])


	def connect (self):
		self.beanstalk = DepQueue.beanstalkc.Connection(host=self.conf['beanstalkd_host'], port=self.conf['beanstalkd_port'])
				
		# Store the default tubes
		self.standard_tubes = []

		# Tube for incoming jobs with dependencies.
		# These will be broken up and put into their own job-specific tube
		# with the name <self.deps_tube>-<job_with_deps.id>
		# The original job with dependencies will then be buried
		self.jobs_tube = self.conf['beanstalkd_tube']
		self.standard_tubes.append (self.jobs_tube)

		# This tube gets jobs describing the dependencies (not the actual dependency jobs)
		# The jobs here have the form <job_with_deps.id><tab><tube for this job's dependencies>
		# Workers either watch this tube or a job-specific dependency tube
		# This tube serves to alert workers of pending jobs in job-specific dependency tube
		# Jobs are released back into this tube when they are picked up, and then this tube is ignored so that
		# the worker now watches the job-specific tube
		self.deps_tube = self.conf['beanstalkd_tube']+'-deps'
		self.standard_tubes.append (self.deps_tube)

		# When all job dependencies are satisfied, the original buried job still in the root tube
		# is deleted, and its body is placed in the jobs-ready tube
		self.jobs_ready_tube = self.conf['beanstalkd_tube']+'-ready'
		self.standard_tubes.append (self.jobs_ready_tube)

		# We're not watching anything other than the default tube yet.


	def add_job_deps (self, job, job_dep_tube):
		ndeps = 0
		old_tube = self.beanstalk.using()

		job_params = job.body.split ("\t")
		job_id = job.stats()['id']
		if len (job_params) > 0:
			ndeps = self.add_job_deps_callback (self, job_dep_tube, job_id, job_params)
				# ...
				# beanstalk.put (dep_job)
				# ndeps += 1
				# ...
		return ndeps

	def add_dependent_job (self, job_dep_tube, body):
		self.beanstalk.use (job_dep_tube)
		self.beanstalk.put (body, ttr = DepQueue.job_time)

	def add_job (self, body):
		self.beanstalk.use (self.jobs_tube)
		self.beanstalk.put (body, ttr = DepQueue.job_time)
				
	def run_job (self, job):
		job_params = job.body.split ("\t")
		job_id = job.stats()['id']
		print "about to run",job.jid
		if len (job_params) > 0:
			self.run_job_callback (self, job_id, job_params)
			print "finished running",job.jid,"- deleting"
		job.delete()


	def run(self, run_job_callback, add_job_deps_callback):
		if not type(run_job_callback) == types.FunctionType or not type(add_job_deps_callback) == types.FunctionType:
			raise ValueError ("both run_job_callback and add_job_deps_callback must be functions")
		self.add_job_deps_callback = add_job_deps_callback
		self.run_job_callback = run_job_callback

		# Make sure we're watching our tubes
		for tube in self.standard_tubes:
			self.beanstalk.watch (tube)

		self.write_log ("Running queue, watching tubes", self.beanstalk.watching(),
			"on",self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])

		deps_tube = self.deps_tube
		jobs_ready_tube = self.jobs_ready_tube
		keepalive = True
		while (keepalive):
			job = self.beanstalk.reserve(60)
			if (job is None):
				self.clean()
				continue
			job_tube = job.stats()['tube']
			if (job_tube == deps_tube):
				(job_id, job_dep_tube) = job.body.split("\t")
				print "running from",job_tube,"job_id",job.jid,"for job_id",job_id,"job_dep_tube",job_dep_tube
				if job_dep_tube in self.beanstalk.tubes():
					job_dep_tube_stats = self.beanstalk.stats_tube(job_dep_tube)
					if job_dep_tube_stats['current-jobs-ready'] > 0:
						job.release()
						self.beanstalk.watch(job_dep_tube)
						self.beanstalk.ignore (deps_tube)
					else:
						# no pending jobs left
						job.delete()
						self.beanstalk.watch(deps_tube)
						self.beanstalk.ignore(job_dep_tube)
				else:
					# The dependency tube is gone
					print "job_dep_tube",job_dep_tube,"is gone! Deleting",job.jid
					job.delete ()
			elif (job_tube == self.jobs_tube):
				job_id = job.stats()['id']
				job_dep_tube = deps_tube+'-'+str(job_id)
				ndeps = self.add_job_deps (job, job_dep_tube)
				if (ndeps):
					self.beanstalk.use (deps_tube)
					self.beanstalk.put (str(job_id)+"\t"+job_dep_tube, ttr = DepQueue.job_time)
					job.bury()
					print "submitted job id",job_id,job.body,"added",ndeps,"dependencies"
				else:
					self.beanstalk.use (jobs_ready_tube)
					self.beanstalk.put (job.body, ttr = DepQueue.job_time)
					print "deleting",job.jid
					job.delete()
					print "submitted job id",job_id,job.body.split("\t"),"has no deps, re-send to",jobs_ready_tube

			else:
				# job_tube is a job_dep_tube or a job_ready
				print "running job id",job.jid,job.body.split("\t")
				self.run_job (job)
				if job_tube != jobs_ready_tube:
				# the job came from a dependency tube.
				# Check if this is the last dependency
					tube_stats = self.beanstalk.stats_tube(job_tube)
					# If there are no more outstanding jobs, ignore the tube
					if not tube_stats['current-jobs-ready'] > 0:
						self.beanstalk.watch(deps_tube)
						self.beanstalk.use (deps_tube)
						self.beanstalk.ignore(job_tube)

					# If there are no reserved jobs and no outstanding jobs, clean up
					if not tube_stats['current-jobs-ready'] > 0 and not tube_stats['current-jobs-reserved'] > 0:
						job_id = job_tube[job_tube.rfind('-')+1:]
						print "peeking at",job_id,"from tube",job_tube
						ready_job = self.beanstalk.peek (int(job_id))
						if (ready_job):
							job_body = ready_job.body
							print "deleting",ready_job.jid
							self.beanstalk.use (job_tube)
							ready_job.delete()
							self.beanstalk.use (jobs_ready_tube)
							self.beanstalk.put (job_body, ttr = DepQueue.job_time)

		# ignore our tubes when cleaning up
		for tube in self.standard_tubes:
			self.beanstalk.ignore (tube)


	def clean (self):
		self.beanstalk.use (self.jobs_tube)
		# Really we would like to kick a specific job, but a general kick may work for now
		self.beanstalk.kick(1)

	def clear_all (self):
		# gather up all the tubes
		our_tubes = self.standard_tubes

		# Get all the dependency tubes as well
		for tube in self.beanstalk.tubes():
			if tube.startswith (self.deps_tube+'-'):
				our_tubes.append (tube)

		# make sure we're watching all of our tubes
		current_watching = self.beanstalk.watching()
		for tube in our_tubes:
			if tube not in current_watching:
				self.beanstalk.watch (tube)
		
		# make sure we're only watching our tubes
		current_watching = self.beanstalk.watching()
		for tube in current_watching:
			if tube not in our_tubes:
				self.beanstalk.ignore (tube)

		# To peek or delete buried jobs, we have to use the tube they're in
		# In contrast, peek_ready (and possibly peek_delayed) pull jobs out of any tube we're watching 
		for tube in our_tubes:
			self.beanstalk.use (tube)
			job = self.beanstalk.peek_ready() or self.beanstalk.peek_delayed() or self.beanstalk.peek_buried()
			while ( job ):
				print "deleting job",job.jid,"from tube",job.stats()['tube']
				job.delete()
				job = self.beanstalk.peek_ready() or self.beanstalk.peek_delayed() or self.beanstalk.peek_buried()

		# ignore all but the standard tubes
		for tube in self.beanstalk.tubes():
			if tube not in self.standard_tubes:
				self.beanstalk.ignore(tube)


	# Convenience method to return absolute path of provided program
	@staticmethod
	def which(program):
		import os
		def is_exe(fpath):
			return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

		fpath, fname = os.path.split(program)
		if fpath:
			if is_exe(program):
				return os.path.abspath(program)
		else:
			for path in os.environ["PATH"].split(os.pathsep):
				path = path.strip('"')
				exe_file = os.path.join(path, program)
				if is_exe(exe_file):
					return os.path.abspath(exe_file)
		return None


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
	fatal_exit = 3

	def __init__(self, run_job_callback, add_job_deps_callback):
		self.queue = DepQueue()
		self.workers = {}
		self.run_job_callback = run_job_callback
		self.add_job_deps_callback = add_job_deps_callback

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

		# keep going until we get a fatal exception
		while (True):
			try:
				queue.wait_connect()
				queue.run (self.run_job_callback, self.add_job_deps_callback)
			except DepQueue.beanstalkc.SocketError:
				self.beanstalk = None
				queue.write_log ("Lost connection to beanstalk server on", self.conf['beanstalkd_host'],'port',self.conf['beanstalkd_port'])
			except DepQueue.beanstalkc.DeadlineSoon:
				queue.write_log ("Got DeadlineSoon excetion - ignoring")
			# The default exception is fatal to us and the parent
			except Exception as e:
				queue.write_log ("Exception: ", e)
				QueueMain.terminating = True
				traceback.print_exc(file=sys.stderr)
				sys.exit(QueueMain.fatal_exit)



	def sighandler(self, signum, frame):
		print 'Signal handler called with signal', QueueMain.signals_handled[signum]
		workers = self.workers
		if signum == signal.SIGINT or signum == signal.SIGTERM:
			QueueMain.terminating = True
			print "terminating workers"
			for worker in workers:
				if workers[worker]['process']: workers[worker]['process'].terminate()

		elif signum == signal.SIGCHLD:
			for worker in workers.keys():
				process = workers[worker]['process']
				w_name = workers[worker]['name']
				if (process and not process.is_alive()):
					print "worker", w_name, "died with exitcode", process.exitcode
					workers[worker]['process'] = None
					if (not QueueMain.terminating and not process.exitcode == QueueMain.fatal_exit):
						print "restarting worker",w_name
						process = Process (target = self.work, name = w_name, args = (w_name,))
						process.start()
						workers[worker]['process'] = process
					elif process.exitcode == QueueMain.fatal_exit:
						QueueMain.terminating = True

		elif signum == signal.SIGHUP:
			print "restarting workers"
			# They will send SIGCHLD when they terminate, which will restart them
			# when terminate is False
			# Main should re-read the config as well...
			for worker in workers:
				if workers[worker]['process']: workers[worker]['process'].terminate()

def run_job_callback(queue, job_id, job_params):
	pass
def add_job_deps_callback(queue, dep_tube, job_id, job_params):
	pass

def main():
	if not DepQueue.has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)


	main_scope = QueueMain(run_job_callback, add_job_deps_callback)
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

