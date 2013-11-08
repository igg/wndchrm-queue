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
import ConfigParser
import io
import socket # for socket.gethostbyaddr
import subprocess
import sys
import os
import types


has_beanstalkc = False
try:
	import beanstalkc
	has_beanstalkc = True
except:
	pass

conf_path = '/etc/wndchrm/wndchrm-queue.conf'

class DepQueue (object):
	def __init__(self, run_job_callback, add_job_deps_callback):
		self.conf = None
		read_config (self)

		self.beanstalk = None
		connect (self)

		if not type(run_job_callback) == types.FunctionType or not type(add_job_deps_callback) == types.FunctionType
			raise ValueError ("both run_job_callback and add_job_deps_callback must be functions")
		self.add_job_deps_callback = add_job_deps_callback
		self.run_job_callback = run_job_callback

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
		self.beanstalk.watch (self.jobs_tube)

		# When all job dependencies are satisfied, the original buried job still in the root tube
		# is deleted, and its body is placed in the jobs-ready tube
		self.jobs_ready_tube = self.conf['beanstalkd_tube']+'-ready'
		self.beanstalk.watch (self.jobs_tube)
		


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


	def add_job_deps (self, job, job_dep_tube):
		ndeps = 0
		old_tube = self.beanstalk.using()
	
		self.beanstalk.use (job_dep_tube)
		ndeps = self.add_job_deps_callback (job)
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

	def run(self):
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
			job_dep_tube = dep_tube+'-'+str(job_id)
			ndeps = add_job_deps (job, job_dep_tube)
			if (ndeps)
				self.beanstalk.use (dep_tube)
				self.beanstalk.put (job_id+"\t"+job_dep_tube)
				job.bury()
		else:
			run_job (job)
			tube_stats = self.beanstalk.stats_tube(job_tube)
			if (not tube_stats['current-jobs-reserved'] + tube_stats['current-jobs-ready'] > 0):
				self.beanstalk.watch(deps_tube)
				self.beanstalk.ignore(job_tube)
				self.beanstalk.use (deps_tube)


def main():
	if not has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)
watch tubes:
	jobs
	job-deps
	jobs-ready
	
	while (1):
		try:
			queue = dependent_queue()
			retries = 0
			queue.run()
		except beanstalkc.SocketError:
			retries += 1
			if (retries > max_retries):
				print_log ("Giving up after "+retries+" retries. Worker exiting.")
				sys.exit(1)
			if queue:
				print_log ("beanstalkd on "+beanstalkd_host+" port "+beanstalkd_port+" not responding. Retry in "+wait_retry+" seconds.")
				queue = None
			time.sleep(wait_retry)
		except Exception e:
			retries += 1
			if (retries > max_retries):
				print_log ("Giving up after "+retries+" retries. Worker exiting.")
				sys.exit(1)
			print_log (str(e))
			if not queue:
				time.sleep(wait_retry)


if __name__ == "__main__":
    main()

