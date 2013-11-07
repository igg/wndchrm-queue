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

has_beanstalkc = False
try:
	import beanstalkc
	has_beanstalkc = True
except:
	pass

def add_job_deps (job, job_dep_tube):
	ndeps = 0
	old_tube = beanstalk.using()
	
	beanstalk.use (job_dep_tube)
	# ...
	# beanstalk.put (dep_job)
	# ndeps += 1
	beanstalk.use (old_tube)
	return ndeps

def run_job (job):
	# ...
	job.delete()

job = beanstalk.reserve()
job_tube = job.stats()['tube']
if (job_tube == deps_tube):
	(job_id, job_dep_tube) = job.body.split("\t")
	if (beanstalk.stats_tube(job_dep_tube)['current-jobs-ready'] > 0):
		job.release()
		if (job_dep_tube not in beanstalk.watching()): beanstalk.watch(job_dep_tube)
		beanstalk.ignore (deps_tube)
	else:
		beanstalk.watch(deps_tube)
		beanstalk.ignore(job_dep_tube)
		if (not beanstalk.stats_tube(job_dep_tube)['current-jobs-reserved'] > 0):
			ready_job = beanstalk.peek (job_id)
			if (ready_job):
				beanstalk.use (jobs_ready_tube)
				beanstalk.put (ready_job.body)
elif (job_tube == jobs_tube):
	job_id = job.stats()['id']
	job_dep_tube = dep_tube+'-'+str(job_id)
	ndeps = add_job_deps (job, job_dep_tube)
	if (ndeps)
		beanstalk.use (dep_tube)
		beanstalk.put (job_id+"\t"+job_dep_tube)
		job.bury()
else:
	run_job (job)
	tube_stats = beanstalk.stats_tube(job_tube)
	if (not tube_stats['current-jobs-reserved'] + tube_stats['current-jobs-ready'] > 0):
		beanstalk.watch(deps_tube)
		beanstalk.ignore(job_tube)
		beanstalk.use (deps_tube)


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

