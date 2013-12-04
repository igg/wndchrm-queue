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

from DepQueue import DepQueue, QueueMain
import signal
import time # for sleep
import datetime
import subprocess
import sys
import os


def is_wndchrm_command (command):
	# looks for "wndchrm" after the last '/'
	if command[command.rfind("/")+1:] == "wndchrm":
		return True
	else:
		return False

def run_job_callback (queue, job_id, job_params):
	# This job has already been looked at by add_job_deps_callback, which generated more sub-jobs
	# So, this particular job is ready to execute as is.
	#
	print "in run_job_callback, job",job_id,":",job_params
	if is_wndchrm_command (job_params[0]):
		(out, err) = subprocess.Popen(job_params, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
		# FIXME: Do something with the error output
	else:
		print "don't know what to do with",job_params

def add_job_deps_callback (queue, dep_tube, job_id, job_params):
	n_deps = 0
	# This job has just entered the job queue, so we have to see if it can be broken into sub-jobs
	# The breaking into subjobs is all that we do here - we do not "run" anything in addition to that.
	# The number of subjobs the main job can be broken into is returned (or 0)
	# use queue.add_dependent_job (dep_tube, job, job_params) to add dependencies to the queue

	# wndchrm commands that are not 'check' are turned into check commands.
	if is_wndchrm_command (job_params[0]) and len (job_params) > 2 and not job_params[1] == 'check':
		# run wndchrm the same way, but using the "check" command instead of what we were doing before
		job_params[1] = 'check'
		
		# Set up the dependent jobs resulting from running this one with the "check" command
		# keep any wndchrm parameters (beginning with '-')
		# replace the wndchrm command with "train", and the output with /dev/null.
		dep_job = [job_params[0],"train"]
		for param in job_params[2:]:
			if param.startswith ("-"):
				dep_job.append (param)
		# add placeholder for input file and output file
		dep_job.extend([None,"/dev/null"])
		input_idx = len(dep_job)-2

		print "Getting dependencies from runinng",job_params
		proc = subprocess.Popen(job_params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		# process output one line at a time - or whatever wndchrm buffers as...
		for line in iter(proc.stdout.readline,''):
			file_path = line.strip()
			dep_job[input_idx] = file_path
			if os.path.isfile(file_path):
				print "Adding dependent job",dep_job
				queue.add_dependent_job (dep_tube, dep_job)
				n_deps += 1
			else:
				print "Ignoring",file_path
			sys.stdout.flush()
		# FIXME: Do something with the error output
	return (n_deps)

def main():
	if not DepQueue.has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)

	main_scope = QueueMain(run_job_callback, add_job_deps_callback)
	main_scope.launch_workers()

	# Nothing to do here.
	# Don't want to join any process, since we'd block on only one process
	# Take nice naps.
	while (not QueueMain.terminating):
		time.sleep (100)

if __name__ == "__main__":
    main()

