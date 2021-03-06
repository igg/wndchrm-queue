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

from DepQueue import DepQueue
import datetime
import time # for sleep
import sys

# all the args excluding this executable constitute a "main" job
# without args, just monitors the queue
def main():
	if not DepQueue.has_beanstalkc:
		print "The beanstalkc module is required for "+sys.argv[0]
		sys.exit(1)

	queue = DepQueue()
	queue.connect()

	param_idx = 1
	# If the first parameter starts with '-', the job arguments follow the -c flag
	try:
		if sys.argv[1].startswith('-'):
			param_idx = sys.argv.index('-c') + 1
	except (ValueError,IndexError):
		pass

	params = sys.argv[param_idx:]
	
	if len (params) < 1:
		print "No jobs specified"
	else:
		params[0] = DepQueue.which(params[0])

	if len (params) > 0 and params[0] is None:
		print "First parameter","'"+params[0]+"'","is not an executable file."
		print "exiting..."
		sys.exit(0)

	if len(params) == 1:
		print "No parameters specified for",params[0]
		print "exiting..."
		sys.exit(0)

	if len(params) > 1:
		print "Adding job:"," ".join (params)
		queue.add_job (params)

	while True:
		queue.get_stats()
		print str(datetime.datetime.now().replace(microsecond=0))
		print "Total workers:  ",queue.total_workers
		print "Idle workers:   ",queue.idle_workers
		print "Busy workers:   ",queue.busy_workers
		print "Jobs with deps: ",queue.jobs_with_deps
		print "Jobs left:      ",queue.jobs_left
		for job_id, job_info in queue.job_stats.items():
			print "    ID:", job_id, "working: ", job_info['working_workers'], "idle: ", job_info['idle_workers'], "deps: ", job_info['n_deps'], "left: ", job_info['n_deps_left']
		print "-------------------------"
		time.sleep (10)
if __name__ == "__main__":
    main()

