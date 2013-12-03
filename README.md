wndchrm-queue
=============

A distributed wndchrm queue, using the DepQueue dependency queue

DepQueue implements a dependency queue on top of [beanstalkd](http://kr.github.io/beanstalkd/), allowing a submitted job to generate dependent jobs which must run before
the original job is run.  Accomplished with two call-backs:

  * `run_job_callback()`: Runs the specified job
  * `add_job_deps_callback()`: Does whatever necessary to determine dependencies for the specified job, and adds them to the queue as dependent jobs

Files:

  * DepQueue.py includes the DepQueue class and the QueueMain class to instantiate a specified number of workers (uses python's multiprocessing).
  * DepQueue-submit.py has an example of submitting a job (using `queue.add_job(job_param_list)`) and monitoring queue status (using `queue.get_stats()`)
  * wndchrm-worker.py is a worker daemon defining callbacks specific for wndchrm jobs.

A simple example defining job callbacks, submitting a job and running it with multiple workers:

    import subprocess, sys
    from DepQueue import DepQueue, QueueMain

    def run_job_callback (queue, job_id, job_params):
    	# job_params is just like sys.argv[]
    	(out, err) = subprocess.Popen(job_params, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

    def add_job_deps_callback (queue, dep_tube, job_id, job_params):
    	# job_params is the sys.argv[] of the submitted job
    	# do something to get dependencies for the specified job
    	# dep_jobs = get_job_deps(job_params)
    	ndeps = 0
    	proc = subprocess.Popen(job_params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    	for job in dep_jobs:
    		queue.add_dependent_job (dep_tube, job)
    		n_deps += 1
    	return ndeps

    def main():
    	main_scope = QueueMain(run_job_callback, add_job_deps_callback)
    	main_scope.queue.add_job (sys.argv[1:]) # Don't add ourselves - argv[0]
 
    	# QueueMain takes care of the signal handling to clean up children, relaunch dead ones, etc.
    	main_scope.launch_workers()
    	# this process is forked and workers end up in QueueMain.work()
    	# We are still in the parent, so if we terminate, our child processes (the workers doing the work) will terminate also
    	# Note that the queue will only terminate on errors, not when it runs out of jobs.
    	while (not QueueMain.terminating):
    		time.sleep (100)

    if __name__ == "__main__":
    	main()
