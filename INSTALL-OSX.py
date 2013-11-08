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

import socket # for socket.gethostbyaddr
import subprocess
import sys
import os
from sys import platform
import io
import socket
import ConfigParser

if (os.geteuid() == 0):
	print "Do not run this script as root or with sudo."
	sys.exit(1)

if not platform == "darwin":
	print "This script is meant to run on OS X (platform = '"+platform+"', expecting 'darwin')"
	sys.exit(1)



def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


def check_pip():
	(out, err) = subprocess.Popen(["command", "-v", "pip"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print 'it is recommended to install the python module manager pip'
		print 'sudo easy_install pip'
		ans = raw_input("OK to execute above command? y or n [n] : ")
		if ans in ['y', 'Y']:
			subprocess.call(['sudo', 'easy_install', 'pip'])
	else:
		return

	(out, err) = subprocess.Popen(["command", "-v", "pip"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print "pip is required to continue.  Please install it manually."
		sys.exit(1)

def check_gcc():
	(out, err) = subprocess.Popen(["command", "-v", "gcc"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print "There does not appear to be a gcc compiler installed."
		print "Please install either the official developer command line tools from Apple:"
		print "https://developer.apple.com/downloads/"
		print "  * You will need an Apple ID (free)."
		print "  * Search for 'command line tools' and download the version appropriate for your OS."
		print "Or, use one of the installers here:"
		print "https://github.com/kennethreitz/osx-gcc-installer/"
		sys.exit(1)

def check_homebrew():
	(out, err) = subprocess.Popen(["command", "-v", "brew"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print "On OS X, it is recommended to first install homebrew (http://brew.sh/):"
		print 'ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"'
		ans = raw_input("OK to execute above command? y or n [n] : ")
		if ans in ['y', 'Y']:
			subprocess.call(['sh', '-c', 'ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"'])
	else:
		return

	(out, err) = subprocess.Popen(["command", "-v", "brew"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print "homebrew is required to continue.  Please install it manually."
		sys.exit(1)

def get_host_fqdn():
	hostname = socket.gethostname()
	fqdn = ""

	try:
		fqdn = socket.gethostbyaddr(socket.gethostname())[0]
		if (len(fqdn) > 3):
			return fqdn
	except:
		pass
		
	(out, err) = subprocess.Popen(["sh", "-c", "nslookup '"+hostname+"' | grep Name"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip().split()) > 1):
		fqdn = out.strip().split()[1]
		return fqdn

	(out, err) = subprocess.Popen(["scutil", "--get", "ComputerName"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	out = out.strip()
	if (len(out) > 3 and hostname != out):
		hostname = out
		(out, err) = subprocess.Popen(["sh", "-c", "nslookup '"+hostname+"' | grep Name"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
		if (len(out.strip().split()) > 1):
			fqdn = out.strip().split()[1]
			return fqdn
	
	fqdn = socket.gethostbyname(socket.gethostname())
	return fqdn
		

conf_path = '/etc/wndchrm/wndchrm-queue.conf'
def make_config():
	global conf_path

	(out, err) = subprocess.Popen(["command", "-v", "wndchrm"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	wndchrm_executable = out.strip()
	ans = raw_input("wndchrm executable ["+wndchrm_executable+"] : ")
	if (len (ans)): wndchrm_executable = ans

	(out, err) = subprocess.Popen(["sysctl", "-n", "hw.physicalcpu"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	num_workers = int (out.strip()) - 1
	ans = raw_input("Number of wndchrm workers ["+str(num_workers)+"] : ")
	if (len (ans)): num_workers = int(ans)

	beanstalkd_host = 'localhost'
	ans = raw_input("Beanstalk host ["+beanstalkd_host+"] : ")
	if (len (ans)): beanstalkd_host = ans

	beanstalkd_port = '11300'
	ans = raw_input("Beanstalk port ["+beanstalkd_port+"] : ")
	if (len (ans)): beanstalkd_port = ans

	worker_host = get_host_fqdn()
	ans = raw_input("Worker host (this hostname) ["+worker_host+"] : ")
	if (len (ans)): worker_host = ans

	worker_PID_dir = '/var/run/wndchrm'
	worker_log = '/var/log/wndchrm-workers.log'
	beanstalkd_tube = 'wndchrm'
	beanstalkd_wait_retry = '30'
	beanstalkd_retries = '10'
	
	conf_dir, conf_file = os.path.split(conf_path)
	if not os.path.isdir(conf_dir):
		print "Creating "+conf_dir+ " as sudo..."
		subprocess.call(['sudo', 'mkdir', '-p', conf_dir])
	tmp_file = '/tmp/'+conf_file
	with open(tmp_file, 'w') as the_file:
		the_file.write("wndchrm_executable    = "+wndchrm_executable+"\n")
		the_file.write("num_workers           = "+str(num_workers)+"\n")
		the_file.write("beanstalkd_host       = "+beanstalkd_host+"\n")
		the_file.write("beanstalkd_port       = "+beanstalkd_port+"\n")
		the_file.write("worker_host           = "+worker_host+"\n")
		the_file.write("beanstalkd_tube       = "+beanstalkd_tube+"\n")
		the_file.write("beanstalkd_wait_retry = "+beanstalkd_wait_retry+"\n")
		the_file.write("beanstalkd_retries    = "+beanstalkd_retries+"\n")
		the_file.write("worker_PID_dir        = "+worker_PID_dir+"\n")
		the_file.write("worker_log            = "+worker_log+"\n")

	print "Creating "+conf_path+ " as sudo..."
	subprocess.call(['sudo', 'mv', tmp_file, conf_path])

	print "Configuration file in "+conf_path
	print "----------------------------------"
	print open(conf_path, 'r').read().strip()
	print "----------------------------------"
	print ""
	if (beanstalkd_host == 'localhost' or beanstalkd_host == '127.0.0.1'):
		print "Remote workers can connect to the local beanstalkd server using:"
		print "beanstalkd_host       = "+get_host_fqdn()
	ans = raw_input("Change any settings in a separate editor and hit return.")


def check_config():
	global conf_path
	if not os.path.isfile(conf_path):
		make_config()
	config = ConfigParser.SafeConfigParser()
	config.readfp(io.BytesIO('[conf]\n'+open(conf_path, 'r').read()))
	conf = {
		'wndchrm_executable'    : None,
		'num_workers'           : None,
		'beanstalkd_host'       : None,
		'beanstalkd_port'       : None,
		'worker_host'           : None,
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
	for k in conf.keys():
		if (k in conf_ints):
			conf[k] = int (config.get("conf", k))
		else:
			conf[k] = config.get("conf", k)
	return conf

	

has_PyYAML = False
try:
    import yaml
    has_PyYAML = True
except ImportError:
	print "The PyYAML module is required for the beanstalkc module"
	check_gcc()
	check_homebrew()

#
# Install PyYAML using homebrew
# 	if (not os.access(get_python_lib(), os.W_OK))
# 		print "For brew to install Python modules, it is recommended to make "+get_python_lib()+" writeable by the admin group"
# 		print "sudo chown -R :admin "+get_python_lib()
# 		print "sudo chmod -R g+w "+get_python_lib()
# 		ans = raw_input("OK to execute above commands? y or n [n] : ")
# 		if ans in ['y', 'Y']:
# 			subprocess.call("sudo chown -R :admin "+get_python_lib())
# 			subprocess.call("sudo chmod -R g+w "+get_python_lib())
# 	print "install PyYAML on OS X using homebrew:"
# 	print "brew tap sampsyo/py"
# 	print "brew install PyYAML"
# 	ans = raw_input("OK to execute above commands? y or n [n] : ")
# 	if ans in ['y', 'Y']:
# 		subprocess.call("brew tap sampsyo/py")
# 		subprocess.call("brew install PyYAML")

	(out, err) = subprocess.Popen(["brew", "list", "libyaml"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
	if (len(out.strip()) < 3):
		print 'The python PyYAML module depends on libyaml'
		print 'brew install libyaml'
		ans = raw_input("OK to execute above command? y or n [n] : ")
		if ans in ['y', 'Y']:
			subprocess.call(['brew', 'install', 'libyaml'])
		else:
			print "Please install libyaml manually and try again."
			sys.exit(1)

	check_pip()

	print 'sudo pip install PyYAML'
	ans = raw_input("OK to execute above command? y or n [n] : ")
	if ans in ['y', 'Y']:
		subprocess.call(['sudo', 'pip', 'install', 'PyYAML'])
		import yaml
		has_PyYAML = True
	else:
		print "Please install PyYAML manually and try again."
		sys.exit(1)
	

	
#
# Check beanstalkc python module
has_beanstalkc = False
try:
	import beanstalkc
	has_beanstalkc = True
except ImportError:
	check_pip()

	print 'sudo pip install beanstalkc'
	ans = raw_input("OK to execute above command? y or n [n] : ")
	if ans in ['y', 'Y']:
		subprocess.call(['sudo', 'pip', 'install', 'beanstalkc'])
		import beanstalkc
		has_beanstalkc = True
	else:
		print "Please install beanstalkc manually and try again."
		sys.exit(1)

#
# Check beanstalkd
(out, err) = subprocess.Popen(["command", "-v", "beanstalkd"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
if (len(out.strip()) < 3):
	print "A beanstalkd server is required on the network.  Not necessarily on this machine."
	ans = raw_input("Install beanstalkd locally? y or n [n] : ")
	if ans in ['y', 'Y']:
		subprocess.call(['brew', 'install', 'beanstalkd'])
		ans = raw_input("Launch beanstalk automatically on login? y or n [n] : ")
		if ans in ['y', 'Y']:
			subprocess.call(['sh', '-c', 'ln -sfv /usr/local/opt/beanstalk/*.plist ~/Library/LaunchAgents'])
			subprocess.call(['sh', '-c', 'launchctl load ~/Library/LaunchAgents/homebrew.mxcl.beanstalk.plist'])

#
# Check config
conf = check_config()
try:
	beanstalk = beanstalkc.Connection(host=conf['beanstalkd_host'], port=conf['beanstalkd_port'])
	beanstalk.use('test')
	beanstalk.watch('test')
	beanstalk.put('test')
	job = beanstalk.reserve(timeout=0)
	if (not job or job.body != 'test'):
		print "Couldn't retrieve submitted job on tube 'test', server "+conf['beanstalkd_host']+" port "+str(conf['beanstalkd_port'])
except beanstalkc.SocketError:
	print "No beanstalk server appaears to be running on "+conf['beanstalkd_host']+" port "+str(conf['beanstalkd_port'])


