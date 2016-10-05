#!/usr/bin/python

import time, os
import subprocess

# total job threshold to be able to handle overload problems
himster_total_job_threshold = 1500
# chop all jobs into bunches of 100 which is max job array size on himster atm
max_jobarray_size = 100
# resubmit sleep time in seconds
resubmit_sleep_time_in_seconds=1800

class ClusterJobs:
    job_range_low=1
    job_range_high=1
    num_nodes=1
    num_proc_per_node=1
    # in case you do not want to use local file io put the following value to 0
    local_file_quota_in_mb=200
    job_variables=dict()
    
    def __init__(self, called_script_name_, job_name_, job_log_dir_, job_walltime_):
        self.called_script_name = called_script_name_
        self.job_name = job_name_
        self.job_log_dir = job_log_dir_
        self.job_walltime = job_walltime_
    
    def insertJobVariable(self, name, value):
        self.job_variables[name]=str(value)

    def createClusterCommandList(self):
      job_list=[]
      for job_index in range(self.job_range_low, self.job_range_high + 1, max_jobarray_size):
        command = 'qsub -t ' + str(job_index) + '-' + str(min(job_index + max_jobarray_size - 1, self.job_range_high)) + ' -N ' + self.job_name \
                    + ' -l nodes=' + str(self.num_nodes) + ':ppn=' + str(self.num_proc_per_node) + ',walltime=' + self.job_walltime
                    
        if self.local_file_quota_in_mb > 0:
          self.insertJobVariable('use_local_file', 1)
          command = command + ',file=' + str(self.local_file_quota_in_mb) + 'mb'
        
        command = command + ' -j oe -o ' + self.job_log_dir + '/' + self.job_name + '.log -V'
        
        first=True
        for name, value in self.job_variables.iteritems():
          prefix=','
          if first:
            prefix=' -v '
            first=False
          command = command + prefix + name + '="' + value+'"'
        
        command = command + ' ./' + self.called_script_name
        job_list.append(command)
        return job_list

# check number of jobs currently running or in queue on himster from my side
def getNumJobsOnHimster():    
    bash_submit_command = 'qstat -t | wc -l'
    returnvalue = subprocess.Popen(bash_submit_command, shell=True, stdout=subprocess.PIPE)
    out, err = returnvalue.communicate()
    return int(out)

# check if a command is known on this machine
def executableExists(fpath):
  return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

def submitJobsToCluster(cluster_jobs):
  print 'preparing job arrays in index range ' + str(cluster_jobs.job_range_low) + ' - ' + str(cluster_jobs.job_range_high)

  program = 'qsub'
  is_cluster = 0
  for path in os.environ["PATH"].split(os.pathsep):
    path = path.strip('"')
    exe_file = os.path.join(path, program)
    if executableExists(exe_file):
      is_cluster = 1

  failed_submit_commands = []

  if is_cluster:
    print 'This is a cluster environment... submitting jobs to cluster!'
  
    joblist = cluster_jobs.createClusterCommandList()
  
    for job in joblist:
      bash_submit_command = job
      jobs_on_himster = getNumJobsOnHimster()

      if getNumJobsOnHimster() < himster_total_job_threshold:
        returnvalue = subprocess.call(bash_submit_command.split())
        if returnvalue > 0:
          print 'There was some problem submitting to the cluster... This job array is put into the the queue.'
          failed_submit_commands.append(bash_submit_command)
        else:
          time.sleep(5) # sleep 5 sec to make the queue changes active
      else:
        print 'Exceeded the total job threshold (' + str(getNumJobsOnHimster()) + '/' + str(himster_total_job_threshold) + ')! This job array is put into the the queue.'
        failed_submit_commands.append(bash_submit_command)
  else:
    print 'This is not a cluster environment! Please make sure to run this script on a cluster frontend node!' 

  while failed_submit_commands:
    print 'There are ' + str(len(failed_submit_commands)) + ' jobs arrays that were not accepted!'
  
    bash_submit_command = failed_submit_commands.pop(0)
    print 'trying to resubmit ' + bash_submit_command
  
    if getNumJobsOnHimster() > himster_total_job_threshold:
      returnvalue = 1
    else:
      returnvalue = subprocess.call(bash_submit_command.split())
    # if we failed to submit it again
    if returnvalue > 0:
      # put the command back into the list 
      failed_submit_commands.insert(0, bash_submit_command)
      # and sleep for 30 min
      print 'waiting for ' + resubmit_sleep_time_in_seconds/60 + ' min and then trying another resubmit...'
      time.sleep(resubmit_sleep_time_in_seconds) #sleep for some time
