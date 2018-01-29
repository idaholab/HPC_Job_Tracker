#!/usr/bin/env python

"""
Copyright 2017 Battelle Energy Alliance, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Description
-----------
PBS job tracking executable wrapper.
Collects Memory and load information for all nodes associated with a PBS job.
Dumps data in directory (job_tracker_PBS_JOBID) in current working directory.
Also has reporting and plotting capabilities.

Usage: job_tracker -h

Date Created: Fri Jan 29 10:52:56 MST 2016

Author: Cormac Garvey

"""

import subprocess
import re
import sys
import os
import socket
import csv
import time
import argparse
import tempfile
import glob


MPI_CMD_LIST = ['mpirun', 'mpiexec', 'mpirun_rsh', 'mpiexec_mpt']



def getComputeNodeType(compute_node_name):
    if re.search('^r6i', compute_node_name) is not None:
       return "broadwell"
    elif re.search('^fission-', compute_node_name) is not None:
       return "fission"
    elif re.search('^bechler', compute_node_name) is not None:
       return "bechler"
    else:
       return "haswell"


def getNumberComputeCores(compute_node_type):
    if compute_node_type == "haswell":
       number_compute_node_cores = 24
    elif compute_node_type == "broadwell":
       number_compute_node_cores = 36
    elif compute_node_type == "fission":
       number_compute_node_cores = 32
    elif compute_node_type == "bechler":
       number_compute_node_cores = 32
    else:
       sys.exit("Error: Do not recognize compute_node_type (%s)" % compute_node_type)
    return number_compute_node_cores


class ExecuteCmd(object):

      def __init__(self, cmd):
          self.f = ExecuteCmd.getFileHandler(self, cmd)

      def getFileHandler(self, cmd):
          return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout



class CollectAgent(object):
  
      def __init__(self, exe_pattern, pbsjobid):
          self.exe_pattern = exe_pattern
#          print self.exe_pattern
          self.pbsjobid = pbsjobid
          self.pids = []
          self.collect = False
          self.data = CollectAgent.getData(self)


      def getData(self):
          job_memory = CollectAgent.getJobMemory(self)
          node_memory = CollectAgent.getNodeMemory(self)
          node_load = CollectAgent.getNodeLoad(self)
          cgroup_memory = CollectAgent.getCgroupMemory(self)
#          print node_load
          tasklayout = CollectAgent.getTaskLayout(self)
#          print tasklayout
          return [time.time(), job_memory, int(node_memory), float(node_load), cgroup_memory] + tasklayout


      def getNodeMemory(self):
          free_cmd = ExecuteCmd("free")
          for free_str in free_cmd.f:
              if 'buffers/cache' in free_str:
                 data = free_str.split(':')
          return data[-1].split()[0]


      def getNodeLoad(self):
          load_cmd = ExecuteCmd("uptime")
          for uptime_str in load_cmd.f:
              data = uptime_str.split(',')
          return data[-3].split(':')[1]


      def getJobMemory(self):
          total_job_mem = 0
#          print "(getJobMemory)self.command_args.executable_name=",self.command_args.executable_name,socket.gethostname()
          ps_cmd = ExecuteCmd("ps -e -o pid,rss,args")
          for ps_str in ps_cmd.f:
#              print ps_str
              if self.exe_pattern.search(ps_str) is not None and __file__ not in ps_str and not CollectAgent.foundMpiCmd(self, ps_str) and re.search('/sh\s',ps_str) is None:
#              if self.command_args.executable_name in ps_str and __file__ not in ps_str and not CollectAgent.foundMpiCmd(self, ps_str):
                 data = ps_str.split()
                 total_job_mem = total_job_mem + int(data[1])
                 pid = data[0]
                 if pid not in self.pids:
                    self.pids.append(pid)
                 self.collect = True
          return total_job_mem


      def getCgroupMemory(self):
          cgroup_used_mem_file = "/cgroup/memory/pbspro/"+self.pbsjobid+"/memory.usage_in_bytes"
          myCommand = "cat %s" % (cgroup_used_mem_file)
          f = subprocess.Popen(myCommand,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
          for line in f:
              return int(line.strip())/1024


      def getTaskLayout(self):
          tasklayout = []
          for pid in self.pids:
#              print pid
              pathname = os.path.join("/proc",pid,"task")
              for pid_dir in os.listdir(pathname):
                  pathname2 = os.path.join(pathname,pid_dir)
                  if os.path.isdir(pathname2):
                     f = open(os.path.join(pathname2,"stat"))
                     line = f.read()
                     data = line.split()
                     physical_id = int(data[-6])
                     state = data[2]
                     if state == "R":
#                        print physical_id
                        tasklayout.append(pid_dir)
                        tasklayout.append(physical_id)
          return tasklayout
                  

      def foundMpiCmd(self, ps_str):
          found = False
          for mpi_cmd in MPI_CMD_LIST:
              if mpi_cmd in ps_str:
                 found = True
                 break
          return found


class CollectAgent2(object):
  
      def __init__(self):
#          print self.exe_pattern
          self.pids = []
          self.collect = True
          self.data = CollectAgent2.getData(self)


      def getData(self):
          node_memory = CollectAgent2.getNodeMemory(self)
          node_load = CollectAgent2.getNodeLoad(self)
          return [time.time(), int(node_memory), float(node_load)]


      def getNodeMemory(self):
          free_cmd = ExecuteCmd("free")
          for free_str in free_cmd.f:
              if 'buffers/cache' in free_str:
                 data = free_str.split(':')
          return data[-1].split()[0]


      def getNodeLoad(self):
          load_cmd = ExecuteCmd("uptime")
          for uptime_str in load_cmd.f:
              data = uptime_str.split(',')
          return data[-3].split(':')[1]


class CommandArgs(object):

      def __init__(self):
          parser = argparse.ArgumentParser(description="Job Tracking wrapper")
          self.args = CommandArgs.getArgs(self, parser)
#          print self.args
          self.exe_args = " ".join(self.args.exe_args)
          self.exe_args = self.args.exe_args
          self.home = os.getenv('HOME')
#          print "(CommandArgs) self.exe_args = ", self.exe_args
          if self.args.exe_args:
             self.args = self.replace_args()
             self.executable_path,self.executable_name = CommandArgs.findExecutable(self, self.args.exe_args[0].split())
             self.exe_pattern = self.exePattern()
             print "(CommandArgs) self.executable_name=",self.executable_name,self.exe_pattern


      def replace_args(self):
          new_args = self.args
          new_args.exe_args[0] = self.args.exe_args[0].replace('$HOME',self.home)
#          print "new_args.exe_args[0]=",new_args.exe_args[0]
          return new_args


      def exePattern(self):
          if re.search('abaqus',self.executable_name) is not None:
             exe_pattern = re.compile('pre|standard|explicit')
          elif re.search('starccm',self.executable_name) is not None:
             exe_pattern = re.compile('star-ccm')
          else:
             exe_pattern = re.compile(self.executable_name)
          return exe_pattern


      def getArgs(self, parser):
          tracker_group = parser.add_argument_group('Job Tracking', 'The following options control how the job_tracker tracks memory usage/load')
          tracker_group.add_argument('--interval', metavar='float', type=float, default=0.75, nargs=1, help='Sleep interval between data collection.')
          tracker_group.add_argument('exe_args', metavar='command', nargs='*', help='Executable and arguments(if any), must be enclosed in quotes')
          internal_group = parser.add_argument_group('Internal', 'Internal options (Do not use)')
          internal_group.add_argument('--pbsjobid', metavar='internal', nargs=1, help='Internal option.')
          internal_group.add_argument('--node_mem_load_only', action='store_true', help='Internal option.')
          internal_group.add_argument('--collect', action="store_true", help='Internal option.')
          internal_group.add_argument('--exe_pattern', metavar='internal', nargs=1, help='Internal option.')
          internal_group.add_argument('--collection_time', metavar='float', type=float, nargs=1, help='Internal option.')
          internal_group.add_argument('--cwd', metavar='internal', nargs=1, help='Internal option.')
          general_group = parser.add_argument_group('General options', 'The following options are used in combination with other arguments')
          general_group.add_argument('--rawdata', metavar='dir', nargs=1, help='Specify directory containing raw job tracking data.')
          report_group = parser.add_argument_group('Generate text report', 'The following options control how the report is generated')
          report_group.add_argument('--report', action='store_true',help='Generate a text report, make sure you specify the directory containing raw job tracking data. (--rawdata)')
          plot_group = parser.add_argument_group('Generate plot data and graphs', 'The following options control how plot data is generated and plotted')
          plot_group.add_argument('--gen_plot_data', metavar="filename", nargs=1, help='Generate plot data files from the raw tracking data, specify a filename for the generated plot data file. (Make sure you specify the location of the raw tracking data (--rawdata).')
          plot_group.add_argument('--plot_data', metavar='plot_file', nargs='*', help='Plot data files, specify the plot files to be plotted')
          return parser.parse_args() 


      def findExecutable(self, exe_args):
#          print exe_args
          indx = 0
          if exe_args[0] == 'time':
             indx = 1
          if exe_args[indx] in MPI_CMD_LIST:
             executable = self.findExeStr(exe_args[indx+1:])
#             print "(findExecutable) executable=",executable
#             print which(executable),os.path.exists(which(executable))
             if os.path.exists(which(executable)):
                return os.path.split(executable)
          else:
             return os.path.split(exe_args[indx].split()[0])


      def findExeStr(self, arg_list):
#          print "(findExeStr) arg_list=",arg_list
          if re.search('^-',arg_list[0]) is None:
             return arg_list[0]
          for arg in arg_list:
              if re.search('mcnp|abaqus|vasp', arg) is not None:
                 return arg
          sys.exit('Error: Cannot find executable string in command args')



class Pbs(object):

      def __init__(self):
          self.hostlist = Pbs.getHostList(self)
          self.jobid = os.getenv('PBS_JOBID')
          self.jobname = os.getenv('PBS_JOBNAME')
#          self.home = os.getenv('HOME')


      def getHostList(self):
          tmp_node_list = []
          pbs_nodefile = os.getenv('PBS_NODEFILE')
          if pbs_nodefile != None:
#             self.rank0 = True
             f = open(pbs_nodefile, 'r')
#             return list(set(f.read().split()))
             return Pbs.remove_duplicates(self, f.read().split())
          else:
             sys.exit("Error: Could not find PBS_NODEFILE, need to run inside PBS(Interactive PBS node or PBS script)")


      def remove_duplicates(self, values):
          output = []
          seen = set()
          for value in values:
              if value not in seen:
                 output.append(value)
                 seen.add(value)
          return output


class DataCollector2(object):
    def __init__(self, command_args):
        print command_args.args.exe_pattern
        self.command_args = command_args
        self.hostname = socket.gethostname()
        self.pbsjobid = self.command_args.args.pbsjobid[0]
#        print self.hostname
#        print self.command_args.args.pbsjobid
        if not self.command_args.args.collect:
           self.cwd = os.getcwd()
           self.directory = os.path.join(self.cwd,"job_tracker_"+self.command_args.args.pbsjobid[0])
           if not os.path.exists(self.directory):
              os.mkdir(self.directory)
#           print self.directory
           self.filename = os.path.join(self.directory,self.hostname + '.csv')
           self.hostlist = self.get_hostlist()
#           print self.hostlist
           if self.command_args.args.node_mem_load_only:
              self.start_scripts2()
           else:
              self.start_scripts()
        else:
           self.cwd = self.command_args.args.cwd[0]
           self.directory = os.path.join(self.cwd,"job_tracker_"+self.command_args.args.pbsjobid[0])
           self.filename = os.path.join(self.directory,self.hostname + '.csv')
           if self.command_args.args.node_mem_load_only:
              self.start_collecting2()
           else:
              self.start_collecting()


    def start_scripts(self):
        for node in self.hostlist:
            if self.command_args.args.collection_time:
               cmd = 'ssh ' + node + ' \''+ 'source /etc/profile.d/modules.sh && module load use.projects utils && ' + __file__ + ' --pbsjobid ' + self.command_args.args.pbsjobid[0] + ' --collection_time ' + str(self.command_args.args.collection_time[0]) + ' --exe_pattern ' + '\"'+self.command_args.args.exe_pattern[0]+'\"' + ' --collect --cwd ' + self.cwd+'\''
            else:
               cmd = 'ssh ' + node + ' \''+ 'source /etc/profile.d/modules.sh && module load use.projects utils && ' + __file__ + ' --pbsjobid ' + self.command_args.args.pbsjobid[0] + ' --exe_pattern ' + '\"'+self.command_args.args.exe_pattern[0]+'\"' + ' --collect --cwd ' + self.cwd+'\''
#            print "(start_scripts) cmd=",cmd
            f_o = open(os.path.join(self.directory,node+'job_tracker_script_'+self.command_args.args.pbsjobid[0]+'_out'),'w')
            f_e = open(os.path.join(self.directory,node+'job_tracker_script_'+self.command_args.args.pbsjobid[0]+'_err'),'w')
            subprocess.Popen(cmd, shell=True, stdout=f_o, stderr=f_e)
#            subprocess.Popen(cmd, shell=True, stdout=tempfile.TemporaryFile(), stderr=tempfile.TemporaryFile())
#            subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def start_scripts2(self):
        for node in self.hostlist:
            if self.command_args.args.collection_time:
               cmd = 'ssh ' + node + ' \''+ 'source /etc/profile.d/modules.sh && module load use.projects utils && ' + __file__ + ' --pbsjobid ' + self.command_args.args.pbsjobid[0] + ' --collection_time ' + str(self.command_args.args.collection_time[0]) + ' --node_mem_load_only  --collect --cwd ' + self.cwd+'\''
            else:
               cmd = 'ssh ' + node + ' \''+ 'source /etc/profile.d/modules.sh && module load use.projects utils && ' + __file__ + ' --pbsjobid ' + self.command_args.args.pbsjobid[0] + ' --node_mem_load_only --collect --cwd ' + self.cwd+'\''
#            print "(start_scripts) cmd=",cmd
            f_o = open(os.path.join(self.directory,node+'job_tracker_script_'+self.command_args.args.pbsjobid[0]+'_out'),'w')
            f_e = open(os.path.join(self.directory,node+'job_tracker_script_'+self.command_args.args.pbsjobid[0]+'_err'),'w')
            subprocess.Popen(cmd, shell=True, stdout=f_o, stderr=f_e)
#            subprocess.Popen(cmd, shell=True, stdout=tempfile.TemporaryFile(), stderr=tempfile.TemporaryFile())
#            subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def start_collecting(self):
        collect = True
        f = open(self.filename,'wb',1)
        job_writer = csv.writer(f)
        cnt = 0
#        print self.command_args.args.exe_pattern
        while(collect):
           collect_agent = CollectAgent(re.compile(self.command_args.args.exe_pattern[0]),self.pbsjobid)
           if self.command_args.args.collection_time:
#              print "collection_time arg set to",self.command_args.args.collection_time[0]
#              print cnt * self.command_args.args.interval
              if cnt * self.command_args.args.interval > self.command_args.args.collection_time[0]:
                 collect = False
           elif (cnt > 10 and not collect_agent.collect):
              collect = False
           job_writer.writerow(collect_agent.data)
           time.sleep(self.command_args.args.interval)
           cnt = cnt + 1
        f.close()


    def start_collecting2(self):
        collect = True
        f = open(self.filename,'wb',1)
        job_writer = csv.writer(f)
        cnt = 0
#        print self.command_args.args.exe_pattern
        while(collect):
           collect_agent = CollectAgent2()
           if self.command_args.args.collection_time:
#              print "collection_time arg set to",self.command_args.args.collection_time[0]
#              print cnt * self.command_args.args.interval
              if cnt * self.command_args.args.interval > self.command_args.args.collection_time[0]:
                 collect = False
           elif (cnt > 10 and not collect_agent.collect):
              collect = False
           job_writer.writerow(collect_agent.data)
           time.sleep(self.command_args.args.interval)
           cnt = cnt + 1
        f.close()



    def get_hostlist(self):
#         myCommand = "qstat -x -n -1 %s" % (self.command_args.args.pbsjobid[0])
        myCommand = "qstat -n -1 %s" % (self.command_args.args.pbsjobid[0])
#        print myCommand
        f = subprocess.Popen(myCommand,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
        for line in f:
#            print line
            if line.startswith(self.command_args.args.pbsjobid[0][:-1]):
#               re.match("\d+\.",line) is not None:
               data = re.split("\s*",line.strip())
#               print data
               compute_nodes_str = data[-1]
#               print compute_nodes_str
               compute_nodes_list = self.parse_compute_nodes_str(compute_nodes_str)
        f.close()
        return compute_nodes_list


    def parse_compute_nodes_str(self,compute_nodes_str):
#        print compute_nodes_str
        compute_nodes_list = []
        compute_nodes_str_list = re.split("\+",compute_nodes_str.strip())
        for node_str in compute_nodes_str_list:
            compute_node_str_list = re.split("/",node_str.strip())
            compute_node_str = compute_node_str_list[0]
            compute_nodes_list.append(compute_node_str)
        return list(set(compute_nodes_list))



class DataCollector(object):

    def __init__(self, command_args):
        self.command_args = command_args
        self.hostname = socket.gethostname()
        self.cwd = os.getcwd()
        if not self.command_args.args.pbsjobid:
           self.pbs = Pbs()
           self.pbsjobid = self.pbs.jobid
#           self.compute_node_type = getComputeNodeType(self.pbs.hostlist[0])
           self.command_args = self.replace_args()
#           print self.command_args.args
#           self.executable_path,self.executable_name = self.findExecutable(self.command_args.args.exe_args[0].split())
#           self.exe_pattern = self.exePattern()
#           print "(DataCollector) self.executable_name=",self.executable_name, self.exe_pattern
           self.directory = os.path.join(self.cwd,"job_tracker_"+self.pbs.jobid)
           print("\n\n Job tracker has started collecting data, raw job tracking data will be deposited in %s" % self.directory)
           print("\n After job has completed you can view your report with the following command (job_tracker.py --rawdata %s --report)" % self.directory)
           print("\n or you can plot your data by first generating the plot data(job_tracker --rawdata %s --gen_plot_data filename)" % self.directory)
           print("\n and then plotting your data job_tracker --plot_data plot_data_file(s))")
           print("\n To see all options (job_tracker.py -h)\n")
           self.start_executable()
           self.start_scripts()
        else:
           self.pbsjobid = self.command_args.args.pbsjobid[0]
           self.directory = os.path.join(self.command_args.args.cwd[0],"job_tracker_"+self.command_args.args.pbsjobid[0])
        if not os.path.exists(self.directory):
           os.mkdir(self.directory)
        self.filename = os.path.join(self.directory,self.hostname + '.csv')
        self.start_collecting()


    def replace_args(self):
        new_args = self.command_args
        new_args.args.exe_args[0] = self.command_args.args.exe_args[0].replace('$PBS_JOBNAME',self.pbs.jobname)
        print "new_args.args.exe_args[0]=",new_args.args.exe_args[0]
        return new_args


    def findExecutable(self, exe_args):
#          print exe_args
          indx = 0
          if exe_args[0] == 'time':
             indx = 1
          if exe_args[indx] in MPI_CMD_LIST:
             executable = self.findExeStr(exe_args[indx+1:])
#             print "(findExecutable) executable=",executable
#             print which(executable),os.path.exists(which(executable))
             if os.path.exists(which(executable)):
                return os.path.split(executable)
          else:
             return os.path.split(exe_args[indx].split()[0])


    def findExeStr(self, arg_list):
#          print "(findExeStr) arg_list=",arg_list
          if re.search('^-',arg_list[0]) is None:
             return arg_list[0]
          for arg in arg_list:
              if re.search('mcnp|abaqus|vasp', arg) is not None:
                 return arg
          sys.exit('Error: Cannot find executable string in command args')


    def exePattern(self):
          if re.search('abaqus',self.executable_name) is not None:
             exe_pattern = re.compile('pre|standard|explicit')
          elif re.search('starccm',self.executable_name) is not None:
             exe_pattern = re.compile('star-ccm')
          else:
             exe_pattern = re.compile(self.executable_name)
          return exe_pattern


    def start_executable(self):
        if not os.path.exists(self.directory):
           os.mkdir(self.directory)
#        print "(start_executable) self.command_args.args.exe_args=",self.command_args.args.exe_args[0]
        f_o = open(os.path.join(self.directory,'job_tracker_exe_'+self.pbs.jobid+'_out'),'w')
        f_e = open(os.path.join(self.directory,'job_tracker_exe_'+self.pbs.jobid+'_err'),'w')
        subprocess.Popen(self.command_args.args.exe_args[0], shell=True, stdout=f_o, stderr=f_e)
        #subprocess.Popen(self.command_args.args.exe_args[0].split(), shell=False, stdout=f_o, stderr=f_e)
#        subprocess.Popen(self.command_args.args.exe_args, shell=False, stdout=tempfile.TemporaryFile(), stderr=tempfile.TemporaryFile())
#        subprocess.Popen(self.command_args.args.exe_args, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#        subprocess.Popen(self.arguments.run[-1].split(), stdout=self.agent.log, stderr=self.agent.log)


    def start_scripts(self):
        cur_dir_exe = os.path.join(self.cwd, self.command_args.executable_name)
        if os.path.exists(cur_dir_exe) and os.access(cur_dir_exe, os.X_OK) and self.command_args.exe_args[0].split()[0] not in MPI_CMD_LIST:
           full_exe_args = self.command_args.exe_args[0].replace(self.command_args.executable_name, cur_dir_exe)
        elif self.command_args.executable_path:
           full_exe_args = self.command_args.exe_args[0]
        else:
           full_exe_args = self.command_args.exe_args[0].replace(self.command_args.executable_name,which(self.command_args.executable_name))
#        print "(start_scripts) full_exe_args=",full_exe_args
        for node in self.pbs.hostlist[1:]:
            cmd = 'ssh ' + node + ' \''+ 'source /etc/profile.d/modules.sh && module load use.projects utils && ' + __file__ + ' --pbsjobid ' + self.pbs.jobid + ' --cwd ' + self.cwd + ' ' + '\"'+full_exe_args+'\"\''
#            print "(start_scripts) cmd=",cmd
            f_o = open(os.path.join(self.directory,node+'job_tracker_script_'+self.pbs.jobid+'_out'),'w')
            f_e = open(os.path.join(self.directory,node+'job_tracker_script_'+self.pbs.jobid+'_err'),'w')
            subprocess.Popen(cmd, shell=True, stdout=f_o, stderr=f_e)
#            subprocess.Popen(cmd, shell=True, stdout=tempfile.TemporaryFile(), stderr=tempfile.TemporaryFile())
#            subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    def start_collecting(self):
        collect = True
        f = open(self.filename,'wb',1)
        job_writer = csv.writer(f)
#        number_compute_node_cores = getComputeNodeCores(self.compute_node_type)
        cnt = 0
        while(collect):
           collect_agent = CollectAgent(self.command_args.exe_pattern, self.pbsjobid)
           if (cnt > 10 and not collect_agent.collect):
              collect = False
#           collect = collect_agent.collect
           job_writer.writerow(collect_agent.data)
           time.sleep(self.command_args.args.interval)
           cnt = cnt + 1
        f.close()
#        f = open(self.filename,'rb')
#        job_reader = csv.reader(f)
#        for row in job_reader:
#            print row


class RawData(object):

      def __init__(self, args):
          self.cwd = os.getcwd()
          if args.args.rawdata:
             self.dir_path = os.path.join(self.cwd,args.args.rawdata[0])
          else:
             sys.exit("Error: Need to specify rawdata directory (--rawdata dir)")
#          print self.dir_path
          self.primary_file = self.find_primary_file()
#          print "(RawData,__init__) self.primary_file=",self.primary_file
          self.rawdata_dict = self.rawDataDict()
          if args.args.node_mem_load_only:
             self.rawdata_dict = self.addPadding2()
             self.max_mem_load_dict = self.get_max_mem_load_dict2()
          else:
             self.rawdata_dict = self.addPadding()
             self.max_mem_load_dict = self.get_max_mem_load_dict()
#          sys.exit(0)
#          print self.rawdata_dict


      def rawDataDict(self):
          rawdata_dict = {}
          for file in glob.glob(self.dir_path + '/*.csv'):
              f = open(file,'rb')
              reader = csv.reader(f)
              node = os.path.split(file)[1][:-4]
              rawdata_dict[node] = []
              for row in reader:
                  rawdata_dict[node].append(row)
          return rawdata_dict


      def addPadding(self):
          rawdata_dict = {}
          for file in self.rawdata_dict:
              pad_num = self.find_pad_num(file)
              self.rawdata_dict[file] = [['0.0','0','0','0.0','0']]*pad_num + self.rawdata_dict[file]
#debug              if pad_num > 0:
#                 print pad_num
#                 print self.rawdata_dict[file][0:5]
          maxlen = 0
          for file in self.rawdata_dict:
#              print file, len(self.rawdata_dict[file])
              if len(self.rawdata_dict[file]) > maxlen:
                 maxlen = len(self.rawdata_dict[file])
#          print maxlen
          for file in self.rawdata_dict:
#              print file, len(self.rawdata_dict[file])
              self.rawdata_dict[file] = self.rawdata_dict[file] + [['0.0','0','0','0.0','0']]*(maxlen-len(self.rawdata_dict[file]))
          return self.rawdata_dict


      def addPadding2(self):
          rawdata_dict = {}
          for file in self.rawdata_dict:
              pad_num = self.find_pad_num(file)
              self.rawdata_dict[file] = [['0.0','0','0.0']]*pad_num + self.rawdata_dict[file]
#debug              if pad_num > 0:
#                 print pad_num
#                 print self.rawdata_dict[file][0:5]
          maxlen = 0
          for file in self.rawdata_dict:
#              print file, len(self.rawdata_dict[file])
              if len(self.rawdata_dict[file]) > maxlen:
                 maxlen = len(self.rawdata_dict[file])
#          print maxlen
          for file in self.rawdata_dict:
#              print file, len(self.rawdata_dict[file])
              self.rawdata_dict[file] = self.rawdata_dict[file] + [['0.0','0','0.0']]*(maxlen-len(self.rawdata_dict[file]))
          return self.rawdata_dict


      def find_pad_num(self, file):
          cnt = 0
#          print self.rawdata_dict[os.path.split(self.primary_file)[1][:-4]]
          for row in self.rawdata_dict[os.path.split(self.primary_file)[1][:-4]]:
#              print row[0],self.rawdata_dict[file][0][0]
              if float(row[0]) >= float(self.rawdata_dict[file][0][0]):
#                 print cnt
                 return cnt
              cnt = cnt + 1
          sys.exit("Error: Cound not find padding number")
                  


      def find_primary_file(self):
          primary_file = ""
          if os.path.exists(self.dir_path):
             cnt = 0
             for file in glob.glob(self.dir_path + '/*.csv'):
                 current_min_time = self.find_min_time(file)
                 if cnt == 0:
                    primary_file = file
                    min_time = current_min_time
#                 print current_min_time, min_time
                 if current_min_time < min_time:
                    min_time = current_min_time
                    primary_file = file
                 cnt = cnt + 1
          else:
             sys.exit('Error: could not find the directory (%s)'% self.dir_path)
          return primary_file
          

      def find_min_time(self, file):
          data_dict = {}
          try:
             f = open(file,'r')
             job_reader = csv.reader(f)
             first_row = next(job_reader)
             time = first_row[0]
          except IOError:
             sys.exit('Error: could not open file (%s)'% file)
          return time


      def get_max_mem_load_dict(self):
          report_dict = {}
          if os.path.exists(self.dir_path):
#             for file in os.listdir(self.dir_path):
#                 if re.search('csv$',file) is not None:
             for file in glob.glob(self.dir_path + '/*.csv'):
#                 print file
                 node = os.path.split(file)[1][:-4]
#                 print node
#                 report_dict[node] = {}
                 report_dict[node] = self.max_mem_load(file)
#                 print report_dict
          else:
             sys.exit('Error: could not find the directory (%s)'% self.dir_path)
#          print report_dict
          return report_dict


      def get_max_mem_load_dict2(self):
          report_dict = {}
          if os.path.exists(self.dir_path):
#             for file in os.listdir(self.dir_path):
#                 if re.search('csv$',file) is not None:
             for file in glob.glob(self.dir_path + '/*.csv'):
#                 print file
                 node = os.path.split(file)[1][:-4]
#                 print node
#                 report_dict[node] = {}
                 report_dict[node] = self.max_mem_load2(file)
#                 print report_dict
          else:
             sys.exit('Error: could not find the directory (%s)'% self.dir_path)
#          print report_dict
          return report_dict


      def max_mem_load(self, file):
          data_dict = {}
          try:
             f = open(file,'rb')
             job_reader = csv.reader(f)
             first_row = next(job_reader)
             time_delta = first_row[0]
             data_dict['max_job_mem'] =    (0.0, first_row[1])
             data_dict['max_node_mem'] =   (0.0, first_row[2])
             data_dict['max_node_load'] =  (0.0, first_row[3])
             data_dict['max_cgroup_mem'] = (0.0, first_row[4])
             for row in job_reader:
                 if int(row[1]) > int(data_dict['max_job_mem'][1]):
                    data_dict['max_job_mem'] = (float(row[0])-float(time_delta),int(row[1]))
#                    print data_dict['max_job_mem']
                 if int(row[2]) > int(data_dict['max_node_mem'][1]):
                    data_dict['max_node_mem'] = (float(row[0])-float(time_delta),int(row[2]))
#                    print data_dict['max_node_mem']
                 if float(row[3]) > float(data_dict['max_node_load'][1]):
                    data_dict['max_node_load'] = (float(row[0])-float(time_delta),float(row[3]))
#                    print data_dict['max_node_load']
                 if int(row[4]) > int(data_dict['max_cgroup_mem'][1]):
#                    print int(row[4])
                    data_dict['max_cgroup_mem'] = (float(row[0])-float(time_delta),int(row[4]))
#                    print data_dict['max_cgroup_mem']
          except IOError:
             sys.exit('Error: could not open file (%s)'% file)
#          print data_dict
          return data_dict


      def max_mem_load2(self, file):
          data_dict = {}
          try:
             f = open(file,'rb')
             job_reader = csv.reader(f)
             first_row = next(job_reader)
             time_delta = first_row[0]
             data_dict['max_node_mem'] =  (0.0, first_row[1])
             data_dict['max_node_load'] = (0.0, first_row[2])
             for row in job_reader:
                 if int(row[1]) > int(data_dict['max_node_mem'][1]):
                    data_dict['max_node_mem'] = (float(row[0])-float(time_delta),int(row[1]))
#                    print data_dict['max_node_mem']
                 if float(row[2]) > float(data_dict['max_node_load'][1]):
                    data_dict['max_node_load'] = (float(row[0])-float(time_delta),float(row[2]))
#                    print data_dict['max_node_load']
          except IOError:
             sys.exit('Error: could not open file (%s)'% file)
#          print data_dict
          return data_dict



class GenPlotData(object):

      def __init__(self, args):
          self.args = args
          self.rawdata = RawData(self.args)
          self.primary_file = self.rawdata.find_primary_file()
          self.rawdata_dict = self.rawdata.get_max_mem_load_dict()
          self.max_node_files = self.get_max_files()
#          print self.max_node_files
          self.number_compute_cores = getNumberComputeCores(getComputeNodeType(os.path.split(self.max_node_files[0][0])[1][:-4]))
#          print self.number_compute_cores
#          print self.max_node_files
          self.create_node_plot_files()
#          self.create_node_plot_layout_files()
          self.create_node_plot_layout_files2()
#          self.create_total_plot_files(1)
#          self.create_total_plot_files(2)
#          self.create_total_plot_files(3)
          self.create_total_plot_files2()


      def get_max_files(self):
          cnt = 0
          for node in self.rawdata_dict:
              if cnt == 0:
                 max_job_mem = self.rawdata_dict[node]['max_job_mem'][1]
                 min_max_job_mem = self.rawdata_dict[node]['max_job_mem'][1]
                 max_node_mem = self.rawdata_dict[node]['max_node_mem'][1]
                 min_max_node_mem = self.rawdata_dict[node]['max_node_mem'][1]
                 max_node_load = self.rawdata_dict[node]['max_node_load'][1]
                 min_max_node_load = self.rawdata_dict[node]['max_node_load'][1]
                 max_cgroup_mem = self.rawdata_dict[node]['max_cgroup_mem'][1]
                 min_max_cgroup_mem = self.rawdata_dict[node]['max_cgroup_mem'][1]
                 max_job_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 min_max_job_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 max_node_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 min_max_node_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 max_node_load_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 min_max_node_load_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 max_cgroup_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
                 min_max_cgroup_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_job_mem'][1] > max_job_mem:
                 max_job_mem = self.rawdata_dict[node]['max_job_mem'][1]
                 max_job_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_job_mem'][1] < min_max_job_mem:
                 min_max_job_mem = self.rawdata_dict[node]['max_job_mem'][1]
                 min_max_job_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_node_mem'][1] > max_node_mem:
                 max_node_mem = self.rawdata_dict[node]['max_node_mem'][1]
                 max_node_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_node_mem'][1] < min_max_node_mem:
                 min_max_node_mem = self.rawdata_dict[node]['max_node_mem'][1]
                 min_max_node_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_cgroup_mem'][1] > max_cgroup_mem:
                 max_cgroup_mem = self.rawdata_dict[node]['max_cgroup_mem'][1]
                 max_cgroup_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              if self.rawdata_dict[node]['max_cgroup_mem'][1] < min_max_cgroup_mem:
                 min_max_cgroup_mem = self.rawdata_dict[node]['max_cgroup_mem'][1]
                 min_max_cgroup_mem_file = os.path.join(self.rawdata.dir_path,node+'.csv')
              cnt = cnt + 1
          return ((max_job_mem_file,min_max_job_mem_file),(max_node_mem_file,min_max_node_mem_file),(max_node_load_file,min_max_node_load_file),(max_cgroup_mem_file,min_max_cgroup_mem_file)) 


      def create_node_plot_files(self):
#          print self.max_node_files
          cnt = 1
          for file_t in self.max_node_files:
#              print file_t, cnt
              self.create_node_plot_data(file_t, cnt)
              cnt = cnt + 1


      def create_node_plot_layout_files(self):
          self.create_node_plot_layout_data(self.max_node_files[2])


      def create_node_plot_layout_files2(self):
          self.create_node_plot_layout_data2(self.max_node_files[2])


      def create_node_plot_layout_data(self, file_t):
#          print file_t
          writer_l = []
          f_i = open(file_t[0],'rb')
          node_max = os.path.split(file_t[0])[1][:-4]
          reader = csv.reader(f_i)
          for physical_id in range(0, self.number_compute_cores):
              file_ext = "_node_layout_" + str(physical_id) + ".csv"
              outfile = self.args.args.gen_plot_data[0] + '_'+node_max+'_max' + file_ext
              f_o = open(outfile,'wb')
              writer_l.append(csv.writer(f_o))
          first_row = next(reader)
          layout_list = self.get_layout_list(first_row)
#          print layout_list
          cnt = 0
          for physical_id in layout_list:
              writer_l[cnt].writerow([0.0,physical_id])
              cnt = cnt + 1
          time_delta = first_row[0]
          for row in reader:
              layout_list = self.get_layout_list(row)
#              print layout_list
              cnt = 0
              for physical_id in layout_list:
                  writer_l[cnt].writerow([float(row[0])-float(time_delta),physical_id])
                  cnt = cnt + 1


      def create_node_plot_layout_data2(self, file_t):
#          print file_t
          writer_d = {}
          f_i = open(file_t[0],'rb')
          node_max = os.path.split(file_t[0])[1][:-4]
          reader = csv.reader(f_i)
          first_row = next(reader)
          time_delta = first_row[0]
          layout_t_l = self.get_layout_t(first_row)
          for (task_pid,phys_id) in layout_t_l:
              file_ext = "_node_layout_" + str(task_pid) + ".csv"
              outfile = self.args.args.gen_plot_data[0] + '_'+node_max+'_max' + file_ext
              f_o = open(outfile,'wb')
              writer_d[task_pid] = csv.writer(f_o)
              writer_d[task_pid].writerow([0.0, phys_id])
          for row in reader:
              layout_t_l = self.get_layout_t(row)
              for (task_pid,phys_id) in layout_t_l:
                  if task_pid not in writer_d:
                     file_ext = "_node_layout_" + str(task_pid) + ".csv"
                     outfile = self.args.args.gen_plot_data[0] + '_'+node_max+'_max' + file_ext
                     f_o = open(outfile,'wb')
                     writer_d[task_pid] = csv.writer(f_o)
                     writer_d[task_pid].writerow([0.0,phys_id])
                  else:
                     writer_d[task_pid].writerow([float(row[0])-float(time_delta),phys_id])


      def get_layout_list(self, csv_l):
          layout_l = csv_l[4:]
#          print layout_l 
          layout_l2 = []
          physical_id_l = []
          for physical_id in layout_l[1::2]:
#              print physical_id
              physical_id_l.append(int(physical_id))
#          print physical_id_l
          for physical_id_loc in range(0, self.number_compute_cores):
              if physical_id_loc in physical_id_l:
                 layout_l2.append(physical_id_loc)
              else:
                 layout_l2.append(0)
          return layout_l2


      def get_layout_t(self, csv_l):
          layout_l = csv_l[5:]
          layout_t_l = []
          for task_pid in layout_l[0::2]:
              layout_t_l.append((task_pid, layout_l[layout_l.index(task_pid)+1]))

          return layout_t_l    


      def create_node_plot_data(self, file_t, type):
          data = []
          f_i = open(file_t[0],'rb')
          if type == 1:
             file_ext = '_job_mem.csv'
          elif type == 2:
             file_ext = '_node_mem.csv'
          elif type == 3:
             file_ext = '_node_load.csv'
          else:
             file_ext = '_cgroup_mem.csv'
          node_max = os.path.split(file_t[0])[1][:-4]
          outfile = self.args.args.gen_plot_data[0] + '_'+node_max+'_max' + file_ext
          f_o = open(outfile,'wb')
          reader = csv.reader(f_i)
          writer = csv.writer(f_o)
          first_row = next(reader)
          time_delta = first_row[0]
          if type == 1 or type == 2 or type == 4:
             writer.writerow([0.0, to_MB(first_row[type])])
          else:
             writer.writerow([0.0, first_row[type]])
          for row in reader:
              if type == 1 or type == 2 or type == 4:
                 writer.writerow([float(row[0])-float(time_delta),to_MB(row[type])])
              else:
                 writer.writerow([float(row[0])-float(time_delta),row[type]])
          f_i.close()
          f_o.close()
          node_min = os.path.split(file_t[1])[1][:-4]
          outfile2 = self.args.args.gen_plot_data[0] + '_'+node_min+'_min' + file_ext
          f_i2 = open(file_t[1],'rb')
          f_o2 = open(outfile2,'wb')
          reader2 = csv.reader(f_i2)
          writer2 = csv.writer(f_o2)
          first_row2 = next(reader2)
          time_delta2 = first_row2[0]
          if type == 1 or type == 2 or type == 4:
             writer2.writerow([0.0, to_MB(first_row2[type])])
          else:
             writer2.writerow([0.0, first_row2[type]])
          for row2 in reader2:
              if type == 1 or type == 2 or type == 4:
                 writer2.writerow([float(row2[0])-float(time_delta2),to_MB(row2[type])])
              else:
                 writer2.writerow([float(row2[0])-float(time_delta2),row2[type]])
          f_i2.close()
          f_o2.close()


      def create_total_plot_files(self, type):
          x = []
          y = []
          previous_y_dict = {}
          if type == 1:
             file_ext = '_job_mem.csv'
          elif type == 2:
             file_ext = '_node_mem.csv'
          else:
             file_ext = '_node_load.csv'
          if os.path.exists(self.rawdata.dir_path):
             f_p = open(self.primary_file,'rb')
             primary_reader = csv.reader(f_p)
             first_row = next(primary_reader)
             time_delta = first_row[0]
             start_time = float(time_delta)
             x.append(0.0)
             if type == 1 or type == 2:
                y.append(int(first_row[type]))
             else: 
                y.append(float(first_row[type]))
             indx = 0
             for primary_row in primary_reader:
                 for file in glob.glob(self.rawdata.dir_path + '/*.csv'):
                     got_value = False
                     if not file == self.primary_file:
                        f = open(file,'rb')
                        reader = csv.reader(f)
                        for row in reader:
                            if float(row[0]) > start_time and float(row[0]) <= float(primary_row[0]):
                               previous_y_dict[file] = row[type]
                               got_value = True
#                               print file, start_time,row[0],primary_row[0],y[indx],row[type]
                               if type == 1 or type == 2:
                                  y[indx] = y[indx] + int(row[type])
                               else:
                                  y[indx] = y[indx] + float(row[type])
                               break
                        if not got_value and file in previous_y_dict:
                           if type == 1 or type == 2:
                              y[indx] = y[indx] + int(previous_y_dict[file])
                           else:
                              y[indx] = y[indx] + float(previous_y_dict[file])
                 indx = indx + 1
                 if type == 1 or type == 2:
                    y.append(int(primary_row[type]))
                 else:
                    y.append(float(primary_row[type]))
                 x.append(float(primary_row[0])-float(time_delta))
                 start_time = float(primary_row[0])
          else:
             sys.exit('Error: could not find the directory (%s)'% self.dir_path)
          outfile = self.args.args.gen_plot_data[0] + '_total' + file_ext
          f_o = open(outfile,'wb')
          writer = csv.writer(f_o)
#          print len(x),len(y)
          for time in x:
              if type == 1 or type == 2:
                 writer.writerow([time, to_MB(y[x.index(time)])])
              else:
                 writer.writerow([time, y[x.index(time)]])
              

      def create_total_plot_files2(self):
          total_l = []
          zip_lists = []
          primary_node = os.path.split(self.rawdata.primary_file)[1][:-4]
          time_delta = self.rawdata.rawdata_dict[primary_node][0][0]
          for node in self.rawdata.rawdata_dict:
              zip_lists.append(self.rawdata.rawdata_dict[node])
          for zip_t in zip(*zip_lists):
#              print zip_t
              total_job_mem = 0
              total_node_mem = 0
              total_node_load = 0
              for row_l in zip_t:
                  total_job_mem = total_job_mem + int(row_l[1])
                  total_node_mem = total_node_mem + int(row_l[2])
                  total_node_load = total_node_load + float(row_l[3])
              total_l.append((total_job_mem,total_node_mem,total_node_load))
          outfile1 = self.args.args.gen_plot_data[0] + '_total_job_mem.csv'
          outfile2 = self.args.args.gen_plot_data[0] + '_total_node_mem.csv'
          outfile3 = self.args.args.gen_plot_data[0] + '_total_node_load.csv'
          f_o1 = open(outfile1,'wb')
          f_o2 = open(outfile2,'wb')
          f_o3 = open(outfile3,'wb')
          writer1 = csv.writer(f_o1)
          writer2 = csv.writer(f_o2)
          writer3 = csv.writer(f_o3)
          cnt = 0
          for primary_row in self.rawdata.rawdata_dict[primary_node]:
              time = float(primary_row[0]) - float(time_delta)
              if time > 0.0 or cnt == 0:
                 writer1.writerow([time, to_MB(total_l[cnt][0])])
                 writer2.writerow([time, to_MB(total_l[cnt][1])])
                 writer3.writerow([time, total_l[cnt][2]])
              cnt = cnt + 1



class Report(object):

      def __init__(self, args):
          self.args = args
          self.rawdata = RawData(self.args)
#          print self.rawdata.rawdata_dict
          self.report_dict = self.rawdata.get_max_mem_load_dict()
          self.primary_file = self.rawdata.find_primary_file()
#          print "self.primary_file=",self.primary_file
#          self.max_total_job_mem = Report.find_max_total_job_mem(self)
##          self.max_total_job_mem = Report.find_max_total_type(self,1)
          self.max_total_t = Report.find_max_total_type2(self,1)
          self.max_total_job_mem = self.max_total_t[0]
#          print "self.max_total_job_mem=",self.max_total_job_mem
#          self.max_total_node_mem = Report.find_max_total_node_mem(self)
##          self.max_total_node_mem = Report.find_max_total_type(self,2)
          self.max_total_node_mem = self.max_total_t[1]
#          print "self.max_total_node_mem=",self.max_total_node_mem
#          self.max_total_load = Report.find_max_total_load(self)
##          self.max_total_load = Report.find_max_total_type(self,3)
          self.max_total_load = self.max_total_t[2]
          self.max_total_cgroup_mem = self.max_total_t[3]
#          print "self.max_total_load=",self.max_total_load
          Report.print_report(self)


      def find_max_total_type(self,type): 
          if os.path.exists(self.rawdata.dir_path):
             f_p = open(self.primary_file,'rb')
             primary_reader = csv.reader(f_p)
             first_row = next(primary_reader)
             time_delta = first_row[0]
             total_t = (first_row[type],0.0)
             total = 0
             start_time = float(time_delta)
             for primary_row in primary_reader:
                 if type == 1 or type == 2:
                    current_total = int(primary_row[type])
                 else:
                    current_total = float(primary_row[type])
                 for file in glob.glob(self.rawdata.dir_path + '/*.csv'):
                     if not file == self.primary_file:
                        f = open(file,'rb')
                        reader = csv.reader(f)
                        for row in reader:
                            if float(row[0]) > start_time and float(row[0]) <= float(primary_row[0]):
                               if type == 1 or type == 2:
                                  current_total = current_total + int(row[type])
                               else:
                                  current_total = current_total + float(row[type])
                               break
                 if current_total > total:
                    total = current_total
                    if type == 1 or type == 2:
                       total_t = (to_MB(current_total), float(primary_row[0])-float(time_delta))
                    else:
                       total_t = (current_total, float(primary_row[0])-float(time_delta))
                 start_time = float(primary_row[0])
          else:
             sys.exit('Error: could not find the directory (%s)'% self.rawdata.dir_path)
          return total_t


      def find_max_total_type2(self,type):
          total_l = []
          zip_lists = []
          primary_node = os.path.split(self.rawdata.primary_file)[1][:-4]
          time_delta = self.rawdata.rawdata_dict[primary_node][0][0]
          for node in self.rawdata.rawdata_dict:
              zip_lists.append(self.rawdata.rawdata_dict[node])
          for zip_t in zip(*zip_lists):
#              print zip_t
              total_job_mem = 0
              total_node_mem = 0
              total_node_load = 0
              total_cgroup_mem = 0
              for row_l in zip_t:
                  total_job_mem = total_job_mem + int(row_l[1])
                  total_node_mem = total_node_mem + int(row_l[2])
                  total_node_load = total_node_load + float(row_l[3])
                  total_cgroup_mem = total_cgroup_mem + float(row_l[4])
              total_l.append((total_job_mem,total_node_mem,total_node_load,total_cgroup_mem))
          max_total_job_mem = 0
          max_total_node_mem = 0
          max_total_node_load = 0.0
          max_total_cgroup_mem = 0
          max_total_job_mem_time = 0.0
          max_total_node_mem_time = 0.0
          max_total_node_load_time = 0.0
          max_total_cgroup_mem_time = 0.0
          for total_t in total_l:
#              print total_t
              if total_t[0] > max_total_job_mem:
                  max_total_job_mem = total_t[0]
                  if float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0]) > max_total_job_mem_time:
                     max_total_job_mem_time = float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])
                  max_total_job_mem_t = (total_t[0],max_total_job_mem_time-float(time_delta))
              if total_t[1] > max_total_node_mem:
                  max_total_node_mem = total_t[1]
                  if float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0]) > max_total_node_mem_time:
                     max_total_node_mem_time = float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])
                  max_total_node_mem_t = (total_t[1],max_total_node_mem_time-float(time_delta))
              if total_t[2] > max_total_node_load:
                 max_total_node_load = total_t[2]
                 if float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0]) > max_total_node_load_time:
                    max_total_node_load_time = float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])
                 max_total_node_load_t = (total_t[2],max_total_node_load_time-float(time_delta))
              if total_t[3] > max_total_cgroup_mem:
                 max_total_cgroup_mem = total_t[3]
                 if float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0]) > max_total_cgroup_mem_time:
                    max_total_cgroup_mem_time = float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])
                 max_total_cgroup_mem_t = (total_t[3],max_total_cgroup_mem_time-float(time_delta))
#          print (max_total_job_mem_t,max_total_node_mem_t,max_total_node_load_t)
          return (max_total_job_mem_t,max_total_node_mem_t,max_total_node_load_t,max_total_cgroup_mem_t) 


      def print_report(self):
          print self.report_dict
          print ("\n\nMaximum Total aggregate Job Memory is %6.2fMB at %6.2fs\n" % (to_MB(self.max_total_job_mem[0]),self.max_total_job_mem[1]))
          print ("Maximum Total aggregate Node Memory is %6.2fMB at %6.2fs\n" % (to_MB(self.max_total_node_mem[0]),self.max_total_node_mem[1]))
          print ("Maximum Total aggregate job Load is %6.2f at %6.2fs\n" % (self.max_total_load[0],self.max_total_load[1]))
          print ("Maximum Total aggregate job cgroup Memory is %6.2fMB at %6.2fs\n\n" % (to_MB(self.max_total_cgroup_mem[0]),self.max_total_cgroup_mem[1]))
          print ("\n\n{0:^15}{1:^30}{2:^29}{3:^23}{4:^34}").format("Node","Max job memory(MB)(Time(s))","Max Node Memory(MB)(Time(s))","Max Node Load(Time(s))","Max job cgroup memory(MB)(Time(s)")
          print ("{0:^15}{1:^30}{2:^29}{3:^23}{4:^34}").format("="*14,"="*29,"="*28,"="*22,"="*33)
          for key in self.report_dict:
              print ("{0:<15} {1:>14.2f}({2:<.2f}) {3:>18.2f}({4:<.2f}) {5:>18.2f}({6:<.2f}) {7:>18.2f}({8:<.2f})").format(key, 
                                                                                                         to_MB(self.report_dict[key]['max_job_mem'][1]),
                                                                                                         self.report_dict[key]['max_job_mem'][0],
                                                                                                         to_MB(self.report_dict[key]['max_node_mem'][1]),
                                                                                                         self.report_dict[key]['max_node_mem'][0],
                                                                                                         float(self.report_dict[key]['max_node_load'][1]),
                                                                                                         self.report_dict[key]['max_node_load'][0],
                                                                                                         to_MB(self.report_dict[key]['max_cgroup_mem'][1]),
                                                                                                         self.report_dict[key]['max_cgroup_mem'][0])
              


class Report2(object):

      def __init__(self, args):
          self.args = args
          self.rawdata = RawData(self.args)
#          print self.rawdata.rawdata_dict
          self.report_dict = self.rawdata.get_max_mem_load_dict2()
          self.primary_file = self.rawdata.find_primary_file()
#          print "self.primary_file=",self.primary_file
#          self.max_total_job_mem = Report.find_max_total_job_mem(self)
##          self.max_total_job_mem = Report.find_max_total_type(self,1)
          self.max_total_t = Report2.find_max_total_type2(self,1)
#          print "self.max_total_job_mem=",self.max_total_job_mem
#          self.max_total_node_mem = Report.find_max_total_node_mem(self)
##          self.max_total_node_mem = Report.find_max_total_type(self,2)
          self.max_total_node_mem = self.max_total_t[0]
#          print "self.max_total_node_mem=",self.max_total_node_mem
#          self.max_total_load = Report.find_max_total_load(self)
##          self.max_total_load = Report.find_max_total_type(self,3)
          self.max_total_load = self.max_total_t[1]
#          print "self.max_total_load=",self.max_total_load
          Report2.print_report(self)


      def find_max_total_type2(self,type):
          total_l = []
          zip_lists = []
          primary_node = os.path.split(self.rawdata.primary_file)[1][:-4]
          time_delta = self.rawdata.rawdata_dict[primary_node][0][0]
          for node in self.rawdata.rawdata_dict:
              zip_lists.append(self.rawdata.rawdata_dict[node])
          for zip_t in zip(*zip_lists):
#              print zip_t
              total_node_mem = 0
              total_node_load = 0
              for row_l in zip_t:
                  total_node_mem = total_node_mem + int(row_l[1])
                  total_node_load = total_node_load + float(row_l[2])
              total_l.append((total_node_mem,total_node_load))
          max_total_node_mem = 0
          max_total_node_load = 0.0
          for total_t in total_l:
#              print total_t
              if total_t[0] > max_total_node_mem:
                  max_total_node_mem = total_t[0]
                  max_total_node_mem_t = (total_t[0],float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])-float(time_delta))
              if total_t[1] > max_total_node_load:
                 max_total_node_load = total_t[1]
                 max_total_node_load_t = (total_t[1],float(self.rawdata.rawdata_dict[primary_node][total_l.index(total_t)][0])-float(time_delta))
#          print (max_total_job_mem_t,max_total_node_mem_t,max_total_node_load_t)
          return (max_total_node_mem_t,max_total_node_load_t) 


      def print_report(self):
          print ("\n\nMaximum Total aggregate Node Memory is %6.2fMB at %6.2fs\n" % (to_MB(self.max_total_node_mem[0]),self.max_total_node_mem[1]))
          print ("Maximum Total aggregate job Load is %6.2f at %6.2fs\n\n" % (self.max_total_load[0],self.max_total_load[1]))
          print ("\n\n{0:^15}{1:^29}{2:^23}").format("Node","Max Node Memory(MB)(Time(s))","Max Node Load(Time(s))")
          print ("{0:^15}{1:^29}{2:^23}").format("="*14,"="*28,"="*22)
          for key in self.report_dict:
              print ("{0:<15}{1:>14.2f} ({2:5.2f}){3:>15.2f} ({4:5.2f})").format(key, 
                                                                                         to_MB(self.report_dict[key]['max_node_mem'][1]),
                                                                                         self.report_dict[key]['max_node_mem'][0],
                                                                                         float(self.report_dict[key]['max_node_load'][1]),
                                                                                         self.report_dict[key]['max_node_load'][0])


class PlotData(object):
    
      def __init__(self, args):
          self.args = args
          self.plotdata()


      def plotdata(self):
          try:
              import matplotlib.pyplot as plt
          except ImportError:
              sys.exit("Error: importing matplotlib, check if matplotlib is available in this version of python")
          for file in self.args.args.plot_data:  
              x = []
              y = []
              f = open(file,'rb')  
              reader = csv.reader(f)
              for row in reader:
                  x.append(row[0])
                  y.append(row[1])
              plt.plot(x,y)
          if re.search('total_node_mem',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Total Memory Usage (MB)")
          elif re.search('node_mem',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Node Memory Usage (MB)")
          elif re.search('total_job_mem',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Total Job Memory Usage (MB)")
          elif re.search('_mem',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Memory Usage (MB)")
          elif re.search('total_node_load',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Total Load")
          elif re.search('node_layout',self.args.args.plot_data[0]) is not None:
             plt.ylabel("Physical core ID's")
          else:
             plt.ylabel("Node Load")
          plt.xlabel("Real Time (sec)")
          plt.show()
          

def which(program):
    def is_exe(fpath):
#        print fpath, os.path.exists(fpath), os.access(fpath, os.X_OK)
        return os.path.exists(fpath) and os.access(fpath, os.X_OK)
    fpath, fname = os.path.split(program)
    print fpath, fname
    if fpath:
       if is_exe(program):
          return program
    else:
         for path in os.environ["PATH"].split(os.pathsep):
             exe_file = os.path.join(path, program)
#             print exe_file
             if is_exe(exe_file):
                return exe_file
    sys.exit('Error: could not find the binary (%s)'% program)


def to_GB(kb):
    return float(kb)/(1024.0*1024.0)


def to_MB(kb):
    return float(kb)/(1024.0)

def main():

    command_args = CommandArgs()
#    print "(main)command_args.args.exe_args=",command_args.args
#    pbs = Pbs()
#    print pbs.hostlist
#    print pbs.jobid
#    print pbs.jobname
#    print command_args.args
    if command_args.args.report:
       if command_args.args.node_mem_load_only:
          report = Report2(command_args)
       else:
          report = Report(command_args)
    elif command_args.args.gen_plot_data:
       report = GenPlotData(command_args)
    elif command_args.args.plot_data:
       report = PlotData(command_args)
    elif command_args.args.pbsjobid and (command_args.args.exe_pattern or command_args.args.node_mem_load_only):
#       print "Yes execute DataCollector2"
#       if command_args.args.collect:
#          print "collect"
       DataCollector2(command_args)
    else:
       DataCollector(command_args)
   # data_collector.start_executable()
#    collect_agent = CollectAgent(command_args)
#    print collect_agent.data


if __name__ == '__main__':
    main()

