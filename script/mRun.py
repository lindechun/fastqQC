#!/usr/bin/env python
# coding=utf-8
__author__ = "Dechun Lin"
__copyright__ = "Copyright 2019, MAGIGENE Research."
__credits__ = ["Dechun Lin"]
__version__ = "0.1.0"
__maintainer__ = "Dechun Lin"
__email__ = "lindechun@magigene.com"

"""
子任务形成一系列子脚本work_[1-9].sh放在/your/path/script.sh_psub/下
"""

import sys,os
from argparse import ArgumentParser
from multiprocessing import Pool
from subprocess import Popen,STDOUT,PIPE

def parseCommand():
    parser = ArgumentParser(description='A script for run batch task by multi Processor')
    parser.add_argument('-i',action='store',dest='scriptFile',help='the script you want to run on compute server')
    parser.add_argument('-n',action='store',dest='number',default=1,type=int,help='set number of lines to form a job, default: 1')
    parser.add_argument('-t',action='store',dest='Processor',default=4,type=int,help='set the maximum number of jobs to throw out, default: 4')
    return parser.parse_args()

def runSingleTask(Task):
    """run单个任务"""
    p = Popen(['sh '+Task+' 1>'+Task+'.o 2>'+Task+'.e && echo "this-work-is-complete" >> '+Task+'.finished'],shell=True,stderr=PIPE,close_fds=True)
    return(p.wait())

class MultiProcessor():
    def __init__(self,filepath,number,nthread):
        self.filepath = filepath
        self.number = number
        self.nthread = nthread
        self.task_list = []
        self.res_list = []

    def CreateSubtask(self):
        """ 生成子任务队列"""
        Binpath = os.path.dirname(os.path.realpath(self.filepath))
        psub = Binpath+'/'+os.path.basename(self.filepath)+"_psub"

        if os.path.exists(psub):
            os.system('rm -rf '+psub)
        os.system('mkdir '+psub)

        ## 生成子任务*_psub/work_*.sh ##
        command_num = 1
        task_num = 1
        for i in open(self.filepath,'r'):
            write_sh = open(psub+"/work_{0}.sh".format(str(task_num)),'a')
            if self.number == 1:
                write_sh.write(i)
                self.task_list.append(psub+"/work_{0}.sh".format(str(task_num)))
                task_num += 1
                write_sh.close()
            else:
                write_sh.write(i)
                if command_num % self.number == 0:
                    self.task_list.append(psub+"/work_{0}.sh".format(str(task_num)))
                    task_num += 1
                    write_sh.close()
                command_num += 1
        ## 生成子任务*_psub/work_*.sh ##

    def run(self):
        """按队列方式跑任务"""
        runQueue = Pool(processes = self.nthread)

        for i in self.task_list:
                res = runQueue.apply_async(runSingleTask,(i,))
                self.res_list.append((i,res))

        runQueue.close()
        runQueue.join()
    
    def WriteState(self):
        """生成日志"""
        log_path = os.path.realpath(self.filepath)
        error_task = 0
        for (task_id,res) in self.res_list:
            task_id_short = '/'.join((task_id.split('/')[-2:]))
            if res.get():
                print(task_id_short+" hasn't been finished!",file=open(log_path+".error",'a'))
                error_task += 1
            else:
                print(task_id_short+" is finished!",file=open(log_path+".finished",'a'))

        if error_task:
            print("There are some job can't run finish, check the shell and qsub again",file=open(log_path+".log",'w'))
            return(-1)
        else:
            print('All jobs are finished correctly',file=open(log_path+".log",'w'))
            return(0)

if __name__ == '__main__':
    para = parseCommand()
    filepath = para.scriptFile
    number = para.number
    thread = para.Processor

    if not filepath:
        sys.exit("Please input -h/--help to get help for mRun.py")

    ## run
    Process = MultiProcessor(filepath,number,thread)
    Process.CreateSubtask()
    Process.run()
    sys.exit(Process.WriteState())
