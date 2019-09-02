#!/usr/bin/env python
# coding=utf-8

__author__ = "Dechun Lin"
__copyright__ = "Copyright 2019, MAGIGENE Research."
__credits__ = ["Dechun Lin"]
__version__ = "0.0.1"
__maintainer__ = "Dechun Lin"
__email__ = "lindechun@magigene.com"

import os,sys
from argparse import ArgumentParser
from subprocess import Popen, PIPE

def parseCommand():
    parser = ArgumentParser(usage='python3 %(prog)s [options]',description='Quality control for raw fastq data')
    parser.add_argument('-i', action='store', dest='inFile', help="a list of PE or SE, include 2/3 col, Sample\tread1\t[read2]")
    parser.add_argument('-o', action='store', dest='Opath', default='./result', help="output path, default is './result'")
    parser.add_argument('-s', action='store', dest='seqtype', choices=['se','pe'], default='pe', help='input sequence type, default is pe')
    parser.add_argument('--filterPara', action='store', dest='filterPara', default='-n 2 -q 15 -c -x -5 -3', help='fastp\' parameter of filter read, default is \'-n 2 -q 15 -c -x -5 -3\'')
    parser.add_argument('-a', action='store', dest='adapter', default=os.path.dirname(os.path.realpath(__file__))+'/../lib/adapter.fa', help='a fasta file to trim both read1 and read2, default is {0}/../lib/adapter.fa'.format(os.path.dirname(os.path.realpath(__file__))))
    parser.add_argument('-t', action='store', dest='thread', type=str, default='6', help='thread number, default is 6')
    parser.add_argument('--mRun', action='store_true', dest='mRun', default=False, help='Use --mRun if you want to run multi-task according to each sample')
    parser.add_argument('--runNumber', action='store', dest='runNumber', type=str, default='4', help='set the maximum number of jobs to throw out, default is 4. only in combination with --mRun')
    parser.add_argument('-p', action='store', dest='prefix', default='all', help="Prefix of stats file, default is 'all'")

    return(parser.parse_args())

def QC(hd1,hd2,Opath,thread,Bin,SampleName,filterPara,adapter,path1,path2='no'):
    hd1.write('## '+SampleName+"\n")
    hd1.write('mkdir '+Opath+'/'+SampleName+"\n")

    newOpath=Opath+'/'+SampleName+'/'+SampleName

    if path2 != 'no':
        fp_command='{0}/../src/fastp -i {1} -I {2} --detect_adapter_for_pe --adapter_fasta {3} -o {4} -O {5} -j {6} -h {7} -R {8} -w {9} {10}'.format(Bin,path1,path2,adapter,newOpath+'_1.clean_fastp.fq.gz',newOpath+'_2.clean_fastp.fq.gz',newOpath+'.json',newOpath+'.html',SampleName,thread,filterPara)
        SOAPnuke_command=Bin+'/../src/SOAPnuke filter -1 {0} -2 {1} -d -C {2} -D {3} -o {4} -Q 2'.format(newOpath+'_1.clean_fastp.fq.gz',newOpath+'_2.clean_fastp.fq.gz',newOpath+'_1.fastq.gz',newOpath+'_2.fastq.gz',Opath+'/'+SampleName)
        clean_fq_list=SampleName+'\t'+newOpath+'_1.fastq.gz\t'+newOpath+'_2.fastq.gz'

    else:
        fp_command='{0}/../src/fastp -i {1} -o {2} -j {3} -h {4} -R {5} -w {6} {7}'.format(Bin,path1,newOpath+'_1.clean_fastp.fq.gz',newOpath+'.json',newOpath+'.html',SampleName,thread,filterPara)
        SOAPnuke_command=Bin+'/../src/SOAPnuke filter -1 {0} -d -C {1} -o {2} -Q 2'.format(newOpath+'_1.clean_fastp.fq.gz',newOpath+'_1.fastq.gz',Opath+'/'+SampleName)

        clean_fq_list=SampleName+'\t'+newOpath+'_1.fastq.gz'

    hd1.write(fp_command+'\n')
    hd1.write(SOAPnuke_command+'\n')
    hd2.write(clean_fq_list+'\n')

    hd1.write('rm '+newOpath+'_*.clean_fastp.fq.gz\n')

def main():
    para=parseCommand()
    inFile=para.inFile
    Opath=para.Opath
    seqtype=para.seqtype
    thread=para.thread
    filterPara=para.filterPara
    adapter=para.adapter
    mRun=para.mRun
    runNumber=para.runNumber
    prefix=para.prefix

    dirpath=os.path.dirname(os.path.realpath(__file__))
    Opath=os.path.dirname(os.path.realpath(Opath))+'/'+os.path.basename(Opath)

    if not inFile:
        sys.exit('Please provide inFile input, see -h/--help')

    if os.path.exists(Opath):
        os.system('rm -rf '+Opath)
    os.system('mkdir '+Opath)

    filter_hd=open(Opath+'/s01.filter.sh','w')
    stat_hd=open(Opath+'/s02.stat.sh','w')
    CleanFq_hd=open(Opath+'/'+prefix+'.clean_fq.list','w')

    if seqtype == 'pe':
        for i in open(inFile,'r'):
            sample,path1,path2=i.strip().split('\t')
            QC(filter_hd,CleanFq_hd,Opath,thread,dirpath,sample,filterPara,adapter,path1,path2)
    elif seqtype == 'se':
        for i in open(inFile,'r'):
            sample,path1=i.strip().split('\t')
            QC(filter_hd,CleanFq_hd,Opath,thread,dirpath,sample,filterPara,adapter,path1)

    filter_hd.close()
    CleanFq_hd.close()
    
    ## stat
    stat_hd.write('python '+dirpath+'/../script/QCstats.py -i '+Opath+' -p '+Opath+'/'+prefix+ ' -s '+seqtype+'\n')
    stat_hd.close()
    
    ## run script
    if mRun:
        batchNumber = '5' ##一个样本对应的命令行数
        command1 = Popen('python '+dirpath+'/../script/mRun.py -i '+Opath+'/s01.filter.sh -n '+batchNumber+' -t '+runNumber+' && echo "this-work-is-complete" > '+Opath+'/s01.filter.sign',shell=True,stderr=PIPE)
    else:
        command1 = Popen('sh '+Opath+'/s01.filter.sh > '+Opath+'/s01.filter.log 2>&1 && echo "this-work-is-complete" > '+Opath+'/s01.filter.sign',shell=True,stderr=PIPE)

    command1.wait()
    run_status1 = 0 if command1.returncode == 0 else -1

    command2 = Popen('sh '+Opath+'/s02.stat.sh && echo "this-work-is-complete" > '+Opath+'/s02.stat.sign',shell=True,stderr=PIPE)
    command2.wait()
    run_status2 = 0 if command2.returncode == 0 else -1

    if run_status1 == -1 or run_status2 == -1:
        sys.exit(-1)
    else:
        sys.exit(0)

if __name__=="__main__":
    main()
