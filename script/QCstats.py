#!/usr/bin/env python
# coding=utf-8

__author__ = "Dechun Lin"
__copyright__ = "Copyright 2019, MAGIGENE Research."
__credits__ = ["Dechun Lin"]
__version__ = "0.0.1"
__maintainer__ = "Dechun Lin"
__email__ = "lindechun@magigene.com"

import os,sys
import re
import json
import pandas as pd
from glob import glob
from argparse import ArgumentParser
from pandas.core.frame import DataFrame

parser = ArgumentParser(usage='python3 %(prog)s [options]',description='quality_of_bases')
parser.add_argument('-i', action='store', dest='inputDir', help='Input dir of json')
parser.add_argument('-s', action='store', dest='select',choices=['se','pe'], default='pe', help='se for single-end , pe for paired-end, default is pe')
parser.add_argument('-p', action='store', dest='prefix', default='all', help="Prefix of stats file, default is 'all'")

def readjson(file1):
    global afterfilter,total
    f1 = open ('%s'%file1,'r')
    data = json.load(f1)
    total = float(data['summary']['before_filtering']['total_bases'])      #总bases
    afterfilter = float(data['summary']['after_filtering']['total_bases']) #过滤后剩下的bases
    apfilter = ((total-afterfilter)/total)*100                           #过滤百分比

    q20rawreadrate = float(data['summary']['before_filtering']['q20_rate'])*100     #q20原始bases百分比
    q30rawreadrate = float(data['summary']['before_filtering']['q30_rate'])*100     #q30原始bases百分比
    return (total, afterfilter, apfilter, q20rawreadrate, q30rawreadrate)

    file1.close()

def readpe(file):
    f2 = open ('%s'%file,'r')
    duplicates = pd.read_csv(f2,sep="\t")

    dup1 = duplicates.iat[4,1].split('(')[0]
    dup2 = duplicates.iat[4,3].split('(')[0]
    duptotal = float(dup1)+float(dup2)                      #去重复的bases
    afterdup = afterfilter-duptotal                     #去重复后剩下的bases
    apdup = ((afterfilter-afterdup)/total)*100          #去重复占总bases的百分比
    remain = (afterdup/total)*100                       #最后剩下的占总bases的百分比

    q20clean1 = duplicates.iat[11,2].split('(')[1].replace('%)','')  #取指定单元格，切，替换
    q20clean2 = duplicates.iat[11,4].split('(')[1].replace('%)','')
    q20cleanreadrate = (float(q20clean1)+float(q20clean2))/2            #q20最后的bases占的百分比,平均

    q30clean1 = duplicates.iat[12,2].split('(')[1].replace('%)','')  #取指定单元格，切，替换
    q30clean2 = duplicates.iat[12,4].split('(')[1].replace('%)','')
    q30cleanreadrate = (float(q30clean1)+float(q30clean2))/2            #q30最后的bases占的百分比,平均
    return(duptotal,apdup,afterdup,remain,q20cleanreadrate,q30cleanreadrate)
    f2.close()

def readse(file):

    f3 = open ('%s'%file,'r')
    duplicates = pd.read_csv(f3,sep="\t")
    duptotal = float(duplicates.iat[4,1].split('(')[0])
    afterdup = afterfilter-duptotal
    apdup = ((afterfilter-afterdup)/total)*100
    remain = (afterdup/total)*100
    q20cleanreadrate = float(duplicates.iat[11,2].split('(')[1].replace('%)',''))
    q30cleanreadrate = float(duplicates.iat[12,2].split('(')[1].replace('%)',''))
    return(duptotal,apdup,afterdup,remain,q20cleanreadrate,q30cleanreadrate)
    f3.close()

def main():
    para = parser.parse_args()
    inputfile = para.inputDir
    select = para.select
    prefix = para.prefix
    namelist = []

    if not inputfile:
        sys.exit("please provide inFile inputDir, see -h/--help\n")

    a=[];b=[];c=[];d=[];e=[];f=[];g=[];h=[];i=[];j=[];k=[]

    for z in glob(inputfile+'/*'):
        name = os.path.basename('%s'%z)
        file1 = z+'/'+name+'.json'
        if os.path.exists(file1):
            namelist.append(name)
        else:
            continue

        file = z+'/Basic_Statistics_of_Sequencing_Quality.txt'

        data1 = readjson(file1)
        if select == 'pe':
            data2 = readpe(file)
        elif select == 'se':
            data2 = readse(file)
        else:
            print("please entry se or pe")

        a.append('%.0f'%data1[0])
        b.append('%.0f'%(data1[0]-data1[1]))
        c.append('%.2f'%data1[2])
        d.append('%.0f'%data2[0])
        e.append('%.2f'%data2[1])
        f.append('%.0f'%data2[2])
        g.append('%.2f'%data2[3])
        h.append('%.2f'%data1[3])
        i.append('%.2f'%data2[4])
        j.append('%.2f'%data1[4])
        k.append('%.2f'%data2[5])

    items = [('Raw Bases',a),
           ('Filter Bases',b),
           ('Filter(%)',c),
           ('Dup Bases',d),
           ('Dup(%)',e),
           ('Clean Bases',f),
           ('Effective(%)',g),
           ('Raw Q20(%)',h),
           ('Clean Q20(%)',i),
           ('Raw Q30(%)',j),
           ('Clean Q30(%)',k)]
    l = dict(items)

    df = DataFrame(l,columns=['Raw Bases','Filter Bases','Filter(%)',
                       'Dup Bases','Dup(%)',
                       'Clean Bases','Effective(%)',
                       'Raw Q20(%)','Clean Q20(%)',
                       'Raw Q30(%)','Clean Q30(%)'],index=namelist)
    
    if not df.shape[0]:
        sys.exit(-1)

    df.index.name = 'Sample'
    df.to_csv(prefix+".QCstats_raw.xls",sep='\t') 
    new_df = df.loc[:,['Raw Bases','Filter Bases','Filter(%)','Dup Bases','Dup(%)','Clean Bases','Clean Q20(%)','Effective(%)']]
    new_df.to_csv(prefix+".QCstats.xls",sep='\t')

if __name__ == "__main__":
    main()
