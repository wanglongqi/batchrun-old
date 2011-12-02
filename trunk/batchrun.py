# -*- coding:utf-8 -*-
'''
Created on 2011-12-1

@author: WLQ
'''
import sys
import optparse

def dofile(x):
    import os
    out=os.popen(x)
    return out.read()

parser = optparse.OptionParser(version="%prog 0.01alpha")

parser.add_option('-n', '--ncpu',
        dest='ncpu',
        help='ncpu, use how many cpus for current job.',
        type='int'
        )
parser.add_option("-v",
        action="count", 
        help='verbosity, dupulicate v to get more details.',
        dest="verbosity"
        )
options, others = parser.parse_args()

if others==[]:    
    others=[None]    
import pp
import time
ppservers=()
if options.ncpu !=None:
    job_server = pp.Server(options.ncpu, ppservers=ppservers)
else:
    job_server = pp.Server(ppservers=ppservers)

time.clock()
for file in others:
    if file==None:
        fid=sys.stdin
    else:
        fid=open(file)
    jobs=[]
    jcount=0
    for line in fid:
        if options.verbosity>1:
            jcount+=1
            print 'Jobs',jcount,':',line,
        jobs.append(job_server.submit(dofile, (line, )))
        
    for i in jobs:
        if options.verbosity>1 and jcount>0:
            print '\nDo you want to execute the jobs?(y/N)'
            if raw_input()=='N':
                break
            jcount=0
        print'Job',i.tid,'output:\n',i()
        if options.verbosity>0:
            print 'Elapsed time: ',time.clock()      
