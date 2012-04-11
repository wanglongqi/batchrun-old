# -*- coding:utf-8 -*-
'''
Created on 2011-4-11

@author: WLQ
'''
import sys
import optparse

def dofile(x):
    import os
    out=os.popen(x)
    return out.read()

parser = optparse.OptionParser(version="%prog 0.1")

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
import subprocess
if options.ncpu !=None:
    import multiprocessing
    options.ncpu=multiprocessing.cpu_count()

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
        jobs.append(line)

if options.verbosity>1 and jcount>0:
    count=0
    for job in jobs:
        print 'Job ',count,' :',job
        count+=1
    print '\nDo you want to execute the jobs?(y/N)'
    if raw_input()=='N':
        sys.exit()

jcount=0
rjob=[]
rinfo=[]
while True:
    try:
        # Add jobs
        while len(rjob)<options.ncpu:
            rjob.append(subprocess.Popen(jobs[jcount]),shell=True)
            rinfo.append((jobs[count],time.clock(),rjob[-1].pid,jcount))
            if options.verbosity>2:
                print 'Executing job',jcount,', Pid is',rjob[-1].pid
            if options.verbosity>3:
                print ':: New Job info :: \n Command: %s \t %Start time: %g\n Job pid: %d\tJob count: %d'%(rinfo[-1][0],rinfo[-1][1],rinfo[-1][2],rinfo[-1][3])
            jcount+=1
            if jcount>len(jobs):
                break
        
        # Remove finished jobs
        for i in range(len(rjob)):
            p=rjob[i].poll()
            if p!=None:
                if p!=0:
                    print rinfo[i][1],'exit abnormal. Exit code',p
                if options.verbosity>2:
                    print 'Job %d Finished!'%rinfo[i][3]
                if options.verbosity>3:
                    print ':: Finished Job info :: \n Command: %s \t %Total time: %g\n Job pid: %d\tJob count: %d'%(rinfo[-1][0],time.clock()-rinfo[-1][1],rinfo[-1][2],rinfo[-1][3])
                rjob.pop(i)
                rinfo.pop(i)
                
        #Do something else
        
        #Sleeping
        time.sleep(0.1)
                
    except KeyboardInterrupt:
        break
        print 'Note: User Abort Determined!'
        print jcount,'jobs has been submitted. Rest jobs will be abandoned.'
        print 'Submited job will not be affected. Kill the process if you will.'
        pass
