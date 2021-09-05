from collections import OrderedDict
try:
   import queue
except ImportError:
   import Queue as queue
   
def find_between_v1(source,left,right,loglines):
    leftque = queue.LifoQueue()
    dictindexs = OrderedDict()
    index = 0
    for item in source:
        if item == left:
            leftque.put(index)
            #print("{} : {}\n".format(leftque.queue,dictindexs))
        elif item == right:
            dictindexs[leftque.get()] = index
        index += 1
    #print(dictindexs)
    lstCondJobs = []
    for left,right in dictindexs.items():
        jobincond = source[left + 1:right]
        if "," in jobincond:
            loglines.append("{}\n".format(jobincond))
            jobincond = jobincond.split(",")[0]
        if "s(" in jobincond or "f(" in jobincond or "e(" in jobincond:
            pass
        else:
            lstCondJobs.append(jobincond)
    
    return list(set(lstCondJobs))

def readjobsdata(filename):
    with open(filename,'r') as data:
        jobslist = []
        for line in data:
            line = line.strip()
            dataarray = line.split(':')
            if 'insert_job' in dataarray:
                jobdict = OrderedDict()
                jobslist.append(jobdict)
                jobnamearray = dataarray[1].split(' ')
                jobname = jobnamearray[1]
                jobtype = dataarray[2]
                jobdict['insert_job'] = jobname.strip()
                jobdict['job_type'] = jobtype.strip()
                #print(jobname + '  ' + jobtype)
                #print('------------------------------')
            elif len(dataarray) > 1:
                property = dataarray[0].strip()
                if len(dataarray) > 2:
                    value = ':'.join(dataarray[1:])
                    jobdict[property] = value.strip()
                    #print(property + ":" + value)
                else:
                    value = dataarray[1]
                    jobdict[property] = value.strip()  
    return jobslist
    
def writejil(jobsdata,desfilename):
    #jobsdata = readjobsdata(sourcefilename)
    jobdataLines = []
    for job in jobsdata:
        jobtype = job["job_type"].strip()
        for key in job.keys():
            if key == "insert_job":
                jobdataLines.append("\n/* ----------------- {} ----------------- */ \n\n".format(job["insert_job"]))
                if jobtype == "BOX":
                    jobdataLines.append("insert_job: {}  job_type: {} \n".format(job["insert_job"],job["job_type"].strip()))
                else:
                    if "box_name" in job.keys():
                        jobdataLines.append(" insert_job: {}  job_type: {} \n".format(job["insert_job"],job["job_type"].strip()))
                    else:
                        jobdataLines.append("insert_job: {}  job_type: {} \n".format(job["insert_job"],job["job_type"].strip()))
            elif key != 'job_type':
                if jobtype == "BOX":
                    jobdataLines.append("{}: {}\n".format(key,str(job[key]).strip()))
                else:
                    if "box_name" in job.keys():
                        jobdataLines.append(" {}: {}\n".format(key,str(job[key]).strip()))
                    else:
                        jobdataLines.append("{}: {}\n".format(key,str(job[key]).strip()))

    #print(desfilename,len(jobsdata))
    with open(desfilename,'w') as finalFile:
        finalFile.writelines(jobdataLines)

if __name__ == '__main__':
    writejil("","")