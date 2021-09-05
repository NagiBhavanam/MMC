from jilprocess import readjobsdata,writejil,find_between_v1
import pandas as pd

def preparewavesjobsjil(sourcefilename,wavejobsourcefilename,destfolder,wave):
    jobsdata = readjobsdata(sourcefilename)
    lstwavejobnames = readwavejobnames(wavejobsourcefilename,wave)
    lstwavejobnames = [jobname.strip() for jobname in lstwavejobnames]

    # get wave jobs from main jobslist
    lstwavejobs = [job for job in jobsdata if job["insert_job"].strip() in lstwavejobnames]

    lstuniquewavejobs = [job["insert_job"].strip() for job in jobsdata if job["insert_job"].strip() in lstwavejobnames]

    print("From helper : {}, From main jobslist : {},UniqueJobs : {}".format(len(lstwavejobnames),len(lstwavejobs),len(list(set(lstuniquewavejobs)))))
    
    #find duplicate jobs list
    seen = set()
    lstduplicatejobs = []
    for jobname in lstuniquewavejobs:
        if jobname in seen:
            lstduplicatejobs.append(jobname)
        else:
            seen.add(jobname)
    print(lstduplicatejobs)
    
    writejil(lstwavejobs,"{}/{}.jil".format(destfolder,wave))

    #getdependencyreport("processedfiles/waves/{}.jil".format(wave),wave)

def readwavejobnames(sourcefilename,wave):
    #wavejobsdata = pd.read_csv(sourcefilename,skiprows = 1)
    wavejobsdata = pd.read_csv(sourcefilename)
    waveone_df = wavejobsdata[wavejobsdata['Wave'] == wave]
    lstwaveonejobsnames = waveone_df["Job_name"].tolist()
    
    #print(len(lstwaveonejobsnames),type(lstwaveonejobsnames))

    return lstwaveonejobsnames

def getdependencyreport(sourcefilename,reportsfolder,wave):
    jobsdata = readjobsdata(sourcefilename)
    lstwavejobnames = []
    dictconditions = {}
    for job in jobsdata:
        jobname = job["insert_job"]
        lstwavejobnames.append(jobname)
        if "condition" in job.keys():
            dictconditions[jobname] = job["condition"]

    print(len(lstwavejobnames))

    condLines = []
    #condLines.append("{},{},{}\n".format("JOB NAME","CONDITION","CONDITION JOBS"))
    condLines.append("JOB NAME,CONDITION,JOBS NOT IN SAME WAVE\n")
    loglines = []
    loglines.append("JOBNAME\n")
    lstdependjobs = []
    for key,val in dictconditions.items():
        jobsincond = find_between_v1(val,'(',')',loglines)
        lstjobs_notin_wave = []
        for job in jobsincond:
            if job not in lstwavejobnames:
                lstjobs_notin_wave.append(job)

        condLines.append("{},{},{}\n".format(key,val.replace(",",":"),lstjobs_notin_wave))

    #print(loglines)

    with open("{}/{}_dependence_report.csv".format(reportsfolder,wave),'w') as condFile:
        condFile.writelines(condLines)

if __name__ == "__main__":
    preparewavesjobsjil("input/MMC_ALLJOBS.txt","helperfiles/MMC_jobs_appOwners.csv","wavejobs","Wave1")
    #getdependencyreport("wavejobs/Wave1.jil","reports","Wave3")