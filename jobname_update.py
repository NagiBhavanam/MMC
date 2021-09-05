from jilprocess import readjobsdata, writejil

def update_jobname(sourcefilename,destinationfilename,logfilename):
    jobsdata = readjobsdata(sourcefilename)
    lstjobnames = [jobdict["insert_job"] for jobdict in jobsdata]
    print(len(lstjobnames))
    loglines = []
    loglines.append("{},{},{},{},{}\n".format("JOBNAME","CHANGED_JOBNAME","insert_job","box_name","condition"))
    for jobname in lstjobnames:
        newjobname = "dmz_{}".format(jobname)
        for jobdict in jobsdata:
            ischanged = False
            jobnamechanged = ""
            boxnamechanged = ""
            conditionchanged = ""
            currentjobname = jobdict["insert_job"]
            if jobname == jobdict["insert_job"]:
                jobnamechanged = "{} : {}".format(jobdict["insert_job"],newjobname)
                jobdict["insert_job"] = newjobname
                ischanged = True
            if "box_name" in jobdict.keys() and jobname == jobdict["box_name"]:
                boxnamechanged = "{} : {}".format(jobdict["box_name"],newjobname)
                jobdict["box_name"] = newjobname
                ischanged = True
            if "condition" in jobdict.keys() and jobname in jobdict["condition"]:
                conditionchanged = "{} : {}".format(jobdict["condition"],newjobname)
                jobdict["condition"] = jobdict["condition"].replace(jobname,newjobname)
                ischanged = True

            if ischanged:
                loglines.append("{},{},{},{},{}\n".format(jobname,currentjobname,jobnamechanged,boxnamechanged,conditionchanged))
        print("{} processed".format(jobname))
    writejil(jobsdata,destinationfilename)

    with open(logfilename,"w") as logfile:
        logfile.writelines(loglines)


if __name__ == '__main__':
    update_jobname("input/ProdDMZ_JIL_08312021.txt","output/ProdDMZ_JIL_08312021_update.txt","logs/ProdDMZ_JIL_08312021_log.csv")
    