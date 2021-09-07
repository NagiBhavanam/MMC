from jilprocess import readjobsdata
from collections import OrderedDict
from substringprocess import find_between_v1

def getmachinejobsreport(sourcedatapath,reportpath):
    
    lstjobsdata = readjobsdata(sourcedatapath)

    reportlines = []
    machinejobsdict = OrderedDict()
    for jobdict in lstjobsdata:
        lstmachinejobs = []
        if "machine" in jobdict.keys():
            machinename = jobdict["machine"]
            if machinename in machinejobsdict.keys():
                machinejobsdict[machinename].append(jobdict)
            else:
                lstmachinejobs.append(jobdict)
                machinejobsdict[machinename] = lstmachinejobs
    onejobmachinelines = []
    twojobmachinelines = []
    onejobmachinelines.append("{},{},{},{},{}\n".format("JOBNAME","DESCRIPTION","MACHINE NAME","COMMAND","JOB TYPE"))
    for machine,lstjobs in machinejobsdict.items():
        #print("{} : {}".format(machine,lstjobs))
        if len(lstjobs) == 1:
            jobdict = lstjobs[0]
            onejobmachinelines.append("{},{},{},{},{}\n".format(jobdict["insert_job"],jobdict["description"].replace(","," "),jobdict["machine"],jobdict["command"].replace(","," "),jobdict["job_type"]))
        if len(lstjobs) == 2:
            for jobdict in lstjobs:
                twojobmachinelines.append("{},{},{},{},{}\n".format(jobdict["insert_job"],jobdict["description"].replace(","," "),jobdict["machine"],jobdict["command"].replace(","," "),jobdict["job_type"]))
    
    reportlines.extend(onejobmachinelines)
    reportlines.extend(twojobmachinelines)
    with open(reportpath,'w') as reportFile:
        reportFile.writelines(reportlines)

def jobs_inbox_report(sourcefileone,sourcefiletwo,reportfilename):
    fileonejobsdata = readjobsdata(sourcefileone)
    filetwojobsdata = readjobsdata(sourcefiletwo)

    lstfileoneboxes = []
    for jobdict in fileonejobsdata:
        if jobdict["job_type"] == "BOX":
            lstfileoneboxes.append(jobdict["insert_job"])
        if "box_name" in jobdict.keys():
            lstfileoneboxes.append(jobdict["box_name"])
    lstfiletwoboxes = []
    for jobdict in filetwojobsdata:
        if jobdict["job_type"] == "BOX":
            lstfiletwoboxes.append(jobdict["insert_job"])
        if "box_name" in jobdict.keys():
            lstfiletwoboxes.append(jobdict["box_name"])

    loglines = []
    lstfileoneuniqueboxes = list(set(lstfileoneboxes))
    lstfiletwouniqueboxes = list(set(lstfiletwoboxes))
    lstdifference = list(set(lstfiletwouniqueboxes) - set(lstfileoneuniqueboxes))
    print(len(lstfileoneuniqueboxes),len(lstfiletwouniqueboxes))

    loglines.append("{},{},{},{}\n".format("BOXNAME","OLDCOUNT","NEWCOUNT","DIFFERENCE"))
    for boxname in lstfileoneuniqueboxes:
        lstcurrentboxfileonejobs = []
        lstcurrentboxfiletwojobs = []
        for jobdict in fileonejobsdata:
           if "box_name" in jobdict.keys():
               if jobdict["box_name"] == boxname:
                   lstcurrentboxfileonejobs.append(jobdict["insert_job"])
        for jobdict in filetwojobsdata:
               if "box_name" in jobdict.keys():
                if jobdict["box_name"] == boxname:
                    lstcurrentboxfiletwojobs.append(jobdict["insert_job"])

        loglines.append("{},{},{},{}\n".format(boxname,len(lstcurrentboxfileonejobs),len(lstcurrentboxfiletwojobs),len(lstcurrentboxfiletwojobs)-len(lstcurrentboxfileonejobs)))
        
    loglines.append("{},{},{}\n".format("EXCESSBOXES","",""))
    for boxname in lstdifference:
        lstcurrentboxfiletwojobs = []
        for jobdict in filetwojobsdata:
            if "box_name" in jobdict.keys():
                if jobdict["box_name"] == boxname:
                    lstcurrentboxfiletwojobs.append(jobdict["insert_job"])
        loglines.append("{},{},{},{}\n".format(boxname,0,len(lstcurrentboxfiletwojobs),len(lstcurrentboxfiletwojobs)))

        #print("{} : {} : {}".format(boxname,len(lstcurrentboxfileonejobs),len(lstcurrentboxfiletwojobs)))

    with open(reportfilename,"w") as reportfile:
        reportfile.writelines(loglines)

def getjobsnotinwave(sourcefilename,reportfilename):
    jobsdata = readjobsdata(sourcefilename)
    lstboxes = []
    loglines = []
    for jobdict in jobsdata:
        lstjobsincondition = []
        if jobdict["job_type"] == "BOX":
            lstboxes.append(jobdict["insert_job"])
        if "box_name" in jobdict.keys():
            lstboxes.append(jobdict["box_name"])
        if "condition" in jobdict.keys():
            condition = jobdict["condition"]
            lstjobsincondition.extend(find_between_v1(condition,"(",")",loglines))

    lstjobstocheck = []
    #lstjobstocheck.extend(lstboxes)
    lstjobstocheck.extend(lstjobsincondition)
    lstjobstocheck = list(set(lstjobstocheck))

    lstalljobnames = [jobdict["insert_job"] for jobdict in jobsdata]
    lstjobsnotincurrentwave = [jobname for jobname in lstjobstocheck if jobname not in lstalljobnames]

    print(lstjobsnotincurrentwave)
    reportlines = "\n".join(lstjobsnotincurrentwave)
    with open(reportfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

def getboxjobsalllevels(sourcefilename,mainsourcefilename,reportfilename):
    jobsdata = readjobsdata(sourcefilename)
    mainsourcejobsdata = readjobsdata(mainsourcefilename)
    lstboxes = []
    for jobdict in jobsdata:
        if jobdict["job_type"] == "BOX":
            lstboxes.append(jobdict["insert_job"])
        if "box_name" in jobdict.keys():
            lstboxes.append(jobdict["box_name"])

    #pull boxjobs all levels
    reportlines = []
    lstuniqueboxes = list(set(lstboxes))
    for leveloneboxname in lstuniqueboxes:
        reportlines.append("{}\n".format(leveloneboxname))
        lstalljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == leveloneboxname]
        lstleveltwoboxjobs = [jobdict for jobdict in lstalljobsinbox if jobdict["job_type"] == "BOX"]
        lstnonboxjobs = [jobdict for jobdict in lstalljobsinbox if jobdict["job_type"] != "BOX"]
        getreportlines(reportlines,lstnonboxjobs,"\t{}\n")
        for leveltwoboxdict in lstleveltwoboxjobs:
            reportlines.append("\t{}\n".format(leveltwoboxdict["insert_job"]))
            lstleveltwoalljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == leveltwoboxdict["insert_job"]]
            lstlevelthreeboxjobs = [jobdict for jobdict in lstleveltwoalljobsinbox if jobdict["job_type"] == "BOX"]
            lstnonboxjobs = [jobdict for jobdict in lstleveltwoalljobsinbox if jobdict["job_type"] != "BOX"]
            getreportlines(reportlines,lstnonboxjobs,"\t\t{}\n")
            for levelthreeboxdict in lstlevelthreeboxjobs:
                 reportlines.append("\t\t{}\n".format(levelthreeboxdict["insert_job"]))
                 lstlevelthreealljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == levelthreeboxdict["insert_job"]]
                 lstlevelfoureboxjobs = [jobdict for jobdict in lstlevelthreealljobsinbox if jobdict["job_type"] == "BOX"]
                 lstnonboxjobs = [jobdict for jobdict in lstlevelthreealljobsinbox if jobdict["job_type"] != "BOX"]
                 getreportlines(reportlines,lstnonboxjobs,"\t\t\t{}\n")
                 for levelfoureboxdict in lstlevelthreeboxjobs:
                     reportlines.append("\t\t\t{}\n".format(levelfoureboxdict["insert_job"]))
                     lstlevelfourealljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == levelfoureboxdict["insert_job"]]
                     lstlevelfiveboxjobs = [jobdict for jobdict in lstlevelfourealljobsinbox if jobdict["job_type"] == "BOX"]
                     lstnonboxjobs = [jobdict for jobdict in lstlevelfourealljobsinbox if jobdict["job_type"] != "BOX"]
                     getreportlines(reportlines,lstnonboxjobs,"\t\t\t\t{}\n")

    with open(reportfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

def getboxjobs_hirarchy(jilfilename,reportfilename):
    jobsdata = readjobsdata(jilfilename)
    #fetch topboxes
    lstleveloneboxes = []
    for jobdict in jobsdata:
        if "BOX" == jobdict["job_type"] and "box_name" not in jobdict.keys():
            lstleveloneboxes.append(jobdict)
    reportlines = []
    for leveloneboxdict in lstleveloneboxes:
        print("{}".format(leveloneboxdict["insert_job"]))
        reportlines.append("{}\n".format(leveloneboxdict["insert_job"]))
        lstleveltwoboxes = []
        getboxes(jobsdata,lstleveltwoboxes,leveloneboxdict)
        for leveltwoboxdict in lstleveltwoboxes:
            print("\t{}".format(leveltwoboxdict["insert_job"]))
            reportlines.append("\t{}\n".format(leveltwoboxdict["insert_job"]))
            lstlevelthreeboxes = []
            getboxes(jobsdata,lstlevelthreeboxes,leveltwoboxdict)
            for levelthreeboxdict in lstlevelthreeboxes:
                print("\t\t{}".format(levelthreeboxdict["insert_job"]))
                reportlines.append("\t\t{}\n".format(levelthreeboxdict["insert_job"]))
                lstlevelfourboxes = []
                getboxes(jobsdata,lstlevelfourboxes,levelthreeboxdict)
                for levelfourboxdict in lstlevelfourboxes:
                    print("\t\t\t{}".format(levelfourboxdict["insert_job"]))
                    reportlines.append("\t\t\t{}\n".format(levelfourboxdict["insert_job"]))
                    lstlevelfiveboxes = []
                    getboxes(jobsdata,lstlevelfiveboxes,levelfourboxdict)

    with open(reportfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

def getboxes(jobsdata,lstlevelboxes,boxdict):
    for jobdict in jobsdata:
        if "BOX" == jobdict["job_type"] and "box_name" in jobdict.keys() and jobdict["box_name"] == boxdict["insert_job"]:
            lstlevelboxes.append(jobdict)
            
if __name__ == "__main__":
    getboxjobs_hirarchy("input/02092021/New_MMC_Prod_JILs_08312021.txt","reports/boxes_hirarchy_report.txt")
    jobscountsarray = [1,2,3,4,5,6]
    #getmachinejobsreport("input/MMC_ALLJOBS.txt","reports")
    #getboxjobsalllevels("input/02092021/MMC_ALLJOBS_schedule_Wave1_new_remove_1.txt","input/02092021/New_MMC_Prod_JILs_08312021.txt","reports/allleveljobs_report.txt")
    #getjobsnotinwave("output/MMC_ALLJOBS_schedule_Wave1_new_remove_1_update.jil","reports/MMC_ALLJOBS_schedule_Wave1_new_remove_1_update_jobsnotin_wave_report.txt")
    #jobs_inbox_report("input/MMC_ALLJOBS_schedule_Wave1.jil","output/MMC_ALLJOBS_schedule_Wave1.jil","reports/boxjobs_check_report.csv")