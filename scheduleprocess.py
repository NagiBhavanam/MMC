from collections import OrderedDict
from jilprocess import find_between_v1, readjobsdata, writejil

def getboxes(jobsdata,lstlevelboxes,boxdict):
    for jobdict in jobsdata:
        if "BOX" == jobdict["job_type"] and "box_name" in jobdict.keys() and jobdict["box_name"] == boxdict["insert_job"]:
            lstlevelboxes.append(jobdict)

def gethirarchydictionary(jobsdata,lstleveloneboxes,boxparentboxdict,reportlines):
    for leveloneboxdict in lstleveloneboxes:
        reportlines.append("{}\n".format(leveloneboxdict["insert_job"]))
        lstleveltwoboxes = []
        getboxes(jobsdata,lstleveltwoboxes,leveloneboxdict)
        for leveltwoboxdict in lstleveltwoboxes:
            reportlines.append("\t{}\n".format(leveltwoboxdict["insert_job"]))
            boxparentboxdict[leveltwoboxdict["insert_job"]] = leveloneboxdict
            lstlevelthreeboxes = []
            getboxes(jobsdata,lstlevelthreeboxes,leveltwoboxdict)
            for levelthreeboxdict in lstlevelthreeboxes:
                reportlines.append("\t\t{}\n".format(levelthreeboxdict["insert_job"]))
                boxparentboxdict[levelthreeboxdict["insert_job"]] = leveltwoboxdict
                lstlevelfourboxes = []
                getboxes(jobsdata,lstlevelfourboxes,levelthreeboxdict)
                for levelfourboxdict in lstlevelfourboxes:
                    reportlines.append("\t\t\t{}\n".format(levelfourboxdict["insert_job"]))
                    boxparentboxdict[levelfourboxdict["insert_job"]] = levelthreeboxdict
                    lstlevelfiveboxes = []
                    getboxes(jobsdata,lstlevelfiveboxes,levelfourboxdict)
                    for levelfiveboxdict in lstlevelfiveboxes:
                        reportlines.append("\t\t\t\t{}\n".format(levelfiveboxdict["insert_job"]))
                        boxparentboxdict[levelfiveboxdict["insert_job"]] = levelfourboxdict
                        lstlevelsixboxes = []
                        getboxes(jobsdata,lstlevelsixboxes,levelfiveboxdict)


def setboxschedule(jilfilename,updatedfilename,logfilename):
    jobsdata = readjobsdata(jilfilename)
    #fetch topboxes
    lstleveloneboxes = []
    jobscheduledict = OrderedDict()
    lstschprop = ["date_conditions","run_calendar","days_of_week","start_times"]
    for jobdict in jobsdata:
        if "BOX" == jobdict["job_type"] and "box_name" not in jobdict.keys():
            lstleveloneboxes.append(jobdict)
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            jobscheduledict[jobdict["insert_job"]] = schdict
    
    boxparentboxdict = OrderedDict()
    reportlines = []
    gethirarchydictionary(jobsdata,lstleveloneboxes,boxparentboxdict,reportlines)

    with open(logfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

    #update schedule for child boxes
    lstfinaldata = []
    for jobdict in jobsdata:
        isloop = True
        if jobdict["date_conditions"].strip() == "0" and jobdict["job_type"].strip() == "BOX":
            jobname = jobdict["insert_job"]
            while(isloop):
                if jobname in boxparentboxdict.keys():
                    if boxparentboxdict[jobname]["date_conditions"] == 1:
                        #apply date condition
                        newjobdict = OrderedDict()
                        scheduledict = jobscheduledict[jobname]
                        for key,value in jobdict.items():
                            if key == "date_conditions":
                                for key,item in scheduledict.items():
                                    newjobdict[key] = item
                            else:
                                newjobdict[key] = value
                        lstfinaldata.append(newjobdict)
                        isloop = False
                    else:
                        jobname = boxparentboxdict[jobname]["insert_job"]
                else:
                    levelonebox = [jobdict for jobdict in jobsdata if jobdict["insert_job"] == jobname][0]
                    if levelonebox["date_conditions"].strip() == "1":
                        newjobdict = OrderedDict()
                        #apply date condition
                        scheduledict = jobscheduledict[jobname]
                        for key,value in jobdict.items():
                            if key == "date_conditions":
                                for key,item in scheduledict.items():
                                    newjobdict[key] = item
                            else:
                                newjobdict[key] = value
                        lstfinaldata.append(newjobdict)
                        #jobdict["date_conditions"] = 1
                    else:
                        lstfinaldata.append(jobdict)
                    isloop = False
            #print("{} : {} : {}".format(jobname,jobdict["insert_job"], jobdict["date_conditions"]))
        else:
            lstfinaldata.append(jobdict)

    writejil(lstfinaldata,updatedfilename)
def setschedulebasedoncondition(sourcefilename,scheduledfilename,logfilename):
    jobsdata = readjobsdata(sourcefilename)
    #get predecissor link
    loglines = []
    jobparentlinkdict = OrderedDict()
    for jobdict in jobsdata:
        if "condition" in jobdict.keys():
            lstcondjobs = find_between_v1(jobdict["condition"],"(",")",loglines)
            if len(lstcondjobs) > 0:
               jobparentlinkdict[jobdict["insert_job"]] = lstcondjobs[0]

    jobscheduledict = OrderedDict()
    lstschprop = ["date_conditions","run_calendar","days_of_week","start_times"]
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            jobscheduledict[jobdict["insert_job"]] = schdict

    lstfinaldata = []
    loglines = []
    loglines.append("{},{},{}\n".format("JOBNAME","PARENTJOBNAME","COMMENTS"))
    reqcount = 1
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1" and "condition" in jobdict.keys():
            print(reqcount)
            reqcount += 1
        #trying to update schedule for job having date_conditions = 0 and having predicissor
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "0" and "condition" in jobdict.keys():
            iscontinue = True
            jobname = jobdict["insert_job"]
            currentjobname = jobname
            while (iscontinue):
                if jobname in jobparentlinkdict.keys():
                    parentjobname = jobparentlinkdict[jobname]
                    lstparentjobs = [jobdict for jobdict in jobsdata if jobdict["insert_job"] == parentjobname]
                    if len(lstparentjobs) > 0:
                        parentjobdict = lstparentjobs[0]
                        if "date_conditions" in parentjobdict.keys() and parentjobdict["date_conditions"].strip() == "1":
                            #apply parentjob schedule to currentjob
                            newjobdict = OrderedDict()
                            scheduledict = jobscheduledict[parentjobname]
                            for key,value in jobdict.items():
                                if key == "date_conditions":
                                    for key,item in scheduledict.items():
                                        newjobdict[key] = item
                                else:
                                    newjobdict[key] = value
                            lstfinaldata.append(newjobdict)
                            #Adding current job schedule to jobscheduledict
                            schdict = OrderedDict()
                            for schprop in lstschprop:
                                if schprop in jobdict.keys():
                                    schdict[schprop] = jobdict[schprop]
                            jobscheduledict[jobname] = schdict
                            iscontinue = False
                            loglines.append("{},{},{}\n".format(currentjobname,parentjobname,"schedule updated"))
                        else:
                            #go for next parent
                            if parentjobname in jobparentlinkdict.keys():
                                jobname = parentjobname
                            else:
                                #No schedule for the parent
                                loglines.append("{},{},{}\n".format(currentjobname,parentjobname,"parent job don't have schedule"))
                                iscontinue = False
                                lstfinaldata.append(jobdict)
                    else:
                        loglines.append("{},{},{}\n".format(currentjobname,parentjobname,"job does not exists in current jil"))
                        lstfinaldata.append(jobdict)
                        iscontinue = False
                else:
                    loglines.append("{},{},{}\n".format(currentjobname,jobname,"don't have parent"))
                    iscontinue = False
                    lstfinaldata.append(jobdict)
        else:
            lstfinaldata.append(jobdict)

    writejil(lstfinaldata,scheduledfilename)

    with open(logfilename,"w") as logfile:
        logfile.writelines(loglines)

    '''
        having CONDITION jobs : 1686
        date_conditions : 1                : 77
        job does not exists in current jil : 244
        parent job don't have schedule : 998
        schedule updated : 367 
    '''
    '''
    for key,val in jobparentlinkdict.items():
        print("{} : {}".format(key,val))
    '''

if __name__ == '__main__':
    setschedulebasedoncondition("input/02092021/New_MMC_Prod_JILs_08312021.txt","output/New_MMC_Prod_JILs_08312021_schedule.txt","logs/New_MMC_Prod_JILs_08312021_schedule_log.csv")
    #setboxschedule("input/02092021/Test_Mainsource.txt","output/Test_Mainsource.txt","logs/Test_Mainsource_log.txt")
    


