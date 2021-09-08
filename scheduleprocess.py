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

    

    #update schedule for child boxes
    lstfinaldata = []
    loglines = []
    loglines.append("{},{},{},{}\n".format("JOBNAME","PARENT_BOX","PARENT_CHAIN","COMMENTS"))
    boxparentboxchain = ""
    for jobdict in jobsdata:
        isloop = True
        if jobdict["date_conditions"].strip() == "0" and jobdict["job_type"].strip() == "BOX":
            jobname = jobdict["insert_job"]
            currentjobname = jobname
            boxparentboxchain = jobname
            while(isloop):
                if jobname in boxparentboxdict.keys():
                    parentboxname = boxparentboxdict[jobname]["insert_job"]
                    boxparentboxchain = "{}-->{}".format(boxparentboxchain,parentboxname)
                    lstparentbox = [jobdict for jobdict in jobsdata if jobdict["insert_job"] == parentboxname]
                    if len(lstparentbox) > 0:
                        parentboxdict = lstparentbox[0]
                        if parentboxdict["date_conditions"].strip() == "1":
                            #apply date condition
                            newjobdict = OrderedDict()
                            scheduledict = jobscheduledict[parentboxname]
                            for key,value in jobdict.items():
                                if key == "date_conditions":
                                    for key,item in scheduledict.items():
                                        newjobdict[key] = item
                                else:
                                    newjobdict[key] = value
                            lstfinaldata.append(newjobdict)
                            jobscheduledict[currentjobname] = scheduledict
                            isloop = False
                            loglines.append("{},{},{},{}\n".format(currentjobname,parentboxname,boxparentboxchain,"Schedule updated"))
                        else:
                            #assign parentjobname to jobname
                            jobname = parentboxname
                    else:
                        isloop = False
                        lstfinaldata.append(jobdict)
                        loglines.append("{},{},{},{}\n".format(currentjobname,parentboxname,boxparentboxchain,"job doesnot exists in jil"))
                        print("{} job doesnot exists in jil".format(parentboxname))
                else:
                    loglines.append("{},{},{},{}\n".format(currentjobname,jobname,boxparentboxchain,"parent job don't have schedule"))
                    lstfinaldata.append(jobdict)
                    isloop = False
            #print("{} : {}".format(currentjobname,boxparentboxchain))
        else:
            lstfinaldata.append(jobdict)

    writejil(lstfinaldata,updatedfilename)

    with open(logfilename,"w") as reportfile:
        reportfile.writelines(loglines)

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

    countsdict = OrderedDict()
    countsdict["Schedule updated"] = 0
    countsdict["parent job don't have schedule"] = 0
    countsdict["job does not exists in current jil"] = 0
    countsdict["Already have schedule"] = 0
    lstfinaldata = []
    loglines = []
    loglines.append("{},{},{},{}\n".format("JOBNAME","PARENTJOBNAME","COMMENTS","PREDICESSOR_CHAIN"))
    reqcount = 1
    for jobdict in jobsdata:
        #trying to update schedule for job having date_conditions = 0 and having predicissor
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "0" and "condition" in jobdict.keys():
            iscontinue = True
            jobname = jobdict["insert_job"]
            currentjobname = jobname
            predicessorchain = jobname
            while (iscontinue):
                parentjobname = jobparentlinkdict[jobname]
                predicessorchain = "{}-->{}".format(predicessorchain,parentjobname)
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
                        jobscheduledict[currentjobname] = scheduledict
                        iscontinue = False
                        countsdict["Schedule updated"] += 1
                        loglines.append("{},{},{},{}\n".format(currentjobname,parentjobname,"schedule updated",predicessorchain))
                    else:
                        #go for next parent
                        if parentjobname in jobparentlinkdict.keys():
                            jobname = parentjobname
                        else:
                            #No schedule for the parent
                            countsdict["parent job don't have schedule"] += 1
                            loglines.append("{},{},{},{}\n".format(currentjobname,parentjobname,"parent job don't have schedule",predicessorchain))
                            iscontinue = False
                            lstfinaldata.append(jobdict)
                else:
                    countsdict["job does not exists in current jil"] += 1
                    loglines.append("{},{},{},{}\n".format(currentjobname,parentjobname,"job does not exists in current jil",predicessorchain))
                    lstfinaldata.append(jobdict)
                    iscontinue = False
            #print(predicessorchain)
        else:
            if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1" and "condition" in jobdict.keys():
                countsdict["Already have schedule"] += 1
            lstfinaldata.append(jobdict)

    writejil(lstfinaldata,scheduledfilename)

    with open(logfilename,"w") as logfile:
        logfile.writelines(loglines)

    totalcondjobs = 0
    print("<--------------CONDITIONJOBS COUNTS----------------->")
    for key,val in countsdict.items():
        totalcondjobs += int(val)
        print("{} : {}".format(key,val))
    print(totalcondjobs)
   
    '''
    for key,val in jobparentlinkdict.items():
        print("{} : {}".format(key,val))
    '''
def updateschedule(sourcefilename,destinationfilename,logfilename):
    jobsdata = readjobsdata(sourcefilename)
    #first prepare jobscheduledict whith jobsname and its schedule properties
    jobscheduledict = OrderedDict()
    lstschprop = ["date_conditions","run_calendar","days_of_week","start_times"]
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            jobscheduledict[jobdict["insert_job"]] = schdict

    iscontinue = True
    runcount = 1
    loglines = []
    loglines.append("{},{},{}\n".format("JOBNAME","UPDATESCHEDULEFROM","COMMENTS"))
    while(iscontinue):
        dummyjobscheduledict = OrderedDict()
        for schdulejobname in jobscheduledict.keys():
            lstdummyjobsdata = []
            for jobdict in jobsdata:
                isapplyschedule = False
                if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "0":
                    #check if schdulejobname in boxname property
                    if "box_name" in jobdict.keys() and schdulejobname == jobdict["box_name"].strip():
                        isapplyschedule = True
                        loglines.append("{},{},{}\n".format(jobdict["insert_job"],schdulejobname,"BOX Level"))
                    #check if schdulejobname in condition property
                    conjobsloglines = []
                    if "condition" in jobdict.keys():
                        lstcondjobs = find_between_v1(jobdict["condition"],"(",")",conjobsloglines)
                        lstschedulejobs = [jobname for jobname in lstcondjobs if jobname == schdulejobname]
                        if len(lstschedulejobs) > 0:
                            isapplyschedule = True
                    if (isapplyschedule):
                        #update schedule
                        scheduledict = jobscheduledict[schdulejobname]
                        for key,item in scheduledict.items():
                            jobdict[key] = item
                        dummyjobscheduledict[jobdict["insert_job"]] = scheduledict
                    else:
                        pass
                else:
                    pass

        if len(dummyjobscheduledict.keys()) > 0:
            jobscheduledict = dummyjobscheduledict
            print("Run ...{} , {}, {}".format(runcount,len(dummyjobscheduledict.keys()),len(jobsdata)))
            runcount += 1
        else:
            iscontinue = False
    
    #To rearrange schedule properties position
    lstfinaljobs = []
    lstschprop = ["date_conditions","run_calendar","days_of_week","start_times"]
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            newjobdict = OrderedDict()
            for key,value in jobdict.items():
                if key == "date_conditions":
                    for key,item in schdict.items():
                        newjobdict[key] = item
                else:
                    newjobdict[key] = value
            lstfinaljobs.append(newjobdict)
        else:
            lstfinaljobs.append(jobdict)

    writejil(lstfinaljobs,destinationfilename)

def updateschedule_V2(sourcefilename,destinationfilename,logfilename):
    jobsdata = readjobsdata(sourcefilename)
    #first prepare jobscheduledict whith jobsname and its schedule properties
    jobscheduledict = OrderedDict()
    lstschprop = ["date_conditions","run_calendar","days_of_week","start_times"]
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            jobscheduledict[jobdict["insert_job"]] = schdict

    iscontinue = True
    runcount = 1
    loglines = []
    loglines.append("{},{},{}\n".format("JOBNAME","UPDATESCHEDULEFROM","COMMENTS"))
    while(iscontinue):
        dummyjobscheduledict = OrderedDict()
        for schdulejobname in jobscheduledict.keys():
            lstdummyjobsdata = []
            scheduledict = jobscheduledict[schdulejobname]
            for jobdict in jobsdata:
                if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "0" and "box_name" not in jobdict.keys():
                    #This is the case for root boxjobs and standloan jobs
                    #check if schdulejobname in condition property
                    conjobsloglines = []
                    if "condition" in jobdict.keys():
                        lstcondjobs = find_between_v1(jobdict["condition"],"(",")",conjobsloglines)
                        lstschedulejobs = [jobname for jobname in lstcondjobs if jobname == schdulejobname]
                        if len(lstschedulejobs) > 0:
                            #update schedule
                            for key,item in scheduledict.items():
                                jobdict[key] = item
                            dummyjobscheduledict[jobdict["insert_job"]] = scheduledict
                else:
                    if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "0" and "box_name" in jobdict.keys():
                        #check if schdulejobname in boxname property then not apply to job but store in dummyjobscheduledict to apply for further jobs
                        if "box_name" in jobdict.keys() and schdulejobname == jobdict["box_name"].strip():
                            dummyjobscheduledict[jobdict["insert_job"]] = scheduledict
                        else:
                            if "condition" in jobdict.keys():
                                conjobsloglines = []
                                lstcondjobs = find_between_v1(jobdict["condition"],"(",")",conjobsloglines)
                                lstschedulejobs = [jobname for jobname in lstcondjobs if jobname == schdulejobname]
                                if len(lstschedulejobs) > 0:
                                    dummyjobscheduledict[jobdict["insert_job"]] = scheduledict
                            

        if len(dummyjobscheduledict.keys()) > 0:
            jobscheduledict = dummyjobscheduledict
            print("Run ...{} , {}, {}".format(runcount,len(dummyjobscheduledict.keys()),len(jobsdata)))
            runcount += 1
        else:
            iscontinue = False
    
    #To rearrange schedule properties position
    lstfinaljobs = []
    for jobdict in jobsdata:
        if "date_conditions" in jobdict.keys() and jobdict["date_conditions"].strip() == "1":
            schdict = OrderedDict()
            for schprop in lstschprop:
                if schprop in jobdict.keys():
                    schdict[schprop] = jobdict[schprop]
            newjobdict = OrderedDict()
            for key,value in jobdict.items():
                if key == "date_conditions":
                    for key,item in schdict.items():
                        newjobdict[key] = item
                else:
                    newjobdict[key] = value
            lstfinaljobs.append(newjobdict)
        else:
            lstfinaljobs.append(jobdict)

    writejil(lstfinaljobs,destinationfilename)

if __name__ == '__main__':
    updateschedule_V2("input/02092021/Test_Mainsource.txt","output/Test_Mainsource_Schedule.txt","output/log.txt")
    #setschedulebasedoncondition("output/MMC_Wave1_Split_Schedule.jil","output/MMC_Wave1_Split_Schedule_schedule.txt","logs/MMC_Wave1_Split_Schedule_log.csv")
    #setboxschedule("input/02092021/New_MMC_Prod_JILs_08312021.txt","output/New_MMC_Prod_JILs_08312021_BOXSCHEDUE.txt","logs/New_MMC_Prod_JILs_08312021_BOXSCHEDUE_log.csv")
    


