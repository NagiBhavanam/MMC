from jilprocess import readjobsdata, writejil

def pullboxjobs(boxessourcefilename,mainsourcefilename,destinationfilename,logfilename):
    #get all boxes
    jobsdataforboxes = readjobsdata(boxessourcefilename)
    mainsourcejobsdata = readjobsdata(mainsourcefilename)
    lstboxes = []
    for jobdict in jobsdataforboxes:
        if jobdict["job_type"] == "BOX":
            lstboxes.append(jobdict["insert_job"])
        if "box_name" in jobdict.keys():
            lstboxes.append(jobdict["box_name"])

    lstuniqueboxes = list(set(lstboxes))
    reportlines = []
    lstfinaljobs = []
    lstaddedjobnames = []
    lstjobsaddedtoreport = []
    lstuniqueboxes = [boxname for boxname in lstuniqueboxes if "emea" in boxname]
    for leveloneboxname in lstuniqueboxes:
        if leveloneboxname not in lstjobsaddedtoreport:
            reportlines.append("{}\n".format(leveloneboxname))
            lstjobsaddedtoreport.append(leveloneboxname)
        levelonebox = [jobdict for jobdict in jobsdataforboxes if jobdict["insert_job"] == leveloneboxname][0]
        lstalljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == leveloneboxname]
        lstleveltwoboxjobs = [jobdict for jobdict in lstalljobsinbox if jobdict["job_type"] == "BOX"]
        lstnonboxjobs = [jobdict for jobdict in lstalljobsinbox if jobdict["job_type"] != "BOX"]
        getreportlines(reportlines,lstnonboxjobs,"\t{}\n",lstjobsaddedtoreport)
        #lstfinaljobs.append(levelonebox)
        lstnonboxjobs.append(levelonebox)
        #lstfinaljobs.extend(lstnonboxjobs)
        lstfinaljobs,lstaddedjobnames = preparefinallist(lstnonboxjobs,lstfinaljobs,lstaddedjobnames)
        for leveltwoboxdict in lstleveltwoboxjobs:
            if leveltwoboxdict["insert_job"] not in lstjobsaddedtoreport:
                reportlines.append("\t{}\n".format(leveltwoboxdict["insert_job"]))
                lstjobsaddedtoreport.append(leveltwoboxdict["insert_job"])
            lstleveltwoalljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == leveltwoboxdict["insert_job"]]
            lstlevelthreeboxjobs = [jobdict for jobdict in lstleveltwoalljobsinbox if jobdict["job_type"] == "BOX"]
            lstnonboxjobs = [jobdict for jobdict in lstleveltwoalljobsinbox if jobdict["job_type"] != "BOX"]
            getreportlines(reportlines,lstnonboxjobs,"\t\t{}\n",lstjobsaddedtoreport)
            #lstfinaljobs.append(leveltwoboxdict)
            lstnonboxjobs.append(leveltwoboxdict)
            #lstfinaljobs.extend(lstnonboxjobs)
            lstfinaljobs,lstaddedjobnames = preparefinallist(lstnonboxjobs,lstfinaljobs,lstaddedjobnames)
            for levelthreeboxdict in lstlevelthreeboxjobs:
                 if levelthreeboxdict["insert_job"] not in lstjobsaddedtoreport:
                    reportlines.append("\t\t{}\n".format(levelthreeboxdict["insert_job"]))
                    lstjobsaddedtoreport.append(levelthreeboxdict["insert_job"])
                 lstlevelthreealljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == levelthreeboxdict["insert_job"]]
                 lstlevelfoureboxjobs = [jobdict for jobdict in lstlevelthreealljobsinbox if jobdict["job_type"] == "BOX"]
                 lstnonboxjobs = [jobdict for jobdict in lstlevelthreealljobsinbox if jobdict["job_type"] != "BOX"]
                 getreportlines(reportlines,lstnonboxjobs,"\t\t\t{}\n",lstjobsaddedtoreport)
                 #lstfinaljobs.append(levelthreeboxdict)
                 lstnonboxjobs.append(levelthreeboxdict)
                 #lstfinaljobs.extend(lstnonboxjobs)
                 lstfinaljobs,lstaddedjobnames = preparefinallist(lstnonboxjobs,lstfinaljobs,lstaddedjobnames)
                 for levelfoureboxdict in lstlevelthreeboxjobs:
                     if levelfoureboxdict["insert_job"] not in lstjobsaddedtoreport:
                        reportlines.append("\t\t\t{}\n".format(levelfoureboxdict["insert_job"]))
                        lstjobsaddedtoreport.append(levelfoureboxdict["insert_job"])
                     lstlevelfourealljobsinbox = [jobdict for jobdict in mainsourcejobsdata if "box_name" in jobdict.keys() if jobdict["box_name"] == levelfoureboxdict["insert_job"]]
                     lstlevelfiveboxjobs = [jobdict for jobdict in lstlevelfourealljobsinbox if jobdict["job_type"] == "BOX"]
                     lstnonboxjobs = [jobdict for jobdict in lstlevelfourealljobsinbox if jobdict["job_type"] != "BOX"]
                     getreportlines(reportlines,lstnonboxjobs,"\t\t\t\t{}\n",lstjobsaddedtoreport)
                     #lstfinaljobs.append(levelfoureboxdict)
                     lstnonboxjobs.append(levelfoureboxdict)
                     #lstfinaljobs.extend(lstnonboxjobs)
                     lstfinaljobs,lstaddedjobnames = preparefinallist(lstnonboxjobs,lstfinaljobs,lstaddedjobnames)
    
    writejil(lstfinaljobs,destinationfilename)

    with open(logfilename,"w") as logfile:
        logfile.writelines(reportlines)

def getreportlines(reportlines,lstjobs,levelstr,lstjobsaddedtoreport):
    for jobdict in lstjobs:
        if jobdict["insert_job"] not in lstjobsaddedtoreport:
            reportlines.append(levelstr.format(jobdict["insert_job"]))
            lstjobsaddedtoreport.append(jobdict["insert_job"])

def preparefinallist(lstjobstoadd,lstfinaljobs,lstaddedjobnames):
    for jobdict in lstjobstoadd:
        if jobdict["insert_job"] not in lstaddedjobnames:
            lstfinaljobs.append(jobdict)
            lstaddedjobnames.append(jobdict["insert_job"])
        else:
            print(jobdict["insert_job"])
    return lstfinaljobs,lstaddedjobnames

if __name__ == '__main__':
    pullboxjobs("input/02092021/MMC_ALLJOBS_schedule_Wave1_new_remove_1.txt","input/02092021/New_MMC_Prod_JILs_08312021.txt","output/MMC_Wave1_Split_NoSchedule.jil","logs/MMC_Wave1_Split_log.txt")
    