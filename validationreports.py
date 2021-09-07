from jilprocess import readjobsdata, writejil
from xmldataprocess import getxmldata
from substringprocess import find_between_v1

def comparereport(jilfilename,xmlfilename,reportfilename):
    jobsdata = readjobsdata(jilfilename)
    datakeys = ["JOBNAME","PARENT_FOLDER","SUB_APPLICATION","APPLICATION"]
    xmljobsdata = getxmldata(xmlfilename,datakeys,"logs/xmlprocess_log.txt")
    #print(len(xmljobsdata))
    reportlines = []
    reportlines.append("{},{},{}\n".format("JOBNAME","CONDITION_JIL_JOBS","CONDITION_XML_JOBS"))
    for jobdict in jobsdata:
        jobname = jobdict["insert_job"]
        lstxmlincond = []
        lstjilcond = []
        if "condition" in jobdict.keys():
            lstjobdictxml = [jobdict for jobdict in xmljobsdata if jobdict["JOBNAME"] == jobname]
            if len(lstjobdictxml) > 0:
                jobdictxml = lstjobdictxml[0]
                if "INCOND" in jobdictxml.keys():
                    lstxmlincond = jobdictxml["INCOND"]
            else:
                lstxmlincond.append("No INCOND for this job in conversion xml")
            loglines = []
            lstjilcond =  find_between_v1(jobdict["condition"],"(",")",loglines)

            reportlines.append("{},{},{}\n".format(jobname,";".join(lstjilcond),";".join(lstxmlincond)))

    with  open(reportfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

def checkjobsincondition(jilfilename,reportfilename):
    jobsdata = readjobsdata(jilfilename)
    lstjobnames = []
    lstjobnamesinCond = []
    loglines = []
    for jobdict in jobsdata:
        if "condition" in jobdict.keys():
            lstjobnamesinCond.extend(find_between_v1(jobdict["condition"],"(",")",loglines))

        lstjobnames.append(jobdict["insert_job"])

    lstjobnamesinCond = list(set(lstjobnamesinCond))
    lstcondjobsnotinwave = [jobname for jobname in lstjobnamesinCond if jobname not in lstjobnames]

    reportlines = []
    for jobname in lstcondjobsnotinwave:
        reportlines.append("{}\n".format(jobname))

    with open(reportfilename,"w") as reportfile:
        reportfile.writelines(reportlines)

if __name__ == '__main__':
    comparereport("output/MMC_ALLJOBS_schedule_Wave1_new_remove_1_update.jil","xmlsource/convertion_xml.xml","reports/cond_compare_report.csv")
    #checkjobsincondition("output/MMC_ALLJOBS_schedule_Wave1_new_remove_1_update.jil","reports/jobsincond_notin_wave_report.txt")
    