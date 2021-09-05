from collections import OrderedDict
from jilprocess import readjobsdata, writejil
import re
import queue

def timezoneprocess(sourcefilename,updatedfileame,logfilename):
    jobsdata = readjobsdata(sourcefilename)

    timezonedict = OrderedDict()
    timezonedict["SYDNEY"] = "SYD"
    timezonedict["LONDON"] = "GMT"
    timezonedict["EUROPE/BERLIN"] = "CET"
    timezonedict["TORONTO"] = "EST"
    timezonedict["MELBOURNE"] = "SYD"
    timezonedict["CENTRALTIME"] = "CST"

    timezoneloglines = []
    timezoneloglines.append("{},{},{}\n".format("JOBNAME","OLD_TIMEZONE","NEW_TIMEZONE"))
    for jobdict in jobsdata:
        if "timezone" in jobdict.keys():
            if jobdict["timezone"].upper() in timezonedict.keys():
                timezoneloglines.append("{},{},{}\n".format(jobdict["insert_job"],jobdict["timezone"],timezonedict[jobdict["timezone"].upper()]))
                jobdict["timezone"] = timezonedict[jobdict["timezone"].upper()]

    writejil(jobsdata,updatedfileame)

    with open(logfilename,"w") as logfile:
        logfile.writelines(timezoneloglines)

def conditionprocess(sourcefilename,updatedfileame,logfilename):
    jobsdata = readjobsdata(sourcefilename)
    loglines = []
    loglines.append("{},{},{}\n".format("JOBNAME","OLD_CONDITION","NEW_CONDITION"))
    for jobdict in jobsdata:
        if "condition" in jobdict.keys():
            if "= 0" in jobdict["condition"]:
                loglines.append("{},{},{}\n".format(jobdict["insert_job"],jobdict["condition"].replace(","," "),updatedcondition(jobdict["condition"]).replace("= 0","").replace(","," ")))
                jobdict["condition"] = updatedcondition(jobdict["condition"]).replace("= 0","")
    
    writejil(jobsdata,updatedfileame)

    with open(logfilename,"w") as logfile:
        logfile.writelines(loglines)

def updatedcondition(source):
    #source = "e(frms-1-b-epp-testbank-d01t007-inbound-files) = 0 & e(my_test_job1) & w(my_test_job2) = 0"
    #source = "e(frms-1-b-dovetail-santander-d01t006.talend-dailyjob-execute) = 0 & s(frms-1-b-dovetail-santander-d01t006.talend-weeklyjob-execute) & s(frms-1-b-dovetail-santander-d01t006.talend-monthyjob-execute) & s(frms-1-b-dovetail-santander-d01t006.talend-eodjob-execute)"

    leftque = queue.LifoQueue()
    dictindexs = OrderedDict()
    index = 0
    left = "("
    right = ")"
    for item in source:
        if item == left:
            leftque.put(index)
        elif item == right:
            dictindexs[leftque.get()] = index
        index += 1
    replaceindex = []
    for left,right in dictindexs.items():
        jobincond = source[left + 1:right + 5]
        if "= 0" in jobincond:
            replaceindex.append(left-1)
            #print(jobincond,strcondtype,left-1)

    for index in replaceindex:
        source = source[:index] + "s" + source[index+1:]
    #print(source)

    return source

def remove_duplicatecondition(sourcefilename,updatedfilename):
    pass

if __name__ == '__main__':
    #updatedcondition("")
    #remove_duplicatecondition("inputjils/prod job export.txt","outputjils/prod job export_remove_duplicate_cond.jil")
    timezoneprocess("output/MMC_Wave1_Split.jil","output/MMC_Wave1_Split_timezone.jil","logs/MMC_Wave1_Split_timezone_log.csv")
    