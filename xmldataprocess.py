import xml.etree.ElementTree as ET
from collections import OrderedDict

def getxmldata(filename,lstjobtagattr,xmlreadprocesslogfilename):
    tree = ET.parse(filename)
    root = tree.getroot()
    #folderscount = 0
    folderjobsdict = OrderedDict()
    for folder in root.findall("SMART_FOLDER"):
        getfolderjobs(folder,lstjobtagattr,folderjobsdict)
        
        for subfolder in folder.findall("SUB_FOLDER"):
            getsubfolderjobs(subfolder,lstjobtagattr,folderjobsdict)
        
    retdataList = []
    joblines = []
    for foldername,fldjbdict in folderjobsdict.items():
        joblines.append("{}\n".format(foldername))
        for jobcount,jobdict in fldjbdict.items():
            if jobdict["JOBNAME"] not in foldername:
                joblines.append("\t{}) {}\n".format(jobcount,jobdict["JOBNAME"]))
            retdataList.append(jobdict)

    with open(xmlreadprocesslogfilename,"w") as jobsfile:
        jobsfile.writelines(joblines)

    return retdataList

def getfolderjobs(folder,lstjobtagattr,folderjobsdict):
    jobnamejobmapdict = OrderedDict()
    jobcount = 1
    folder_subfolder_jobdetails(folder,lstjobtagattr,jobnamejobmapdict,jobcount)
    jobcount = 2
    for job in folder.findall("JOB"):
        #get inconditions data
        condlist = []
        for incond in job.findall("INCOND"):
            condlist.append(incond.attrib["NAME"])
        #get outconditions data
        outcondlist = []
        for outcond in job.findall("OUTCOND"):
            outcondlist.append(outcond.attrib["NAME"])
        #RULE_BASED_CALENDARS
        if job.find("RULE_BASED_CALENDARS") is not None:
            RULE_BASED_CALENDARS = job.find("RULE_BASED_CALENDARS").attrib["NAME"]
        else:
            RULE_BASED_CALENDARS = ""
        #get data from job tag attribute
        jobdict = {}
        for key in lstjobtagattr:
            if key in job.attrib.keys():
                jobdict[key] = job.attrib[key]
            else:
                jobdict[key] = ""
        
        #jobdict["FOLDER_NAME"] = folder.attrib["FOLDER_NAME"]
        jobdict["RULE_BASED_CALENDARS"] = RULE_BASED_CALENDARS
        if len(condlist) > 0:
            jobdict["INCOND"] = condlist
        if len(outcondlist)> 0:
            jobdict["OUTCOND"] = outcondlist
        if job.attrib["JOBNAME"] in jobnamejobmapdict.keys():
            print("{}\t{}".format(job.attrib["JOBNAME"],folder.attrib["FOLDER_NAME"]))
        jobnamejobmapdict[jobcount] = jobdict
        jobcount += 1
    folderjobsdict[folder.attrib["FOLDER_NAME"]] = jobnamejobmapdict

def folder_subfolder_jobdetails(job,lstjobtagattr,jobnamejobmapdict,jobcount):
     #get inconditions data
        condlist = []
        for incond in job.findall("INCOND"):
            condlist.append(incond.attrib["NAME"])
        #get outconditions data
        outcondlist = []
        for outcond in job.findall("OUTCOND"):
            outcondlist.append(outcond.attrib["NAME"])
        #RULE_BASED_CALENDARS
        if job.find("RULE_BASED_CALENDARS") is not None:
            RULE_BASED_CALENDARS = job.find("RULE_BASED_CALENDARS").attrib["NAME"]
        else:
            RULE_BASED_CALENDARS = ""
        #get data from job tag attribute
        jobdict = {}
        for key in lstjobtagattr:
            if key in job.attrib.keys():
                jobdict[key] = job.attrib[key]
            else:
                jobdict[key] = ""
        
        #jobdict["FOLDER_NAME"] = folder.attrib["FOLDER_NAME"]
        jobdict["RULE_BASED_CALENDARS"] = RULE_BASED_CALENDARS
        if len(condlist) > 0:
            jobdict["INCOND"] = condlist
        if len(outcondlist)> 0:
            jobdict["OUTCOND"] = outcondlist
        jobnamejobmapdict[jobcount] = jobdict

def getsubfolderjobs(subfolder,lstjobtagattr,folderjobsdict):
    jobnamejobmapdict = OrderedDict()
    jobcount = 1
    folder_subfolder_jobdetails(subfolder,lstjobtagattr,jobnamejobmapdict,jobcount)
    jobcount = 2
    for job in subfolder.findall("JOB"):
        #get inconditions data
        condlist = []
        for incond in job.findall("INCOND"):
            condlist.append(incond.attrib["NAME"])
        #get outconditions data
        outcondlist = []
        for outcond in job.findall("OUTCOND"):
            outcondlist.append(outcond.attrib["NAME"])
        #RULE_BASED_CALENDARS
        if job.find("RULE_BASED_CALENDARS") is not None:
            RULE_BASED_CALENDARS = job.find("RULE_BASED_CALENDARS").attrib["NAME"]
        else:
            RULE_BASED_CALENDARS = ""
        #get data from job tag attribute
        jobdict = {}
        for key in lstjobtagattr:
            if key in job.attrib.keys():
                jobdict[key] = job.attrib[key]
            else:
                jobdict[key] = ""
        
        #jobdict["FOLDER_NAME"] = folder.attrib["PARENT_FOLDER"]
        jobdict["RULE_BASED_CALENDARS"] = RULE_BASED_CALENDARS
        if len(condlist) > 0:
            jobdict["INCOND"] = condlist
        if len(outcondlist)> 0:
            jobdict["OUTCOND"] = outcondlist
        #jobnamejobmapdict[job.attrib["JOBNAME"]] = jobdict
        jobnamejobmapdict[jobcount] = jobdict
        jobcount += 1
    folderjobsdict[subfolder.attrib["PARENT_FOLDER"] + "/" + subfolder.attrib["JOBNAME"]] = jobnamejobmapdict

def xmldatareport(xmlfilepath,reportfilepath):
    #dataKeys = ["JOBNAME","TIMEFROM","TIMETO","WEEKDAYS","RUN_AS","INSTREAM_JCL","NODEID"]
    datakeys = ["JOBNAME","PARENT_FOLDER","SUB_APPLICATION","APPLICATION",
                #"RUN_AS","TASKTYPE","ACTIVE_FROM","ACTIVE_TILL",
                #"MEMNAME","NODEID","AUTOARCH","CYCLIC","INTERVAL","MAXRERUN","MAXDAYS",
                #"MAXRUNS","MULTY_AGENT","SYSDB","RULE_BASED_CALENDAR_RELATIONSHIP","USE_INSTREAM_JCL",
                #"CREATION_USER","CREATED_BY","CONFIRM","CRITICAL",
                #"TIMEFROM","TIMETO","PRIORITY","CMDLINE","DESCRIPTION"
                ]
    xmldata = getxmldata(xmlfilepath,datakeys,"")
    headers = datakeys
    headersKeysMap = OrderedDict()
    for key in datakeys:
        headersKeysMap[key] = key
    #generateexcelreport(reportfilepath,headers,headersKeysMap,xmldata)
    print(len(xmldata))

def noincondjobs(xmlfilepath,outputpath):
    datakeys = ["JOBNAME","PARENT_FOLDER","SUB_APPLICATION","APPLICATION"]
    xmldata = getxmldata(xmlfilepath,datakeys)
    noincondjobs=[]
    noincondjobs.append("{},{}\n".format("FOLDER_NAME","JOBNAME"))
    for jobdict in xmldata:
        if len(jobdict["INCOND"]) == 0:
            noincondjobs.append("{},{}\n".format(jobdict["PARENT_FOLDER"],jobdict["JOBNAME"]))

    with open(outputpath,'w') as noIncondfile:
        noIncondfile.writelines(noincondjobs)

    #print(len(xmldata))

if __name__ == '__main__':
    noincondjobs("xml_input_data/MASS_BEACON_NEW-for-Krishna.xml","xml_reports/No_Incond_Jobs_Report.csv")
    #xmldatareport("xml_input_data/Centene_autosys_wave1_08072021_1903.xml","xml_reports/Centene_autosys_wave1_08072021_1903.xlsx")