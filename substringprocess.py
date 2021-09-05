from collections import OrderedDict

try:
   import queue
except ImportError:
   import Queue as queue


def find_between(source, first, last,loglines):
    try:
        lstfirst = []
        lstlast = []
        for index in range(len(source)):
            if source[index] == first:
                lstfirst.append(index)
            elif source[index] == last:
                lstlast.append(index)
        
        lstCondJobs = []
        
        for index in range(len(lstfirst)):
            jobincond = source[lstfirst[index] + 1:lstlast[index]]
            if "," in jobincond:
                loglines.append("{}\n".format(jobincond))
                lstCondJobs.append(jobincond.split(",")[0])
            else:
                lstCondJobs.append(jobincond)

        return lstCondJobs
    except ValueError:
        return ""

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
        if "s(" in jobincond or "f(" in jobincond or "e(" in jobincond or "v(" in jobincond or "d(" in jobincond or "t(" in jobincond:
            pass
        else:
            lstCondJobs.append(jobincond)
    
    return list(set(lstCondJobs))

def find_between_v2(source,left,right):
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
    lstvjobs = []
    lstloopbackjobs = []
    lstejobs = []
    lstcondtypes = []
    for left,right in dictindexs.items():
        jobincond = source[left + 1:right]
        strcondtype = source[left-1:left]

        lstcondtypes.append(strcondtype)
        
        if "s(" in jobincond or "f(" in jobincond or "e(" in jobincond or "v(" in jobincond or "d(" in jobincond or "t(" in jobincond:
            #print(jobincond)
            pass
        elif "," in jobincond:
            jobincond = jobincond.split(",")[0]
            lstloopbackjobs.append(jobincond)
            lstCondJobs.append(jobincond)
        else:
            if(strcondtype.lower() == "v"):
                varvalue = source[right+3:right+9]
                lstvjobs.append("{} = {}".format(jobincond,varvalue))
            elif(strcondtype.lower() == "e"):
                lstejobs.append(jobincond)

            lstCondJobs.append(jobincond)
    
    #print(lstejobs,lstvjobs,lstloopbackjobs)

    return list(set(lstCondJobs)),list(set(lstejobs)),list(set(lstvjobs)),list(set(lstloopbackjobs))

if __name__ == '__main__':
    source1 = "(s(ITDWH-XCLUSN-X10000_GEORGIA_BOX) | f(ITDWH-XCLUSN-X10000_GEORGIA_BOX)) & (s(ITDWH-XCLUSN-X10000_HAWAII_BOX) | f(ITDWH-XCLUSN-X10000_HAWAII_BOX)) & (s(ITDWH-XCLUSN-X10000_IA_BOX) | f(ITDWH-XCLUSN-X10000_IA_BOX)) & (s(ITDWH-XCLUSN-X10000_ILLINOIS_BOX) | f(ITDWH-XCLUSN-X10000_ILLINOIS_BOX)) & (s(ITDWH-XCLUSN-X10000_KENTUCKY_BOX) | f(ITDWH-XCLUSN-X10000_KENTUCKY_BOX)) & (s(ITDWH-XCLUSN-X10000_LEIE_BOX) | f(ITDWH-XCLUSN-X10000_LEIE_BOX)) & (s(ITDWH-XCLUSN-X10000_LOUISIANA_BOX) | f(ITDWH-XCLUSN-X10000_LOUISIANA_BOX)) & (s(ITDWH-XCLUSN-X10000_MISSISSIPPI_BOX) | f(ITDWH-XCLUSN-X10000_MISSISSIPPI_BOX)) & (s(ITDWH-XCLUSN-X10000_MISSOURI_BOX) | f(ITDWH-XCLUSN-X10000_MISSOURI_BOX)) & (s(ITDWH-XCLUSN-X10000_NEBRASKA_BOX) | f(ITDWH-XCLUSN-X10000_NEBRASKA_BOX)) & (s(ITDWH-XCLUSN-X10000_NEW_JERSEY_BOX) | f(ITDWH-XCLUSN-X10000_NEW_JERSEY_BOX)) & (s(ITDWH-XCLUSN-X10000_NEY_YORK_BOX) | f(ITDWH-XCLUSN-X10000_NEY_YORK_BOX)) & (s(ITDWH-XCLUSN-X10000_SAM_BOX) | f(ITDWH-XCLUSN-X10000_SAM_BOX)) & (s(ITDWH-XCLUSN-X10000_SOUTH_CAROLINA_BOX) | f(ITDWH-XCLUSN-X10000_SOUTH_CAROLINA_BOX)) & (s(ITDWH-XCLUSN-X10000_TENNESSEE_BOX) | f(ITDWH-XCLUSN-X10000_TENNESSEE_BOX)) & (s(ITDWH-XCLUSN-X10000_TEXAS_BOX) | f(ITDWH-XCLUSN-X10000_TEXAS_BOX)) & (s(ITDWH-XCLUSN-X10000_ARKANSAS_BOX) | f(ITDWH-XCLUSN-X10000_ARKANSAS_BOX)) & (s(ITDWH-XCLUSN-X10000_CALIFORNIA_BOX) | f(ITDWH-XCLUSN-X10000_CALIFORNIA_BOX)) & (s(ITDWH-XCLUSN-X10000_CONNECTICUT_BOX) | f(ITDWH-XCLUSN-X10000_CONNECTICUT_BOX)) & (s(ITDWH-XCLUSN-X10000_FLORIDA_BOX) | f(ITDWH-XCLUSN-X10000_FLORIDA_BOX)) & s(ITDWH-XCLUSN-H10100-INSERT_EXCLSN_INT_SRC_CARE1ST_DGTD_ENT)"
    source = 's(ITDWH-PBM-X20000_FACT_INVOICE_BOX) & s(ITDWH-PBM-X20000_FACT_CET_MIC_BOX) & n(ITDWH-PBM-H20100-cetfact)'
    source2 = "(s(ITDWH-XCLUSN-X10000_GEORGIA_BOX) | f(ITDWH-XCLUSN-X10000_GEORGIA_BOX))"
    source3 = "s(ITDWH-AETNAPDP-CMS-SPLIT-CGD-1200-S-FILEGEN) & (e(ITDWH-AETNAPDP-CMS-SPLIT-CGD-1200-S-FILEGEN) != 1 | e(ITDWH-AETNAPDP-CMS-SPLIT-CGD-1200-S-FILEGEN) = 2)"
    source4 = "v(PR25_processing) = 'YES' & v(jha_processing) = 'YES' & s(is-1-p-jha_env_cdmod_scp_envjhaactetl,02:90) & s(is-1-p-jha_env_cdmod_scp_envjhaposetl) & s(is-1-p-jha_env_cdmod_scp_envjhapos) & (s(is-1-p-jha_env_cdmod_scp_envjhatrnetl) | e(is-1-p-jha_env_cdmod_chk_account_transactions) = 10)"
    source5 = "v(PR19_processing) = 'YES' & v(lpb_processing) = 'YES' & s(is-1-p-lpb_bet_cdint_sendfiles_btalpbmfrsp5,01.00)"
    source6 = "s(is-1-p-loc_adbod_glxspo_glxjca) & v(PR01_processing) = 'YES' & v(jca_processing) = 'YES'"
    source7 = "v(PR24_processing) = 'YES' & v(eam_processing) = 'YES' & ((s(is-1-p-eam_adbod_eveam) & s(is-1-p-eam_adbod_rjaeamtrn) & s(is-1-p-eam_msb_adbod_msbeamtrd,0)) | s(is-1-p-eam-cdbod-dropdead-extract,0))"
    loglines = []
    #lstsubstrings = find_between(source,"(",")",loglines)
    #print(find_between_v2(source4.strip(),"(",")"))
    print(find_between_v2(source7.strip(),"(",")"))