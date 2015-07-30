#!/usr/bin/env python
import logging
import fileinput
import re
import time
import sys
import json
import pprint
import urllib2
import os
import os.path
#from pattern.web import URL
import distutils.util
#import pickle
import BillPairMatch
pp = pprint.PrettyPrinter(indent=4)
tim1 = time.time()

usage = """usage: %prog [options]

Examples:         
   python findLikeBills_hadoop.py --baseDir=/user/alexeys/MaryTests/ --source=lexs
"""
import argparse
parser = argparse.ArgumentParser(description=usage)
#
# directory parms
#
parser.add_argument("--baseDir", dest="baseDir",
                  help="")
# model directories parms
parser.add_argument("--modelDir", dest="modelDir", default="modelLegislation",
                  help="The directory in baseDir that all model legislation files are under. Defaults to 'modelLegislation'")
parser.add_argument("--modelOrg", dest="modelOrg", default=None,
                  help="The org that wrote the model legislation. Defaults to None, which will do all ")
# state legislature directories
parser.add_argument("--source", dest="source",
                  help="The directory under legDir for the source of the state bills.")
parser.add_argument("--legDir", dest="legDir", default="bills",
                  help="The directory in baseDir that all state legislation files are under. Defaults to 'bills'")
parser.add_argument("--state", dest="state",
                  help="")
parser.add_argument("--session", dest="session", default=None,
                  help="The state session to match. Defaults to None, which will do all sessions")
parser.add_argument("--legTextDir", dest="legTextDir", default="text",
                  help="The directory in baseDir that the text bills are. Defaults to 'text'")

#
# Model filter
#
parser.add_argument("--modelbillFilterRe", dest="modelbillFilterRe", default=None,
                  help="If re search on the model bill path matchs the bill will not be used for a match. Defaults to None")
#
# debug
#
parser.add_argument("--debug", dest="debug", default=0, type=int,
                  help="When true ... Defaults to 0(false)")
#
# word group parms
#
parser.add_argument("--grpMax", dest="grpMax", default=13, type=int,
                  help="Max word group size. Defaults to 13")
parser.add_argument("--grpMin", dest="grpMin", default=3, type=int,
                  help="Min word group size. Defaults to 3")
parser.add_argument("--grpFibonacciOnly", dest="grpFibonacciOnly", default=1, type=int,
                  help="When true use only Fibonacci numbers(1,2,3,5,8,13,21) in range >= grpMin and <=grpMax.  Defaults to 1")
parser.add_argument("--grpPercent", dest="grpPercent", default=-1, type=int,
                  help="The pecentage of word group matchs to do a match on all worrd groups. Defaults to 10")
parser.add_argument("--minGrpPercent", dest="minGrpPercent", default=2, type=int,
                  help="The minumum pecentage of all word group matchs to save the bills . Defaults to 1")
parser.add_argument("--grpMinMatch", dest="grpMinMatch", default=10, type=int,
                  help="The min number of matching word groups, used to keep short model bills from matching with a small number of matchs. Defaults to 10")
parser.add_argument("--saveSkipped", dest="saveSkipped", default=0, type=int,
                  help="Save second pass skipped matchs to csv file. Defaults to 0")
parser.add_argument("--runNext", dest="runNext", default=None,
                  help=" Defaults to None")
parser.add_argument("--interweave", dest="interweave", default=1, type=int,
                  help="When true will interweave word match word sets. Defaults to 1")
parser.add_argument("--graphMode", dest="graphMode", default=0, type=int,
                  help="When true will calculate all against all similarity and read it into the graph. Defaults to 0")
parser.add_argument("--useSections", dest="useSections", default=0, type=int,
                  help="When true will calculate similarities on a section level. Defaults to 0")

#*********************************************************
#* get options from rc file
#*********************************************************
logStrs = []
rcFilePath = "%s/.findLikeBills.pyrc" % os.environ['HOME']
logStrs.append("reading option from file:%s" % rcFilePath)
print "reading option from file:%s" % rcFilePath
rcOptions = []
if os.path.exists(rcFilePath):
    RCFILE = open(rcFilePath, "r")
    for line in RCFILE.readlines():
       logStrs.append("Adding parm from rc file - %s" % line)
       print "Adding parm from rc file - %s" % line
       rcOptions.extend(distutils.util.split_quoted(line))
#
rcOptions.extend(sys.argv[1:])

options = parser.parse_args(rcOptions)
#***************************************
#* 
#***************************************
fibNumbers = ( 3,5,8,13,21,34)
grpNums = []
if options.grpFibonacciOnly:
   for i in range(options.grpMin, options.grpMax + 1):
      if i in fibNumbers:
         grpNums.append(i)
else:
   grpNums = range(options.grpMin, options.grpMax + 1)
#***************************************
#* 
#***************************************
modelbillsDir = "%s/%s/text/" % (options.baseDir, options.modelDir)
if (options.modelOrg):
   modelbillsDir += "%s/" % options.modelOrg
print "modelbillsDir=%s" % modelbillsDir
#***************************************
#* 
#***************************************
billsTxtDir = "%s/%s/%s/%s/" % (options.baseDir, options.legDir, options.source, options.legTextDir)
billDirPath = os.path.abspath(billsTxtDir)
hdfs_bill_path = billDirPath
#***************************************
#* 
#***************************************
logDirBasePath = os.path.abspath("%s/%s/%s/logs/" % (options.baseDir, options.legDir, options.source))
if options.state != None:
   billDirPath += "/%s" % options.state
   logDirBasePath += "/%s" % options.state
   if options.session != None:
      billDirPath += "/%s" % options.session
      logDirBasePath += "/%s" % options.session
print "billDirPath=%s" % billDirPath
#***************************************
#* set up logging
#***************************************
if not os.path.exists("%s/logs" % billDirPath):
   os.mkdir("%s/logs" % billDirPath)
logDir = "%s/findLikeBills_" % logDirBasePath + time.strftime("%y%m%d-%H%M%S")
os.makedirs(logDir)
os.environ['LOGDIR'] = logDir
#***************************************
#* 
#***************************************
logger = logging.getLogger("findLikeBills")
if (options.debug):
   logger.setLevel(logging.DEBUG)
#log rc file info
for line in logStrs:
   logger.info(line)
#***************************************
#* 
#***************************************
errMsgLst = []
if not os.path.exists(billDirPath):
   errMsgLst.append("ERROR - billsTxtDir:%s dir not found" % billDirPath)
if not os.path.exists(modelbillsDir):
   errMsgLst.append("ERROR modelbillsDir:%s dir not found" % modelbillsDir)
for errMsg in errMsgLst:
   #logger.error(errMsg)
   print errMsg
if errMsgLst:
   sys.exit()

# Hadoop CPU intensive part goes here
startMatchTime = time.time()
import MRAnalyzer
mr_analyzer = MRAnalyzer.MRAnalyzer(options.baseDir,billDirPath,hdfs_bill_path,options.grpPercent,options.graphMode,options.useSections)
mr_analyzer.applyScheduleAll()

print "Below needs to be fixed first"

#Read matchedBills from file on HDFS
matchedBills = list()
labels = ['modelBill','stateBill','matchPrecent','modelBillContent','stateBillContent']

if not options.graphMode: 
    for state,year in mr_analyzer.scheduler:
        for line in open(mr_analyzer.hdfs_matches_dir+state+"/"+year+"/part-00000").readlines():
            if line.strip(): matchedBills.append(dict(zip(labels, line.split('^^^')))) 
else:
    for tuple1,tuple2 in mr_analyzer.scheduler:
        state1, year1 = tuple1
        state2, year2 = tuple2
        for line in open(mr_analyzer.hdfs_matches_dir+state1+year1+"_"+state2+year2+"/part-00000").readlines():
            if line.strip(): matchedBills.append(dict(zip(labels, line.split('^^^'))))
endMatchTime = time.time()
print "Timing report %s" % (endMatchTime-tim1)

#******************************************
#*
#******************************************
grpTemp = ""
grpHeader = ""
for grpNum in grpNums:
   grpTemp += "%(grp" + str(grpNum) + ")s\t"
   grpHeader += "grp%2.2i\t" % grpNum
header = grpHeader + "skipped\tmodelFileName\tsource\tlegTextDir\tstate\tsession\tbill\tfileName\n"
OFILE = open("%s/matchs.csv" % logDir, 'w')
print "%s/matchs.csv" % logDir
OFILE.write(header)
grpMinMatchSkips = 0

#FIXME
modelNum = 100 #len(open(hdfs_bill_path+"/"+options.state+"/"+options.year+"/catalog_"+options.state+options.year).readlines())
billsMatched = len(matchedBills)
matchResults = list()

#Second pass over bills starts here
from joblib import Parallel, delayed  
import multiprocessing
num_cores = multiprocessing.cpu_count()
#parallel part
def doBill(mb,minGrpPercent,grpNums,interweave):
    matchBill = BillPairMatch.BillPairMatch(mb['modelBillContent'],mb['stateBillContent'],mb['matchPrecent'],interweave)
    matchResults = matchBill.getMatchResults(wordGrps=grpNums, stats=0, skipGrpsPercent=minGrpPercent )
    return matchResults

matchResults = Parallel(n_jobs=num_cores)(delayed(doBill)(mb,options.minGrpPercent,grpNums,options.interweave) for mb in matchedBills)  

#keep this part serial
for mb, mr in zip(matchedBills,matchResults):
   #print type(grpTemp), " ", grpTemp
   #print type(mr), " ", mr 
   csvstr = grpTemp %  mr
   if  mr['skipGrps']:
      #logger.info("skipping match model:%s bill:%s %s" % ( mb['modelBill']['billFileName'], mb['stateBill']['billFileName'], csvstr))
      grpMinMatchSkips += 1
      if not options.saveSkipped:
         continue
   csvstr += "%s\t" % mr['skipGrps']
   #FIXME in case of local file: csvstr += mb['modelBill']
   #in case of all-against-all
   csvstr += mb['modelBill'].split("/")[-1]
   mbl = mb['stateBill'].split("/")
   #FIXME folder structure for local and all-against-all will be different 
   #csvstr += "\t%s\t%s\t\t%s\t%s\t%s\t%s\n" % tuple(mbl[-6:])
   csvstr += "\t%s\t%s\t\t%s\t%s\t%s\n" % tuple(mbl)

   OFILE.write(csvstr)
   logger.debug(csvstr)
OFILE.close()
  
endCsvWrite = time.time()
#print "Timing report (extras on the matched results): %s" % (endCsvWrite-endMatchTime)

#******************************************
#*
#******************************************
statusHash = {
      "TimeModelsLoad": 0,
      "TimeBillMatchFirstPass": int(endMatchTime - startMatchTime),
      "TimeBillMatchSecondPass": int(endCsvWrite - endMatchTime),
      "TimeBillMatchTotal": int(endCsvWrite - startMatchTime),
      "numBillsSearched": mr_analyzer.billsSearched, #billsSearched, just sum of numbers of lines in each catalog file
      "numBillsMatchedFirstPass": billsMatched, #just the number of lines in the mtached bills file
      "numBillsMatched": billsMatched - grpMinMatchSkips, 
      "numBillsMatchsDropedInSecondPass": grpMinMatchSkips,
      "numModelBillsSkiped" : 0, # modelBillsSkiped,
      "numModelBills" : modelNum, #sum of numbers of lines in the model files
      "numBill/ModelComparisons" : modelNum*mr_analyzer.billsSearched, #it is essentially the number of model bills after filtering times the number of statebills #numBillsCompared,
      "rateFirstPass" : 0,
      "rateSecondPass" : 0,
}
if statusHash["TimeBillMatchFirstPass"]:
   statusHash["rateFirstPass"] = int(statusHash["numBill/ModelComparisons"] /statusHash["TimeBillMatchFirstPass"])
if statusHash["TimeBillMatchSecondPass"]:
   statusHash["rateSecondPass"] = int(statusHash["numBillsMatchedFirstPass"] /statusHash["TimeBillMatchSecondPass"])

with open("%s/status.yml" % logDir, 'w') as OFILE:
   OFILE.write("---\nstats:\n")
   for key in statusHash.keys():
      print "%30.30s - %s" % (key, statusHash[key])
      OFILE.write("   %s: %s\n" % (key, statusHash[key]))
   OFILE.write("parms:\n")
   optionsDict = vars(options)
   for key in optionsDict.keys():
      OFILE.write("   %s: %s\n" % (key, optionsDict[key]))
if options.runNext:
    print "had runNext"
    optionsDict['logDir'] = logDir
    cmd = options.runNext % optionsDict
    print "running runNext command: %s" % cmd
    os.system(cmd)

#end_prog = time.time()
#print "Timing report: %s" % (end_prog-startMatchTime)
