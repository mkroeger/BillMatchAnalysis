#!/usr/bin/env python

import sys
import re

#Read default parameters from command line, configured in MRAnalyzer
default_grpSize = int(sys.argv[1])
default_matchThr = float(sys.argv[2])
hashed = int(sys.argv[3])
catalog_models = str(sys.argv[4]).split("/")[-1]
use_sections = int(sys.argv[5]) #0 - false, 1 -true

def getHashedSetsHadoop(line_document, grpSize, output, interweave, hashed=0):
   sid, content = line_document.split("^^^")
   list_of_words = list()
   content = content.translate(None, '?<>,[]{}()*.0123456789:;-\'"_')
   for token in content.split():
      #token = delLettersRe.sub(' ', token)
      token = token.strip()
      token = token.lower()
      if len(token) > 1: list_of_words.append(token)

   wGrps = set()
   step=1
   if not interweave:
      step = grpSize
   for n in range(0, len(list_of_words), step):
      cgrouplst = list_of_words[n:n+grpSize]
      cgrp = " ".join(cgrouplst)
      if hashed:
         cgrp = hash(cgrp)
      wGrps.add(cgrp)

   if output == 'aslist':
       return [sid, wGrps, content.rstrip()]
   else: 
       return (sid, wGrps, content.rstrip())
  
#Section broken
def getHashedSetsHadoopBroken(line_document, grpSize, output, interweave, hashed=0):
    sid, content = line_document.split("^^^")

    #here is where you breaks stuff into sections 
    import re
    regexp1 = r'SECTION \d\.'
    regexp2 = r'\[A> SECTION \d\.'
    regexp3 = r'SECTION \[A> \d\.'
    regexp = re.compile(regexp1 + '|' + regexp2 + '|' + regexp3)

    contents = re.split(regexp, content)
    sids = [sid+"_s"+str(i) for i in range(len(contents)-1)]
    wGrps = list()
 
    for content in contents:
        list_of_words = list()
        content = content.translate(None, '?<>,[]{}()*.0123456789:;-\'"_')
        for token in content.split():
            #token = delLettersRe.sub(' ', token)
            token = token.strip()
            token = token.lower()
            if len(token) > 1: list_of_words.append(token)

        wGrp = set()
        step=1
        if not interweave:
            step = grpSize
        for n in range(0, len(list_of_words), step):
            cgrouplst = list_of_words[n:n+grpSize]
            cgrp = " ".join(cgrouplst)
            if hashed:
                cgrp = hash(cgrp)
            wGrp.add(cgrp)
        #store it in the list of sets
        wGrps.append(wGrp) 

    if output == 'aslist':
        output = []
        for sid,wGrp,content in zip(sids,wGrps,contents): 
            output.append([sid,wGrp,content.rstrip()])
        return output
    else:
        output = []
        for sid,wGrp,content in zip(sids,wGrps,contents): 
            output.append((sid,wGrp,content.rstrip()))
        return output


#Model bill file goes on every worker node, so access as local file
modelBills = list()
labels = ['mid','m_wGrps','m_content']
for modelBill in open(catalog_models).readlines():
    if use_sections:
        all_those_lists = getHashedSetsHadoopBroken(modelBill,default_grpSize,'aslist',1,hashed)
        for ll in all_those_lists:
            modelBills.append(dict(zip(labels,ll)))
    else:
        modelBills.append(dict(zip(labels, getHashedSetsHadoop(modelBill,default_grpSize,'aslist',1,hashed))))

#each line is a new document
for line in sys.stdin:
    if use_sections:
        all_those_tuples = getHashedSetsHadoopBroken(line, default_grpSize, '', 1,hashed)
    else:
        #soo... in this case we are just having a lit containing 1 tuple
        all_those_tuples = list()
        all_those_tuples.append(getHashedSetsHadoop(line, default_grpSize, '', 1,hashed))

    for ituple in all_those_tuples:

        sid, s_wGrps, s_content = ituple
        #for each state bill check against all the model bills
        for modelBill in modelBills:
            intersec = modelBill['m_wGrps'].intersection(s_wGrps)
            matchCnt =len(intersec)
            matchPrecent = 0.
            try:
                matchPrecent = float(matchCnt)/ len(modelBill['m_wGrps']) * 100.
                #remove self loops
                if matchPrecent > default_matchThr and modelBill['mid'] != sid:
                    print "%s^^^%s^^^%s^^^%s^^^%s" % (modelBill['mid'],sid,matchPrecent,modelBill['m_content'],s_content)
            except: pass
