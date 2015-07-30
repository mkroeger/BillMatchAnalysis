#!/usr/bin/env python
'''Preprocessor: convert text files into
document_id, document contents as string files'''

import sys
import re
import os
from subprocess import Popen

base_hdfs_dir = "/user/alexeys/MaryTests/bills/lexs/text/"
billWalkOpj =  os.walk(base_hdfs_dir)
for (billdirpath, billdirnames, billfilenames) in billWalkOpj:
    fname = ''
    ofiledir = ''
    for bfname in billfilenames:
        (bfn, bftype) = os.path.splitext(bfname)
        if bftype != ".txt":
            continue
        state = billdirpath.split('/')[-3] 
        year = billdirpath.split('/')[-2]
        ofiledir = "/".join(billdirpath.split("/")[:-1])
        #print ofiledir+"/catalog"+str(state)+str(year)
        billFilePath = '%s/%s' % (billdirpath,bfname)
        sid = billFilePath
      
        #print billFilePath
        data=open(billFilePath).read().replace('\n', ' ')
        fname = "catalog_"+str(state)+str(year)
        name = ofiledir+"/"+fname
        data_tuple = sid+"^^^"+data+"\n"
        if not os.path.isfile(fname):
            ofile = open(fname,"wb")
            ofile.write(data_tuple)
            ofile.close()
        else: 
            ofile = open(fname,"ab") 
            #ofile.seek(0, 2)
            ofile.write(data_tuple)
            ofile.close()
    if fname != '' and ofiledir != '':
        Popen("cp "+fname+" "+ofiledir,shell=True).wait()
        #Popen("rm "+fname,shell=True).wait() 
