from subprocess import Popen
import os 

class MRAnalyzer(object):
    def __init__(self,hdfs_base_dir,billDirPath,hdfs_bill_path,matchThr,graph_mode=0,use_sections=0,grpSize=5,hashed=1):
        self.capacity = 8 #12
        self.scheduler = list()
        self.popen_procs = list()
        self.hdfs_base_dir = hdfs_base_dir #options.baseDir 
        self.hdfs_matches_dir = hdfs_base_dir+"matches/"
        self.hdfs_bill_path = hdfs_bill_path
        self.billDirPath = billDirPath
        self.hdfs_model_dir = hdfs_base_dir+"/modelLegislation/text/"
        self.billsSearched = 0
        self.default_matchThr = str(matchThr)
        self.default_grpSize = str(grpSize)
        self.hashed = str(hashed)
        self.graph = graph_mode
        self.use_sections = str(use_sections)

    def applySchedule(self,last_proc_idx):
        maxjobs = min(self.capacity,len(self.scheduler))
        for njobs in range(maxjobs+1):
            try:
                state,year = self.scheduler[last_proc_idx]
                last_proc_idx += 1
                dir = self.hdfs_bill_path+"/"+state+"/"+year+"/catalog_"+state+year
                self.popen_procs.append(Popen("yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -Dmapreduce.map.memory.mb=3800 -Dmapred.map.tasks=20 -input "+self.hdfs_bill_path+"/"+state+"/"+year+"/catalog_"+state+year+" -output "+self.hdfs_matches_dir+state+"/"+year+" -mapper \'mapper_hasher_matcher.py "+self.default_grpSize+" "+self.default_matchThr+" "+self.hashed+" "+"catalog_models"+" "+self.use_sections+"\' -file mapper_hasher_matcher.py -file "+self.hdfs_model_dir+"catalog_models",shell=True))        
            except: continue
        exit_codes = [p.wait() for p in self.popen_procs]
        return last_proc_idx 

    def applyScheduleGraph(self,last_proc_idx):
        maxjobs = min(self.capacity,len(self.scheduler))
        for njobs in range(maxjobs+1):
            try:
                tuple1,tuple2 = self.scheduler[last_proc_idx]
                state1, year1 = tuple1
                state2, year2 = tuple2
                folder1_str = self.hdfs_bill_path+"/"+state1+"/"+year1+"/catalog_"+state1+year1
                folder2_str = self.hdfs_bill_path+"/"+state2+"/"+year2+"/catalog_"+state2+year2 
                last_proc_idx += 1
                self.popen_procs.append(Popen("yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -Dmapreduce.map.memory.mb=3800 -Dmapred.map.tasks=20 -input "+folder1_str+" -output "+self.hdfs_matches_dir+state1+year1+"_"+state2+year2+" -mapper \'mapper_hasher_matcher.py "+self.default_grpSize+" "+self.default_matchThr+" "+self.hashed+" "+folder2_str+" "+self.use_sections+"\' -file mapper_hasher_matcher.py -file "+folder2_str,shell=True))
            except: continue
        exit_codes = [p.wait() for p in self.popen_procs]
        return last_proc_idx

    def fillSchedule(self):
       walkOpj =  os.walk(self.billDirPath)
       for (dirpath, dirnames, filenames) in walkOpj:
            #logger.info( "model working in dirpath:%s" % dirpath)
            for fname in filenames:
                if "catalog" not in fname: continue
                info = dirpath.split("/")
                state = info[-2]
                year = info[-1]
                try:   
                    self.billsSearched += int(open(dirpath+"/index_"+state+year).readline())
                except: 
                    self.billsSearched += len(open(dirpath+"/"+fname).readlines())
                self.scheduler.append((state,year))

    def fillScheduleGraph(self):
        self.fillSchedule()
        temp = self.scheduler
        self.scheduler = []
        for ifolder in temp:
            for jfolder in temp: 
                #impose causality and remove self loops  
                if ifolder[1] < jfolder[1]: 
                    self.scheduler.append((ifolder,jfolder))
        print self.scheduler

    def applyScheduleAll(self):
        if self.graph:
            idx = 0
            self.fillScheduleGraph()
            while idx < len(self.scheduler):
                idx = self.applyScheduleGraph(idx)
        else:
            idx = 0
            self.fillSchedule()
            while idx < len(self.scheduler):
                idx = self.applySchedule(idx)
