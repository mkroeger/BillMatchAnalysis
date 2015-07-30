import re
import numpy as np
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
import scam_dist as scam
import nltk.cluster.util as nltkutil
from sklearn.feature_extraction.text import TfidfVectorizer
import logging
logger = logging.getLogger("BillPairMatch")

class BillPairMatch(object):
   #***************************************
   #* __init__
   #***************************************
   def __init__(self, model_content, statebill_content, match, interweave=1):
      self.model_content = model_content
      self.statebill_content = statebill_content
      self.match = match
      self.interweave = interweave

   #***************************************
   #* getScikitNum
   #***************************************
   def getScikitNum(self):
      documents = [self.model_content, self.statebill_content]
      tfidf = TfidfVectorizer().fit_transform(documents)
      pairwise_similarity = tfidf * tfidf.T
      return(pairwise_similarity[(0, 1)])

 
   #***************************************
   #* getScamNum
   #***************************************
   def getScamNum(self):
      return self.doTest(scam.scam_distance)
   
   def getEuclideanNum(self):
      return self.doTest(nltkutil.euclidean_distance)
   
   def getCosineNum(self):
      return self.doTest(nltkutil.cosine_distance)

   #***************************************
   #* doTest
   #***************************************
   def doTest(self, fuc):
      documents = [self.model_content, self.statebill_content]
      cats = [ 1,1] # not sure what this is doing may need to change
      tfidf = TfidfVectorizer().fit_transform(documents)
      # no need to normalize, since Vectorizer will return normalized tf-idf
      pairwise_similarity = tfidf * tfidf.T
      pipeline = Pipeline([
                  ("vect", CountVectorizer(min_df=0, stop_words="english")),
                  ("tfidf", TfidfTransformer(use_idf=False))])
      tdMatrix = pipeline.fit_transform(documents, cats)
      doc1 = np.asarray(tdMatrix[0, :].todense()).reshape(-1)
      doc2 = np.asarray(tdMatrix[1, :].todense()).reshape(-1)
      return fuc(doc1, doc2)

   #***************************************
   #*  getMatchResults
   #***************************************
   def getMatchResults(self, wordGrps=None, stats=0, skipGrpsPercent=2):
       if wordGrps == None:
          wordGrps = (5,8,13)
       results = {}
       results['skipGrps'] = 0
       skipGrps = 0 # used to skip matchs after a low match < 2
       for wordGrpLen in wordGrps:
          cPercent = 0
          if not skipGrps:
             cPercent = self.getMatchPercentage(grpSize=wordGrpLen,hashed=1) #self.wordGrpMatch(other, grpSize=wordGrpLen, hashed=1)['matchPercent']
             if cPercent < skipGrpsPercent:
                skipGrps = 1
                results['skipGrps'] = wordGrpLen
          results["grp%s" % wordGrpLen] = cPercent
       if stats:
          results['scikit'] = "%3.3f" % self.getScikitNum(other)
          results['cosine'] = "%3.3f" % self.getCosineNum(other)
          results['scamNum'] = "%3.3f" % self.getScamNum(other)
          results['euclideanNum'] = "%3.3f" % self.getEuclideanNum(other)
       return results

   def getHashedSets(self,content, grpSize,hashed):
        wGrps = set()
        list_of_words = list()
        for token in content.split():
            #token = delLettersRe.sub(' ', token)
            token = token.strip()
            token = token.lower()
            if len(token) > 1: list_of_words.append(token)

        step=1
        if not self.interweave:
           step = grpSize
        for n in range(0, len(list_of_words), step):
            cgrouplst = list_of_words[n:n+grpSize]
            cgrp = " ".join(cgrouplst)
            if hashed:
                cgrp = hash(cgrp)
            wGrps.add(cgrp)
        return wGrps

   def getMatchPercentage(self,grpSize,hashed):
        m_wGrps = self.getHashedSets(self.model_content, grpSize,hashed)
        s_wGrps = self.getHashedSets(self.statebill_content, grpSize,hashed)

        intersec = m_wGrps.intersection(s_wGrps)
        matchCnt =len(intersec)
        matchPrecent = 0
        if len(m_wGrps):
            matchPrecent = int((float(matchCnt)/ len(m_wGrps) * 100))
        return matchPrecent
