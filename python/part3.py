# -*- coding: utf-8 -*-
from os import walk
from pprint import pprint
# from os.path import isfile, join
import re
import numpy as np

from operator import add

# from datetime import datetime  #, time, timedelta
from time import time
from time import localtime
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import DecisionTree

from pyspark import StorageLevel
from pyspark.serializers import MarshalSerializer
import inspect
import os
import sys
import traceback
from tempfile import NamedTemporaryFile
import math
import xml.etree.ElementTree as ET
import random
import shutil

def parseArgs(args):
    print("args {}".format(args))
    parsed_args = {}
    for idx,arg in enumerate(args):
        if idx > 0:
            parse = re.split('=',arg)
            if len(parse) == 2:
                (key, val) = parse
                parsed_args[key]=val
            else:
                print("input error: coudn't parse: {}".format(parse))
    return parsed_args

def vectorFile(texts=None,vector_size=None):
    if texts and vector_size:
        vector_file = os.path.join('data/_pickles/data',texts,'vector_pickle',vector_size)
    else:
       #vector_file = 'data/_pickles/data/text_10/vector_pickle/5'
       #vector_file = 'data/_pickles/data/text_tiny/vector_pickle/10000'
       #vector_file = 'data/_pickles/data/text_part/vector_pickle/10000'
       vector_file = 'data/_pickles/data/text_full_lewes/vector_pickle/300'
       #vector_file = 'data/_pickles/data/text_www_pickle'

    return vector_file

def subjectDict(filepath):
    subject_dict = {}
    filehandle = open( filepath, "r" )
    for line in filehandle:
        result = re.split("\t",line)
        if len(result)==2:
            subject_dict[result[0]] = str(result[1]).strip()
    return subject_dict

def timestring():
    cltime = localtime(time())
    return "{:02d}:{:02d}:{:02d}".format(cltime.tm_hour,cltime.tm_min,cltime.tm_sec)
    #return "{}".format(cltime)


def timeDeltas():
    global s_time
    global is_time
    ds_time = time()
    deltas_since_start = ds_time - s_time
    deltas_since_last = ds_time - is_time
    is_time = ds_time
    return {"time_since_start":deltas_since_start,'time_since_last':deltas_since_last}

def filePrint(string):
        global g_filehandle
        if g_filehandle:
            g_filehandle.write("{}\n".format(string))
        print(string)

def logfuncWithArgs():
        return logfunc("args", 'novals', sys._getframe().f_back)


def logfuncWithVals():
        return logfunc("noargs", 'vals', sys._getframe().f_back)


def logfunc(args=None, vals=None, frame=None):
        elapsed_time = time()-g_start_time
        frame = frame if frame else sys._getframe().f_back
        line_number = frame.f_code.co_firstlineno
        name = frame.f_code.co_name
        argvals = frame.f_locals if vals is "vals" else ""
        argnames = inspect.getargvalues(frame)[0] if args is "args" else ""
        comments = inspect.getcomments(frame)
        comments = comments if comments else ""
        print ("{comments}{time: >9,.3f} {line:>4} {name} {argmames} {argvalse}".format(
            # comments,elapsed_time,line_number,name,argnames,argvals))
            comments=comments,time=elapsed_time,line=line_number,name=name,argmames=argnames,argvalse=argvals))

def logTimeIntervalWithMsg(msg):

    def timeDeltas():
        global s_time
        global is_time
        ds_time = time()
        deltas_since_start = ds_time - s_time
        deltas_since_last = ds_time - is_time
        is_time = ds_time
        return {"time_since_start":deltas_since_start,'time_since_last':deltas_since_last}


    if 1:
        time_deltas = timeDeltas()
        message = msg if msg else ""
        string = ":{0[time_since_start]:7,.3f} :{0[time_since_last]:7,.3f}  {1:}".format(time_deltas, message)
        filePrint(string)

def unpickle(sc,pickle_name):
    #print ("unpickle_name: {}".format(pickle_name))
    rdd = sc.pickleFile(pickle_name, 3)
    #print ("unpickle_rdd: {}".format(rdd.collect()))

    return rdd



def reportResultsForCollectedHashOnOneLine(hash_prediction,hashtable_size,use_hash_signing, total_time=0, filehandle=None):
    '''
    :param hashPrediction:array of COLLECTED prediction results
    :param filefilehandle: file to write results to
    :return:confusionDict
    '''
    #logfuncWithArgs()

    cd = confusionDict(hash_prediction)
    cd['time_since_last'] = float(total_time)

    string = "{0}\t{1}\t{2[TP]}\t{2[FP]}\t{2[FN]}\t{2[TN]}\t" \
             "{2[Recall]:.3f}\t{2[Precision]:.3f}\t{2[Fmeasure]:.3f}\t{2[Accuracy]:.3f}\t" \
             "{2[time_since_last]:0.3f}"\
        .format(hashtable_size, use_hash_signing, cd)
    filePrint(string)
    return cd

def reportResultsForHashOnOneLine(hash_prediction,hashtable_size,use_hash_signing, total_time=0, filehandle=None):
    '''
    :param hashPrediction:prediction generate from hashes
    :param filefilehandle: file to write results to
    :return:confusionDict
    '''
    hash_prediction = hash_prediction.collect()
    return reportResultsForCollectedHashOnOneLine(hash_prediction,hashtable_size,use_hash_signing,total_time,filehandle)


def confusionDict(tupleList):
    #logfuncWithVals()
    mx = [0, 0, 0, 0]

    for (x, y) in tupleList:
        mx[((int(x) << 1) + int(y))] += 1
    dict = {'TN': mx[0], 'FP': mx[1], 'FN': mx[2], 'TP': mx[3]}

    dict['TotalTrue'] = dict['TP'] + dict['FN']
    dict['TotalFalse'] = dict['TN'] + dict['FP']
    dict['TotalSamples'] = len(tupleList)
    dict['TotalPositive'] = dict['TP'] + dict['FP']
    dict['TotalNegative'] = dict['TN'] + dict['FN']
    dict['TotalCorrect'] = dict['TP'] + dict['TN']
    dict['TotalErrors'] = dict['FN'] + dict['FP']
    dict['Recall'] = \
        float(dict['TP']) / dict['TotalTrue'] \
        if dict['TotalTrue'] > 0 else 0
    dict['Precision'] = \
        float(dict['TP']) / dict['TotalPositive'] \
        if dict['TotalPositive'] > 0 else 0
    dict['Sensitivity'] = \
        float(dict['TP']) / dict['TotalSamples'] \
        if dict['TotalSamples'] > 0 else 0
    dict['Specificity'] = \
        float(dict['TN']) / dict['TotalSamples'] \
        if dict['TotalSamples'] > 0 else 0
    dict['ErrorRate'] = \
        float(dict['TotalErrors']) / dict['TotalSamples'] \
        if dict['TotalSamples'] > 0 else 0
    dict['Accuracy'] = \
        float(dict['TotalCorrect']) / dict['TotalSamples'] \
        if dict['TotalSamples'] > 0 else 0
    dict['Fmeasure'] = \
        2 * float(dict['TP']) / (dict['TotalTrue'] + dict['TotalPositive']) \
        if (dict['TotalTrue'] + dict['TotalPositive'] > 0) else 0
    dict['Fmeasure2'] = \
        1 / ((1 / dict['Precision']) + (1 / dict['Recall'])) \
        if dict['Precision'] > 0 and dict['Recall'] > 0 else 0
    dict['Fmeasure3'] = \
        2 * dict['Precision'] * dict['Recall'] / (dict['Precision'] + dict['Recall']) \
        if (dict['Precision'] + dict['Recall'] > 0) else 0

    return dict


'''SECTION 3: CLASSIFICATION'''

def arrayOfFrequentSubjects(rdd):
    rdd = rdd.flatMap(lambda x:[(id_txt,1) for id_txt in x[1]])
    rdd = rdd.map(lambda x:(x[0][1],x[1])).reduceByKey(add)
    rdd = rdd.map(lambda x:(x[1],x[0]))
    rdd = rdd.sortByKey(False)
    rdd = rdd.take(60)
    return rdd

if __name__ == "__main__":
    global s_time
    global is_time
    global g_start_time
    global g_filehandle
    g_start_time = time()
    s_time = g_start_time

    s_time = time()
    is_time = s_time
    g_start_time = time()




    logfile = 'part3_out.txt'
    logfile = os.path.abspath(logfile)
    g_filehandle = open(logfile, 'a')

    parsed_args = parseArgs(sys.argv)

    vector_size =  parsed_args['v'] if 'v' in parsed_args else None
    texts =   parsed_args['t'] if 't' in parsed_args else None
    folds =  int(parsed_args['folds']) if 'folds' in parsed_args else 4
    cores = int(parsed_args['c']) if 'c' in parsed_args else 2
    text_path = parsed_args['t'] if 't' in parsed_args else None
    nb_lambda = float(parsed_args['nbl']) if 'nbl' in parsed_args else 0.1

    mem = parsed_args['m'] if 'm' in parsed_args else 8
    parrellelismMultiplier = int(parsed_args['p']) if 'p' in parsed_args else 8


    masterConfig = "local[{}]".format(cores)

    temp_dir = 'data/_tmp'
    temp_dir = os.path.abspath(temp_dir)

    sparkConf = SparkConf()
    sparkConf.setMaster(masterConfig).setAppName("project")
    sparkConf.set("spark.driver.host","localhost")
    sparkConf.set("spark.default.parallelism",4)


    masterConfig = "local[{}]".format(cores)
    memoryConfig = "{}g".format(mem)
    parallelism = cores*parrellelismMultiplier
    parallelismConfig = "{}".format(parallelism)
    sparkConf = SparkConf()
    sparkConf.setMaster(masterConfig).setAppName("project")
    #sparkConf.setCheckpointDir
    #sparkConf.setCheckpointDir(checkpoint_path)
    sparkConf.set("spark.logConf","true")
    sparkConf.set("spark.logLevel","INFO")
    sparkConf.set("spark.executor.memory",memoryConfig)
    sparkConf.set("spark.python.worker.memory",memoryConfig)
    sparkConf.set("spark.storage.memoryFraction","0.3") #http://stackoverflow.com/a/22742982/1375695

    sparkConf.set("spark.default.parallelism",parallelism)
    sparkConf.set("spark.eventLog.enabled","false")
    sparkConf.set("spark.local.dir",temp_dir)

    sparkConf.set("spark.ui.port","7171")
    sparkConf.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError")



    sc = SparkContext(conf=sparkConf)

    meta_pickle = 'data/_meta/metapickle'
    wd_path = '/Volumes/jHome/_new/_cs/_bigdata/proj'
    vector_file = vectorFile(texts,vector_size)
    if  not os.path.exists(vector_file):
        logTimeIntervalWithMsg( "ERROR: no pickled vector at address {}".format(vector_file))
        exit(0)


    subject_dict = subjectDict('subjects.txt') if os.path.exists('subjects.txt') else None
    filePrint("\n\nstarted run at {}".format(timestring()))
    filePrint("vector_file: {}".format(vector_file))
    if texts:
        filePrint("corpus: {}".format(texts))
        filePrint("vector size: {}".format(vector_size))
    else: filePrint()



    '''

       3 Training classifiers (30%)
    a) Find the 10 most frequent subjects.

    '''
    vectors = unpickle(sc,vector_file).cache()
    #print "vectors take 1: {}".format(vectors.take(2))
    hash_table_size = len(vectors.take(1)[0][1])



    meta_rdd = unpickle(sc,meta_pickle).sortByKey()
    #print ("metapickle :{}".format(meta_rdd.take(2)))
    frequent_subjects = arrayOfFrequentSubjects(meta_rdd)


    #determine the sample folds


    ebook_ids = vectors.keys().collect()

    random.shuffle(ebook_ids)
    ebook_ids_rdd = sc.parallelize(ebook_ids)
    ebook_folds = []


    fold_length = int(len(ebook_ids)/folds)
    for k in range (0,folds):
        start = k*fold_length
        if k == folds-1:
             stop = None
        else:
            stop = start+fold_length
        ebook_fold = ebook_ids[start:stop]
        ebook_folds.append(ebook_fold)


    validation_results = {}

    for subject in frequent_subjects:
        subject = subject[1]
        subject_name = subject
        if subject_dict and subject in subject_dict:
            subject_name = (subject_dict[subject])
        filePrint("\nsubject: {}".format(subject_name))

        def mapSubjectInSubjectList (id,subject_list):
            #if (random.randrange(100)==0):
            #print ("subject[1] {}".format(subject[1]))
            #logfuncWithVals()
            subject_names = [subject_name for (subj_code,subject_name) in subject_list]
            result = 1 if subject in subject_names else 0
            #print ("id: {} result:{}".format(id,result))
            return (id,result)

        #vectors_ids = [id for (id, val) in vectors.collect()]
        #print ('vectors_ids {}'.format(vectors_ids))
        id_lbl = meta_rdd.map(lambda (id,subL): mapSubjectInSubjectList(id,subL))
        #filtered = id_lbl.filter(lambda x: x[0]in vectors_ids)

        #print ("filtered :{}".format(filtered.collect()))

        id_lbl_Tfi = id_lbl.join(vectors)
        #print ("id_lbl_Tfi :{}".format(id_lbl_Tfi.collect()))

        #print ("vectors count {} ".format(vectors.cache().count()))  # [(id, (subject, [vector]))]

        #print ("id_lbl_Tfi count {} ".format(id_lbl_Tfi.cache().count()))  # [(id, (subject, [vector]))]
        #lblPnts = id_lbl_Tfi.map(lambda (tid, lbl_Tfi): LabeledPoint (lbl_Tfi[0],lbl_Tfi[1]))
        #print ("lblPnts :{}".format(lblPnts.take(2)))

        test_rdds = []
        train_rdds = []
        models = []
        array_of_predictions=[]

        string = "hSize\tlambda\tTP\tFP\tFN\tTN\tRecall\tPrcsion\tFMeasre\tAcc\tTime"
        filePrint(string)
        for idx, test_fold in enumerate(ebook_folds):
            lap_time = time()
            remainder_rdd = id_lbl_Tfi
            test_rdd = remainder_rdd.filter(lambda x: x[0] in test_fold).cache()
            #print ("test_rdd count:{}".format(test_rdd.count()))#test set
            #remainder_rdd = remainder_rdd.subtract(test_rdd)
            training_rdd = remainder_rdd.filter(lambda x: x[0] not in ebook_fold)
            #print ("remainder_rdd count:{}".format(remainder_rdd.cache().count()))#test set



        # build the modesl
            def labelPoint(tid, lbl_Tfi):
                #logfuncWithVals()
                labelPoint = LabeledPoint (lbl_Tfi[0],lbl_Tfi[1])
                #print ("labelPoint: label {}".format(labelPoint.label))
                #print ("labelPoint: features {}".format(labelPoint.features))
                return labelPoint

            labelled_rdd = training_rdd.map(lambda (tid, lbl_Tfi): labelPoint(tid, lbl_Tfi))
            #print ("labelled_rdd: {}".format(labelled_rdd.collect()))

            model = NaiveBayes.train(labelled_rdd, nb_lambda )
            #print ("model: {}".format(model))
            models.append(model)

        #make the predictions
            def predict (f,x,model):
                #logfuncWithVals()
                prediction = model.predict(x[1])
                #print ("f: {} class: {} prediction {}".format(f,x[0],prediction))
                return (x[0],prediction)


            prediction_rdd = test_rdd.map(lambda (f, x):
                                          (predict(f,x,model)))
            #prediction_rdd = test_rdd.map(lambda (f, x):
             #
             # '''                           (f, int(model.predict(x).item())))

            prediction_dict = {}
            prediction_dict['model'] = model
            prediction_dict['prediction'] = prediction_rdd

            array_of_predictions.append(prediction_dict)


            prediction_array = prediction_rdd.cache().collect()
            #print("prediction_array: {}".format(prediction_array))
            ltime = time()-lap_time
            reportResultsForCollectedHashOnOneLine(prediction_array,hash_table_size,nb_lambda,ltime, g_filehandle)


            if subject not in validation_results:
                validation_results[subject] = []
            validation_results[subject].append(prediction_dict)

            '''








        #use takeSample to extract test set



    end_s_time = time()
    runtime_s = end_s_time - g_start_time
    filePrint("\ntotal running time:{}".format(runtime_s))
    filePrint("ended run at {}\n".format(timestring()))
    '''
'''
    'subject'[(id,1),(id,0),(id,1)]


    b) For each of these subjects, build a classifier (contains/does not contain the subject) using:
    i) Naive Bayes
    ii) Decision trees
    iii) Logistic regression
    c) Find the best regularisation values for Decision Trees and Logistic Regression using Cross Validation and document the training validation and and testing error for each classification method and each regularisation value.
    4 Efficiency, measurements, and discussion. (30%)
    a) Experiment with different Vector sizes and document the changes in accuracy, running time and memory usage (will be covered after reading week).
    b) Discuss the results of task 4a) and those of task 3) with respect to the known complexities of the used algorithms.
    c) Discuss possible application scenarios and for the classification, and what suitable extensions or optimisations in those scenarios are. Possible aspects to consider are: the encoded subject classes, user needs, natural languages, and the related technical requirements.
    5 Task for pairs Further improvements. (40%)
    a) Double hash vector.
    i) Implement a feature vector that is divided into to parts of similar but different size, apply the same hash function, but perform the modulo operation separately for each part.
    ii) Compare this version experimentally with the previous version in terms of accuracy. iii) Document and interpret the results of your test.
    b) Estimating the author's year of birth. This is an experimental task to see whether we can find stylistic properties of an English text the enable us to estimate the time of the authors birth.
    a) Extract the author's year of birth from the metadata.
    b) Train a linear regression model (LassoWithSGD from mllib) on the TF.IDF vectors. c) Discuss the results and options for improvement.
'''


'''
for subject in frequent_subjects:
        print("subject: {}".format(subject))
        id_lbl = meta_rdd.map(lambda (id,subL): (id, (1 if "#{}".format(subject[1]) in subL else 0)))
        print ("id_lbl :{}".format(id_lbl.sample(False,20)))


        id_lbl_Tfi = id_lbl.join(vectors)
        print ("id_lbl_Tfi count {} ".format(id_lbl_Tfi.count()))  # [(id, (subject, [vector]))]
        lblPnts = id_lbl_Tfi.map(lambda (tid, lbl_Tfi): LabeledPoint (lbl_Tfi[0],lbl_Tfi[1]))
        print ("lblPnts :{}".format(lblPnts.take(2)))
        size = lblPnts.count()
        print ("size {}".format(size))
        foldSize = int(size/10)
        foldRdds = []
        for k in range (0,10):

            #rdd = lblPnts.sample(False,0.1)

            arrayOfLabelPoints = lblPnts.takeSample(False,foldSize,1)
            print("array {}".format(arrayOfLabelPoints[0][0]))
            #print rdd
            #foldRdds.append(rdd)
           # lblPnts = lblPnts.subtract(arrayOfLabelPoints)  #test set
           # lblPnts = lblPnts.filter(lambda x: x not in arrayOfLabelPoints)  #test set




        #trainSet = lblPnts.subtract(testSet)
       # model = NaiveBayes.train(lblPnts,1.0)

        #use takeSample to extract test set

'''

