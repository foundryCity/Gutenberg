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

'''OUTPUT'''

def pickle(rdd, name=None,delete_files=0):
    #logfuncWithVals()

    """

    :rtype : string
    """
    if name:
        if os.path.exists(name) and delete_files:
            shutil.rmtree(name)
        if os.path.exists(name):
            print ("already pickled: {}".format(name))
            return name
    else:
        tmp = NamedTemporaryFile(delete=True)
        tmp.close()
        name = tmp.name
    rdd.saveAsPickleFile(name,3)
    return name

def unpickle(sc,pickle_name):
    #print ("unpickle_name: {}".format(pickle_name))
    rdd = sc.pickleFile(pickle_name, 3)
    #print ("unpickle_rdd: {}".format(rdd.collect()))

    return rdd

def summariseResultsForCollectedHashOnOneLine(subject,hash_prediction,hashtable_size,use_hash_signing, total_time=0, filehandle=None):
    '''
    :param hashPrediction:array of COLLECTED prediction results
    :param filefilehandle: file to write results to
    :return:confusionDict
    '''
    #logfuncWithArgs()

    cd = confusionDict(hash_prediction)
    cd['time_since_last'] = float(total_time)

    string = "\t{0}\t{1}\t{2[TP]}\t{2[FP]}\t{2[FN]}\t{2[TN]}\t" \
             "{2[Recall]:.3f}\t{2[Precision]:.3f}\t{2[Fmeasure]:.3f}\t{2[Accuracy]:.3f}\t" \
             "{2[time_since_last]:0.3f}"\
        .format(hashtable_size, use_hash_signing, cd)
    substring = "{}\t{}".format(string,subject[0:20])
    filePrint(substring)

    return cd

def reportResultsForCollectedHashOnOneLine(hash_prediction,hashtable_size,use_hash_signing, total_time=0, filehandle=None):
    '''
    :param hashPrediction:array of COLLECTED prediction results
    :param filefilehandle: file to write results to
    :return:confusionDict
    '''
    #logfuncWithArgs()

    cd = confusionDict(hash_prediction)
    cd['time_since_last'] = float(total_time)

    string = "\t{0}\t{1}\t{2[TP]}\t{2[FP]}\t{2[FN]}\t{2[TN]}\t" \
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



def arrayOfFrequentSubjectsFromMetaDict(rdd, count=60):
    rdd = rdd.flatMap(lambda x:[(id_txt,1) for id_txt in x[1]['subjects']])
    rdd = rdd.reduceByKey(add)
    rdd = rdd.map(lambda x:(x[1],x[0]))
    rdd = rdd.sortByKey(False)
    rdd = rdd.take(count)
    return rdd

def arrayOfFrequentSubjects(rdd, count=60):
    rdd = rdd.flatMap(lambda x:[(id_txt,1) for id_txt in x[1]])
    rdd = rdd.map(lambda x:(x[0][1],x[1])).reduceByKey(add)
    rdd = rdd.map(lambda x:(x[1],x[0]))
    rdd = rdd.sortByKey(False)
    rdd = rdd.take(count)
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






    parsed_args = parseArgs(sys.argv)

    vector_size =  parsed_args['v'] if 'v' in parsed_args else None
    texts =   parsed_args['t'] if 't' in parsed_args else None
    folds =  int(parsed_args['folds']) if 'folds' in parsed_args else 4
    cores = int(parsed_args['c']) if 'c' in parsed_args else 2
    text_path = parsed_args['t'] if 't' in parsed_args else None

    model_type = "dt" if 'dt' in parsed_args else "nb"
    dt_depth = int(parsed_args['dt']) if 'dt' in parsed_args else 0
    nb_lambda = float(parsed_args['nb']) if 'nb' in parsed_args else 0.1
    if model_type == "dt":
        model_type_string = "decision tree"
        model_variable_label = "depth"
        model_variable = dt_depth
    else:
        model_type_string = "naive bayes"
        model_variable_label = "lambda"
        model_variable = nb_lambda


    subject_count = int(parsed_args['s']) if 's' in parsed_args else 60

    mem = parsed_args['m'] if 'm' in parsed_args else 8
    parrellelismMultiplier = int(parsed_args['p']) if 'p' in parsed_args else 8

    logfile = 'logs/part3_{}_{}_{}_{}.txt'.format(texts,vector_size,model_type,model_variable)
    logfile = os.path.abspath(logfile)
    g_filehandle = open(logfile, 'a')

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



    wd_path = '/Volumes/jHome/_new/_cs/_bigdata/proj'
    vector_file = vectorFile(texts,vector_size)
    if  not os.path.exists(vector_file):
        logTimeIntervalWithMsg( "ERROR: no pickled vector at address {}".format(vector_file))
        exit(0)

    logTimeIntervalWithMsg ("{}".format(parsed_args))
    logTimeIntervalWithMsg("sparkConf.toDebugString() {}".format(sparkConf.toDebugString()))



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


    meta_array = 1


    if meta_array:
        meta_pickle = 'data/_meta/metapickle'
        meta_rdd = unpickle(sc,meta_pickle).sortByKey()
        frequent_subjects = arrayOfFrequentSubjects(meta_rdd,subject_count)
    else:
        meta_pickle = 'data/_meta/_new/data8'
        meta_rdd = unpickle(sc,meta_pickle).sortByKey()
        frequent_subjects = arrayOfFrequentSubjectsFromMetaDict(meta_rdd, subject_count)
    print ("frequent subjects: {}".format(frequent_subjects))


    #determine the sample folds


    ebook_ids = vectors.keys().collect()

    random.shuffle(ebook_ids)
    ebook_ids_rdd = sc.parallelize(ebook_ids)
    validation_folds = []
    test_fold = None
    #print ("ebook_ids_len: {}".format(len(ebook_ids)))
    #print ("folds_len: {}".format(int(len(ebook_ids)/folds)))

    number_of_ebooks = len(ebook_ids)

    if number_of_ebooks < folds:
        folds = number_of_ebooks

    fold_length = int(number_of_ebooks/folds)
    for k in range (0,folds):
        start = k*fold_length
        if k == folds-1:
             test_fold = ebook_ids[start:]
        else:
            stop = start+fold_length
            validation_fold = ebook_ids[start:stop]
            validation_folds.append(validation_fold)

    validation_results = {}
    summary_results = {}
    test_dict={}
    model_for_subject = {}

    def subjectName(subject):
        subject_name = subject
        if subject_dict and subject in subject_dict:
            subject_name = "{} / {}".format (subject,subject_dict[subject])
        return subject_name

    for subject in frequent_subjects:
        subject = subject[1]
        subject_name = subjectName(subject)
        filePrint("\nsubject: {}".format(subject_name))

        def mapSubjectInSubjectList (id,subject_list):
            #if (random.randrange(100)==0):
            #print ("subject[1] {}".format(subject[1]))
            #logfuncWithVals()
            subject_names = [subject_name for (subj_code,subject_name) in subject_list]
            result = 1 if subject in subject_names else 0
            #print ("id: {} result:{}".format(id,result))
            return (id,result)

        def mapSubjectInSubjectListDict (id,subject_list):
            #if (random.randrange(100)==0):
            #print ("subject[1] {}".format(subject[1]))
            #logfuncWithVals()
            #subject_names = [subject_name for (subj_code,subject_name) in subject_list]
            result = 1 if subject in subject_list else 0
            #print ("id: {} result:{}".format(id,result))
            return (id,result)

        #vectors_ids = [id for (id, val) in vectors.collect()]
        #print ('vectors_ids {}'.format(vectors_ids))

        #[(id,has_subect)...])
        #print ("meta_rdd :{}".format(meta_rdd.take(5)))
        if meta_array:
            id_lbl = meta_rdd.map(lambda (id,subL): mapSubjectInSubjectList(id,subL))
        else:
            id_lbl = meta_rdd.map(lambda (id,dict): mapSubjectInSubjectListDict(id,dict['subjects']))


        #filtered = id_lbl.filter(lambda x: x[0]in vectors_ids)

        #print ("id_lbl :{}".format(id_lbl.collect()))
        #print ("vectors :{}".format(vectors.collect()))

        id_lbl_Tfi = id_lbl.join(vectors)
        #print ("id_lbl_Tfi :{}".format(id_lbl_Tfi.take(2)))

        #remove the test set
        test_dict[subject] = id_lbl_Tfi.filter(lambda x: x[0] in test_fold).cache()
        id_lbl_Tfi = id_lbl_Tfi.filter(lambda x: x[0] not in test_fold)



        #print ("vectors count {} ".format(vectors.cache().count()))  # [(id, (subject, [vector]))]

        #print ("id_lbl_Tfi count {} ".format(id_lbl_Tfi.cache().count()))  # [(id, (subject, [vector]))]
        #lblPnts = id_lbl_Tfi.map(lambda (tid, lbl_Tfi): LabeledPoint (lbl_Tfi[0],lbl_Tfi[1]))
        #print ("lblPnts :{}".format(lblPnts.take(2)))

        test_rdds = []
        train_rdds = []
        array_of_predictions=[]

        string = "\thSize\t{}\tTP\tFP\tFN\tTN\tRecall\tPrcsion\tFMeasre\tAcc\tTime".format(model_variable_label)
        filePrint(string)
        for idx, validation_fold in enumerate(validation_folds):
            lap_time = time()
            remainder_rdd = id_lbl_Tfi
            validation_rdd = remainder_rdd.filter(lambda x: x[0] in validation_fold).cache()
            #print ("test_rdd count:{}".format(test_rdd.count()))#test set
            #remainder_rdd = remainder_rdd.subtract(test_rdd)
            training_rdd = remainder_rdd.filter(lambda x: x[0] not in validation_fold)
            #print ("training_rdd: {}".format(training_rdd.collect()))


            def labelPoint(tid, lbl_Tfi):
                                #logfuncWithVals()
                                labelPoint = LabeledPoint (lbl_Tfi[0],lbl_Tfi[1])
                                #print ("labelPoint: label {}".format(labelPoint.label))
                                #print ("labelPoint: features {}".format(labelPoint.features))
                                return labelPoint

            if model_type == 'dt':
                labelled_rdd = training_rdd.map(lambda (tid, lbl_Tfi): labelPoint(tid, lbl_Tfi))
                labelled_rdd_pickle = pickle(labelled_rdd)
                model = DecisionTree.trainClassifier(unpickle(sc,labelled_rdd_pickle), numClasses=2, categoricalFeaturesInfo={},
                                                     impurity='gini', maxDepth=dt_depth, maxBins=100)
                labelled_test_rdd = validation_rdd.map(lambda(tid, lbl_Tfi): labelPoint(tid, lbl_Tfi)).cache()
                prediction_rdd = model.predict(labelled_test_rdd.map(lambda x: x.features))
                truth_rdd = labelled_test_rdd.map(lambda x: x.label)
                prediction_rdd = prediction_rdd.zip(truth_rdd)


            elif model_type == 'nb':
                labelled_rdd = training_rdd.map(lambda (tid, lbl_Tfi): labelPoint(tid, lbl_Tfi))
                #print ("labelled_rdd: {}".format(labelled_rdd.collect()))
                model = NaiveBayes.train(labelled_rdd, nb_lambda )
                #print ("model: {}".format(model))

            #make the predictions
                def nbPredict (x,model):
                    #logfuncWithVals()
                    prediction = model.predict(x[1])
                    #print ("f: {} class: {} prediction {}".format(f,x[0],prediction))
                    return (x[0],prediction)

                prediction_rdd = validation_rdd.map(lambda (f, x):(nbPredict(x,model)))

            prediction_dict = {}
            prediction_dict['model'] = model
            prediction_dict['prediction'] = prediction_rdd
            model_for_subject[subject] = model


            array_of_predictions.append(prediction_dict)


            prediction_array = prediction_rdd.cache().collect()
            #print("prediction_array: {}".format(prediction_array))
            ltime = time()-lap_time
            reportResultsForCollectedHashOnOneLine(prediction_array,hash_table_size,model_variable,ltime, g_filehandle)

            prediction_dict['lap_time'] = ltime

            if subject not in validation_results:
                validation_results[subject] = []
            validation_results[subject].append(prediction_dict)

    'summarise results'
    #print ("validation results")
    #pprint(validation_results)

    for subject, subject_results in validation_results.iteritems():
        for hash_prediction in subject_results:
            if subject in summary_results:
                summary_results[subject]['prediction'] = summary_results[subject]['prediction']\
                                                                           .union(hash_prediction['prediction'])

                summary_results[subject]['total_time'] += hash_prediction['lap_time']

                #print summary_results[hash_table_size].take(20)
            else:
                summary_results[subject] = {}
                summary_results[subject]['prediction'] = hash_prediction['prediction']
                summary_results[subject]['total_time'] = hash_prediction['lap_time']

                #print summary_results[hash_table_size].take(20)

    print ("\nresults for model type: {}".format(model_type_string))
    #pprint(summary_results)


    filePrint ("\n\ncross-validation totals - all folds\n")

    string = "\thSize\t{}\tTP\tFP\tFN\tTN\t" \
        "Recall\tPrcsion\tFMeasre\tAcc\tTime".format(model_variable_label)
    filePrint(string)
    for subject in frequent_subjects:
        subject = subject[1]
        subject_name = subjectName(subject)
        result = summary_results[subject]
        prediction = result['prediction']
        total_time = result['total_time']
        prediction = prediction.collect()
        summariseResultsForCollectedHashOnOneLine(subject_name,prediction,hash_table_size,model_variable,total_time,g_filehandle)





    filePrint ("\n\ntest results\n")

    string = "\thSize\t{}\tTP\tFP\tFN\tTN\t" \
        "Recall\tPrcsion\tFMeasre\tAcc\tTime".format(model_variable_label)
    filePrint(string)

    s_time = time()
    sums_time = 0
    totals_time = 0
    cumulative_lap_time = 0
    for subject in frequent_subjects:
        subject = subject[1]
        subject_name = subjectName(subject)
        test_start_s_time = time()
        test_prediction = None
        test_rdd = test_dict[subject]
        model = model_for_subject[subject]
        if model_type is "nb":
            def nbPredict (x,model):
                    #logfuncWithVals()
                    prediction = model.predict(x[1])
                    #print ("f: {} class: {} prediction {}".format(f,x[0],prediction))
                    return (x[0],prediction)

            prediction_rdd = test_rdd.map(lambda (f, x):(nbPredict(x,model)))

        elif model_type is "dt":
            labelled_test_rdd = test_rdd.map(lambda(tid, lbl_Tfi): labelPoint(tid, lbl_Tfi)).cache()
            prediction_rdd = model.predict(labelled_test_rdd.map(lambda x: x.features))
            truth_rdd = labelled_test_rdd.map(lambda x: x.label)
            prediction_rdd = prediction_rdd.zip(truth_rdd)

        test_prediction = prediction_rdd.collect()
        test_end_s_time = time()
        laps_time = test_end_s_time - test_start_s_time
        summariseResultsForCollectedHashOnOneLine(subject_name,test_prediction,hash_table_size,model_variable,laps_time,g_filehandle)



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

