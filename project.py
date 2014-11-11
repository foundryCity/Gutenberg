import sys
from os import walk
from pprint import pprint
# from os.path import isfile, join
import re
# import numpy
from operator import add
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils

# from datetime import datetime  #, time, timedelta
from time import time
from pyspark import SparkContext
import inspect
import collections
import os
import sys,traceback




"""EXCEPTIONS"""
def exceptionTraceBack(exctype, value, tb):
     print 'Jonathan\'s Python / Spark Errors...'
     print 'Type:', exctype
     print 'Value:', value
     traceback.print_tb(tb, limit=20, file=sys.stdout)


"""LOGGING TRACEBACKS """

def logfuncWithArgs(start_time=None):
    stime = start_time if start_time else ""
    return logfunc(stime,"args",'novals',sys._getframe().f_back)

def logfuncWithVals(start_time=None):
    stime = start_time if start_time else ""
    return logfunc(stime,"noargs",'vals',sys._getframe().f_back)

def logexep(frame):
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


def logfunc(start_time=None, args=None, vals=None, frame=None):
    fargs={}
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



""" INPUT  """

def validateInput():
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: spamPath <folder> (optional) stoplist<file>"
        exit(-1)


def stopList(stop_file):
    '''
    :param stop_file: path to file of stopwords
    :return:python array of stopwords
    '''
    rdd = sc.textFile(stop_file)
    return rdd.flatMap (lambda x: re.split('\W+',x)).collect()


def rddOfWholeTextFileRDDs(path):
    '''
    '''

    textFiles = sc.wholeTextFiles(path)
    (location, folders, files) = walk(path).next()
    for folder in folders:
        sub_path = os.path.join(location, folder)
        textFiles = textFiles.union(rddOfWholeTextFileRDDs(sub_path))

    return textFiles

def idAndBodyText(txt,regex):
    result = None
    print ".",
    searchResult = regex.search(txt)
    if searchResult:
        result = (searchResult.group(1), searchResult.group(3))

    return result

def rddWithHeadersRemovedIndexedByID(rdd):
    '''

    '''

    '''
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    '''
    #print rdd.take(1)
    regx = regex()
    rdd = rdd.map(lambda x:idAndBodyText(x[1],regx))

    '''
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    '''



    return rdd

def regex():
    regex = re.compile(
        ".*"  #anything
        "(\#[\d]+)"  #the EBOOK id
        "\].*"  #anything
        #"(^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
        "(^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header

        "(.*)"  #anything
        #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
        ,flags=re.DOTALL|re.MULTILINE)
    return regex

' THE MAIN LOOP '

if __name__ == "__main__":
    sys.excepthook = exceptionTraceBack
    global s_time


    s_time = time()
    start_s_time = s_time
    validateInput()

    sc = SparkContext(appName="project")
    #logTimeIntervalWithMsg("spark initialised, resetting timers")
    #s_time = time()

    filehandle = open('out.txt', 'a')
    textPath = sys.argv[1]
    #textFiles = dictOfFileRDDs(textPath)
    '''a) Start by traversing the text-part directory ,
    and loading all text files using loadTextFile(),
    which loads the text as lines.
    '''
    bigRDD = rddOfWholeTextFileRDDs(textPath)
    print ("read texts {}".format(len(bigRDD.collect())))

    '''
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    '''


    '''
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    '''
    bigRDD = rddWithHeadersRemovedIndexedByID(bigRDD)
    print ("found texts {}".format(len(bigRDD.collect())))




    stop_list = stopList(sys.argv[2]) if len(sys.argv)>2 else []

    end_s_time = time()
    runtime_s = end_s_time - start_s_time
    print("\ntotal running time:{}".format(runtime_s))


    '''

    1 Reading and preparing text files (25%)
    c)You need to extract the ID of the text, which occurs in the header in the format [EBook #<ID>], where ID is a natural number.
    d) Extract the list of Word Frequency pairs per file (as an RDD) and save it to disk for later use.
    e) Calculate the IDF values and save the list of (word,IDF) pairs for later use.
    f) Calculate the TF.IDF values and create a 10000 dimensional vector per document using the hashing trick.
    '''


    filehandle.close()
