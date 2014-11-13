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





def exceptionTraceBack(exctype, value, tb):
     print 'Jonathan\'s Python / Spark Errors...'
     print 'Type:', exctype
     print 'Value:', value
     traceback.print_tb(tb, limit=20, file=sys.stdout)


#"""LOGGING TRACEBACKS """

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

def logTimeIntervalWithMsg(msg, filehandle=None):
    if (1):
        time_deltas = timeDeltas()
        message = msg if msg else ""
        delta_since_start = time_deltas['time_since_start']
        delta_since_last = time_deltas['time_since_last']
        string = "log:{0[time_since_start]:7,.3f} log:{0[time_since_last]:7,.3f} {1:}".format(time_deltas, message)
        filePrint(string,filehandle)

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


def timeDeltas():
    global c_time
    global s_time
    global is_time
    ds_time = time()
    deltas_since_start = ds_time - s_time
    deltas_since_last = ds_time - is_time
    is_time = ds_time
    return {"time_since_start":deltas_since_start,'time_since_last':deltas_since_last}


#""" INPUT  """

def validateInput():
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: spamPath <folder> (optional) stoplist<file>"
        exit(-1)

def parseArgs(args):
    print("args {}".format(args))
    parsedArgs = {}
    for idx,arg in enumerate(args):
        if idx > 0:
            parse = re.split('\=',arg)
            if len(parse) == 2:
                (key, val) = parse
                parsedArgs[key]=val
            else:
                print("input error: coudn't parse: {}".format(parse))
    return parsedArgs


def stopList(stop_file):
    '''
    :param stop_file: path to file of stopwords
    :return:python array of stopwords
    '''
    rdd = sc.textFile(stop_file)
    return rdd.flatMap (lambda x: re.split('\W+',x)).collect()

def rddOfTextFilesByLine(path,rxID,rxBodyText):
    rdd = sc.parallelize("")
    (location, folders, files) = walk(path).next()
    for file in files:

        print '.',
        if file != '.DS_Store':
            filepath = os.path.join(location, file)
            #print filepath,
            textfile = sc.textFile(filepath).zipWithIndex()
            #print (textfile.take(1))
            id_rdd = textfile.map(lambda x:searchWithRegex(x[0],rxID)).filter(lambda(id):id is not "_")
            header_rdd = textfile.map(lambda x:(searchWithRegex(x[0],rxBodyText),x[1])) \
                              .filter(lambda x:x[0] is not "_")
            header_index_rdd = header_rdd.map(lambda x:x[1])

            # header_rdd1 = textfile.map(lambda x:(searchWithRegex(x[0],rxBodyText),x[1]))\
            #               .filter(lambda(id):id is not "_")

            #print ("id_rdd {}".format(id_rdd.take(1)))
            #print ("header_rdd {}".format(header_rdd.take(1)))
            #print ("header_index_rdd {}".format(header_index_rdd.take(1)))


            text_with_id = textfile.cartesian(id_rdd)
            text_with_header = textfile.cartesian(header_rdd)
            text_minus_header = text_with_header.filter(lambda x:x[0][1]>(x[1][1]+1)).map(lambda x:x[0])
            text_minus_header_with_id = text_minus_header.cartesian(id_rdd)
            #print (sample)
            #print ("text_with_id {}".format(text_with_id.take(1)))
            #print ("text_with_header {}".format(text_with_header.take(1)))
            #print ("text_minus_header {}".format(text_minus_header.take(2)))
            #print ("text_minus_header {}".format(text_minus_header_with_id.take(20)))

            rdd = rdd.union(text_minus_header_with_id) if rdd else text_minus_header_with_id

    for folder in folders:
         sub_path = os.path.join(location, folder)
         rdd = rdd.union(rddOfTextFilesByLine(sub_path,rxID,rxBodyText))

    return rdd





def rddOfWholeTextFileRDDs(path):
    '''
    '''

    textFiles = sc.wholeTextFiles(path)
    (location, folders, files) = walk(path).next()
    for folder in folders:
        print '.',

        sub_path = os.path.join(location, folder)
        textFiles = textFiles.union(rddOfWholeTextFileRDDs(sub_path))

    return textFiles

# def idAndBodyText1(file,txt,regex):
#     logfuncWithArgs()
#
#     result = ("_",("_","_"))
#     print ("\nfile: {}".format(file)),
#     searchResult = regex.search(txt)
#     if searchResult:
#         result = (searchResult.group(1), searchResult.group(3))
#         print (" {} ".format(searchResult.group(1)))
#         print("\nend")
#     else:
#        print("\nend")
#     return result

def searchWithRegex(txt,regex):
    result = "_"
    match = regex.search(txt)
    if match:
        result = match.group(1)
    return result



def extractIdAndBodyText(txt,rxID,rxBodyText):
    idTxt = "_"
    bodyTxt = "_"
    idMatch = rxID.search(txt)
    bodyMatch = rxBodyText.search(txt)
    if idMatch:
        idTxt = idMatch.group(1)
    if bodyMatch:
        bodyTxt = bodyMatch.group(1)
    result = (idTxt,bodyTxt)



    return result




def searchForID(file,txt,regex):
    result = (file,"_")
    searchResult = regex.search(txt)
    if searchResult:
        result[1] = searchResult.group(1)
    return result

def searchTextWithRegex(txt,regex):
    result = ("_")
    searchResult = regex.search(txt)
    if searchResult:
        result[1] = searchResult.group(1)
    return result

# def id(file,txt,regex):
#     logfuncWithArgs()
#
#     result = ("_",("_","_"))
#     print ("\nfile: {}".format(file)),
#     searchResult = regex.search(txt)
#     if searchResult:
#         result = (searchResult.group(1))
#         print (" {} ".format(searchResult.group(1)))
#         print("\nend")
#     else:
#        print("\nend")
#     return result

def rddOfIDs(rdd):
    regx = idRegex()
    rdd = rdd.map(lambda x:id(x[0],x[1],regx))
    return rdd




def rddWithHeadersRemovedIndexedByID(rdd):
    '''

    '''

    '''
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    '''
    #print rdd.take(1)
    rxID = idRegex()
    rxBodyText = bodyTextRegex()
    rdd = rdd.map(lambda x:extractIdAndBodyText(x[1],rxID,rxBodyText)) \
              .filter(lambda (id,txt): id is not "_" and txt is not "_")

              #.flatmap(lambda (id, text): [(id, word) for word in re.split('\W+',text)])

    '''
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    '''



    return rdd

def idAndBodyTextRegex():
    logfuncWithArgs()
    regex = re.compile(
        ".*"  #anything
        "(\#[\d]+)"  #the EBOOK id
        "\].*"  #anything
        "(^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
        "(.*)"  #anything
        #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
        ,flags=re.DOTALL|re.MULTILINE)
    return regex

def headerRegex():
     regex = re.compile(
        "^(\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
        )
     return regex

def bodyTextRegex():
     regex = re.compile(
        "^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3}"  #end of header
        "(.*)"  #anything
        #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
        ,flags=re.DOTALL|re.MULTILINE)
     return regex

def idRegex():
    logfuncWithArgs()
    regex = re.compile(
        "\[EBook (\#[\d]+)\]"  #the EBOOK id
        )
    return regex

def filePrint(string,filehandle=None):
    if filehandle:
        filehandle.write("{}\n".format(string))
    print(string)


def remPlural(word):
    word = word.lower()
    return word[:-1] if word.endswith('s') else word

def processLineRDD(rdd, stop_list=[]):
    '''
    :param rdd:  rdd of lines - ((text,lineNumber),bookID)
    :param stop_list: [list, of, stop, words]
    :return:wordCountPerFileRDD [(bookID,[(word,count)][(word,count)]...)]
    '''
    logfuncWithArgs()


    #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")


    flatmappedRDD = rdd.flatMap(lambda (words, bookID):
                                ([(bookID, remPlural(word))
                                 for word in re.split('\W+', words[0])
                                 if len(word) > 0
                                 and word not in stop_list
                                 and remPlural(word) not in stop_list]))

    #print ("flatmappedRDD {}".format(flatmappedRDD.take(1)))
    # logTimeIntervalWithMsg("flatmappedRDD {}".format(flatmappedRDD.take(1)))
    wordCountPerFileRDD = wordCountPerFile(flatmappedRDD)
    #print ("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))

    # logTimeIntervalWithMsg("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))
    return wordCountPerFileRDD


def processRDD(rdd, stop_list=[]):
    '''
    :param rdd:  rdd as read from filesystem ('filename','file_contents')
    :param stop_list: [list, of, stop, words]
    :return:wordCountPerFileRDD [(filename,[(word,count)][(word,count)]...)]
    '''
    logfuncWithArgs()


    #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")


    flatmappedRDD = rdd.flatMap(lambda (id, words):
                                ([(id, remPlural(word))
                                 for word in re.split('\W+', words)
                                 if len(word) > 0
                                 and word not in stop_list
                                 and remPlural(word) not in stop_list]))

    #print ("flatmappedRDD {}".format(flatmappedRDD.take(1)))
    # logTimeIntervalWithMsg("flatmappedRDD {}".format(flatmappedRDD.take(1)))
    wordCountPerFileRDD = wordCountPerFile(flatmappedRDD)
    #print ("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))

    # logTimeIntervalWithMsg("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))
    return wordCountPerFileRDD

def wordCountPerFile(rdd):
    logfuncWithArgs()
    # input: rdd of (file,word) tuples
    # return: rdd of (file, [(word, count),(word, count)...]) tuples
    logTimeIntervalWithMsg("##### BUILDING wordCountPerFile #####")
    wcf = rdd.map(lambda(x): ((x[0], x[1]), 1))

    print('##### GETTING THE  ((file,word),n)\
     WORDCOUNT PER (DOC, WORD) #####')
    result = wcf.reduceByKey(add)
    #print ("wcf: {}".format(result.take(1)))

    print('##### REARRANGE AS  (file, [(word, count)])  #####')
    result = result.map(lambda (a, b): (a[0], [(a[1], b)]))
    #print ("wordcount: {}".format(result.take(1)))
    print ('##### CONCATENATE (WORD,COUNT) LIST PER FILE \
           AS  (file, [(word, count),(word, count)...])  #####')
    result = result.reduceByKey(add)
    return result

' THE MAIN LOOP '

if __name__ == "__main__":

    '''INITIALISATION'''
    sys.excepthook = exceptionTraceBack

    global s_time
    global is_time
    global g_start_time

    s_time = time()
    start_s_time = s_time
    is_time = s_time
    g_start_time = time()

    global found
    found = 0
    global not_found
    not_found = 0

    validateInput()
    filehandle = open('out.txt', 'a')

    sc = SparkContext(appName="project")
   # stop_list = stopList(sys.argv[2]) if len(sys.argv)>2 else []
  #  line_processing = stopList(sys.argv[3]) if len(sys.argv)>3 else None


    '''END OF INITIALISATION'''

    parsed_args = parseArgs(sys.argv)
    print (parsed_args)
    textPath = parsed_args['t'] if 't' in parsed_args else sys.argv[1]
    line_processing = parsed_args['l'] if 'l' in parsed_args else None
    stop_list = parsed_args['s'] if 's' in parsed_args else []





    '''a) Start by traversing the text-part directory ,
    and loading all text files using loadTextFile(),
    which loads the text as lines.
    '''
    if (line_processing):
        rxID = idRegex()
        rxBodyText = headerRegex()
        logTimeIntervalWithMsg("starting rddOfTextFilesByLine",filehandle)
        lineRDD = rddOfTextFilesByLine(textPath,rxID,rxBodyText)
        logTimeIntervalWithMsg("finished rddOfTextFilesByLine",filehandle)
        #filePrint("lineRDD {}".format(lineRDD.takeSample(False,3,1)))
        logTimeIntervalWithMsg("starting processedByLineRDD",filehandle)
        #remove duplicates
        processedByLineRDD = processLineRDD(lineRDD,stop_list)
        logTimeIntervalWithMsg("ended processedByLineRDD",filehandle)

        #filePrint  ("processedByLineRDD {}".format(processedByLineRDD.take(1)),filehandle)
        filePrint ("found texts {}".format(processedByLineRDD.count()),filehandle)

    else:
        logTimeIntervalWithMsg("starting rddOfWholeTextFileRDDs",filehandle)

        bigRDD = rddOfWholeTextFileRDDs(textPath)

        filePrint ("bigRDD.count {}".format(bigRDD.count()),filehandle) #515

        logTimeIntervalWithMsg("starting rddWithHeadersRemovedIndexedByID",filehandle)


        bigRDD = rddWithHeadersRemovedIndexedByID(bigRDD)
        filePrint ("rddWithHeadersRemovedIndexedByID.count {}".format(bigRDD.count()),filehandle) #395
        #remove duplicates
        bigRDD = bigRDD.reduceByKey(lambda x,y: x)
        filePrint ("reduceByKey.count {}".format(bigRDD.count()),filehandle) #395
        logTimeIntervalWithMsg("starting processedByFileRDD",filehandle)

        processedByFileRDD = processRDD(bigRDD,stop_list)
        logTimeIntervalWithMsg("ended processedByFileRDD",filehandle)

        #filePrint ("processedByFileRDD {}".format(processedByFileRDD.take(1)),filehandle)
        filePrint ("found texts {}".format(processedByFileRDD.count()),filehandle) #161



    #filePrint ("read texts {}".format(bigRDD.count()),filehandle)

    '''
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    '''


    '''
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    '''
    #idRDD = rddOfIDs(bigRDD)


    #fileWord = bigRDD.flatMap(lambda (id,text): [(id, word) for word in re.split('\W+',text)])

    #bigRDD.map(lambda (f,x): (x[0],x[1])).flat


   # filePrint ("sample: {}".format(processedRDD.take(1)),filehandle)





    end_s_time = time()
    runtime_s = end_s_time - start_s_time
    filePrint("\ntotal running time:{}".format(runtime_s),filehandle)


    '''

    1 Reading and preparing text files (25%)
    c)You need to extract the ID of the text, which occurs in the header in the format [EBook #<ID>], where ID is a natural number.
     rdd format (id, text)


    d) Extract the list of Word Frequency pairs per file (as an RDD) and save it to disk for later use.

    (id, text)
    => (id, word) (id, word)
    => (id, (word, 1), (word, 1), (word, 1)...)
    (id ((word, feq),(word,freq...))

    e) Calculate the IDF values and save the list of (word,IDF) pairs for later use.
    f) Calculate the TF.IDF values and create a 10000 dimensional vector per document using the hashing trick.
    '''


    filehandle.close()
