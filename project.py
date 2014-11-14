from os import walk
from pprint import pprint
# from os.path import isfile, join
import re
# import numpy
from operator import add

# from datetime import datetime  #, time, timedelta
from time import time
from pyspark import SparkContext
from pyspark.conf import SparkConf
import inspect
import collections
import os
import sys
import traceback
from tempfile import NamedTemporaryFile
import tempfile

'''DEBUGGING'''

def exceptionTraceBack(exctype, value, tb):
    print 'Jonathan\'s Python / Spark Errors...'
    print 'Type:', exctype
    print 'Value:', value
    traceback.print_tb(tb, limit=20, file=sys.stdout)


'''LOGGING TRACEBACKS'''


def filePrint(string):
    global g_filehandle
    if g_filehandle:
        g_filehandle.write("{}\n".format(string))
    print(string)

def logfuncWithArgs():
    return logfunc("args", 'novals', sys._getframe().f_back)


def logfuncWithVals():
    return logfunc("noargs", 'vals', sys._getframe().f_back)


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


def logTimeIntervalWithMsg(msg):
    if 1:
        time_deltas = timeDeltas()
        message = msg if msg else ""
        string = "log:{0[time_since_start]:7,.3f} log:{0[time_since_last]:7,.3f} {1:}".format(time_deltas, message)
        filePrint(string)


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


def timeDeltas():
    global s_time
    global is_time
    ds_time = time()
    deltas_since_start = ds_time - s_time
    deltas_since_last = ds_time - is_time
    is_time = ds_time
    return {"time_since_start":deltas_since_start,'time_since_last':deltas_since_last}


''' INPUT '''

def validateInput():
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: spamPath <folder> (optional) stoplist<file>"
        exit(-1)

def parseArgs(args):
    print("args {}".format(args))
    parsedArgs = {}
    for idx,arg in enumerate(args):
        if idx > 0:
            parse = re.split('=',arg)
            if len(parse) == 2:
                (key, val) = parse
                parsedArgs[key]=val
            else:
                print("input error: coudn't parse: {}".format(parse))
    return parsedArgs


def stopList(stop_file):
    """
    :param stop_file: path to file of stopwords
    :return:python array of stopwords
    """
    stop_file_rdd = sc.textFile(stop_file)
    return stop_file_rdd.flatMap (lambda x: re.split('\W+',x)).collect()



'''TEXT PROCESSING UTILS'''

#'''This regex not used - it extracted ebook ID and body text following heading all in one go
#but is massively inefficient, presumably due to backtracking'''
# def idAndBodyTextRegex():
#     logfuncWithArgs()
#     regex = re.compile(
#         ".*"  #anything
#         "(\#[\d]+)"  #the EBOOK id
#         "\].*"  #anything
#         "(^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
#         "(.*)"  #anything
#         #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
#         ,flags=re.DOTALL|re.MULTILINE)
#     return regex

def headerRegex():
     '''
     regex to match header lines
     used in per-line processing
     :return: compiled regex
     '''
     regex = re.compile(
        "^(\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
        )
     return regex

def bodyTextRegex():
     '''
     regex to match body text following header line
     used in per-file processing
     :return: compiled regex
     '''
     regex = re.compile(
        "^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3}"  #end of header
        "(.*)"  #anything
        #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
        ,flags=re.DOTALL|re.MULTILINE)
     return regex

def idRegex():
    '''
    regex to match book ID
    :return: compiled regex
    '''
    logfuncWithArgs()
    regex = re.compile(
        "\[EBook (\#[\d]+)\]"  #the EBOOK id
        )
    return regex



def remPlural(word):
    '''
    crude depluralisation
    :param word: string
    :return: string with final s removed
    '''
    word = word.lower()
    return word[:-1] if word.endswith('s') else word


def searchWithRegex(txt,regex):
    '''
    used by rddOfTextFilesByLine(path, rxID, rxBodyText): in per-line processing
    :param txt: text to search
    :param regex: regex of thing to extract
    :return: regext match object
    '''
    result = "_"
    match = regex.search(txt)
    if match:
        result = match.group(1)
    return result



def extractIdAndBodyText(txt,rxID,rxBodyText):
    '''
    used by rddWithHeadersRemovedIndexedByID() in per-file processing
    :param txt: text to search (will be one file)
    :param rxID: regex to extract the ebook id
    :param rxBodyText: regex to extract all text following the header
    :return: tuple of (ebook id, text-with-header-removed)
    '''
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


''' MAIN FUNCTIONS '''

'''PART 1'''''
'''
    a) Start by traversing the text-part directory ,
    and loading all text files using loadTextFile(),
    which loads the text as lines.

    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.

    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    d) Extract the list of Word Frequency pairs per file (as an RDD) and...

'''

def wordCountPerFile(rdd):
    '''
    :param rdd: rdd of (file,word) tuples
    :return:rdd of (file, [(word, count),(word, count)...]) tuples
    '''
    logfuncWithArgs()
    logTimeIntervalWithMsg("##### BUILDING wordCountPerFile #####")
    wcf = rdd.map(lambda(x): ((x[0], x[1]), 1))

    logTimeIntervalWithMsg('##### GETTING THE  ((file,word),n)\
     WORDCOUNT PER (DOC, WORD) #####')
    result = wcf.reduceByKey(add)
    #print ("wcf: {}".format(result.take(1)))

    logTimeIntervalWithMsg('##### REARRANGE AS  (file, [(word, count)])  #####')
    result = result.map(lambda (a, b): (a[0], [(a[1], b)]))
    #print ("wordcount: {}".format(result.take(1)))
    logTimeIntervalWithMsg ('##### CONCATENATE (WORD,COUNT) LIST PER FILE \
           AS  (file, [(word, count),(word, count)...])  #####')
    result = result.reduceByKey(add)
    return result


'''PART 1 - PROCESSING PER LINE'''''

def rddOfTextFilesByLine(path, rxID, rxHeader):
    '''
    :param path: path to text files
    :param rxID: compiled regex to locat ebook ID
    :param rxHeader: compiled regex to locate header
    :return:
    '''
    rdd = sc.parallelize("")
    (location, folders, files) = walk(path).next()
    for filename in files:

        print '.',
        if filename != '.DS_Store':
            filepath = os.path.join(location, filename)
            #print filepath,
            textfile = sc.textFile(filepath).zipWithIndex()
            #print (textfile.take(1))
            id_rdd = textfile.map(lambda x: searchWithRegex(x[0],rxID)).filter(lambda(ebookid):ebookid is not "_")
            header_rdd = textfile.map(lambda x: (searchWithRegex(x[0], rxHeader), x[1])) \
                              .filter(lambda x: x[0] is not "_")


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
         rdd = rdd.union(rddOfTextFilesByLine(sub_path,rxID,rxHeader))

    return rdd



def processLineRDD(rdd, stop_list=[]):
    """
    :param rdd:  rdd of lines - ((text,lineNumber),bookID)
    :param stop_list: [list, of, stop, words]
    :return:wordCountPerFileRDD [(bookID,[(word,count)][(word,count)]...)]
    """
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


def one_a_to_d_by_line(textPath):
    '''
    part 1 (a-d) of the project
    processing the texts line-by-line
    :param textPath: path to texts we want to classify
    :return:rdd of (textID[(word,count),(word,count)...]
    '''
    rxID = idRegex()
    rxHeader = headerRegex()
    logTimeIntervalWithMsg("starting rddOfTextFilesByLine")
    lineRDD = rddOfTextFilesByLine(textPath,rxID,rxHeader)
    logTimeIntervalWithMsg("finished rddOfTextFilesByLine")
    #filePrint("lineRDD {}".format(lineRDD.takeSample(False,3,1)))
    logTimeIntervalWithMsg("starting processedByLineRDD")
    #remove duplicates
    processedByLineRDD = processLineRDD(lineRDD,stop_list)
    logTimeIntervalWithMsg("ended processedByLineRDD")

    #filePrint  ("processedByLineRDD {}".format(processedByLineRDD.take(1)),filehandle)
    filePrint ("found texts {}".format(processedByLineRDD.count()))
    return processedByLineRDD


'''PART 1 - PROCESSING PER FILE'''''

def idfFromWordCountsPerFile(rdd):
    '''
    :param rdd: array of (file,[(word,count),(word,count)...] )
    :return: idf: array of ( word,Nd[[(file,count),(file,count)...]] )
    '''
    number_of_docs = rdd.count()
    '''
    file, (word,count..)
    '''
    #new_rdd = rdd.map(lambda x: x[])

def rddOfWholeTextFileRDDs(path):
    '''
    :param path: path to text files
    :return: rdd of text files
    '''
    textFiles = sc.wholeTextFiles(path)
    (location, folders, files) = walk(path).next()
    for folder in folders:
        print '.',

        sub_path = os.path.join(location, folder)
        textFiles = textFiles.union(rddOfWholeTextFileRDDs(sub_path))

    return textFiles


def processRDD(rdd, stop_list=[]):
    """
    :param rdd:  rdd as read from filesystem ('filename','file_contents')
    :param stop_list: [list, of, stop, words]
    :return:wordCountPerFileRDD [(filename,[(word,count)][(word,count)]...)]
    """
    logfuncWithArgs()


    #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")


    flatmappedRDD = rdd.flatMap(lambda (iebook_id, words):
                                ([(iebook_id, remPlural(word))
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




def rddWithHeadersRemovedIndexedByID(rdd):
    '''
    called from one_a_to_d_by_file(textPath):
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.
    '''
    #print rdd.take(1)
    rxID = idRegex()
    rxBodyText = bodyTextRegex()
    rdd = rdd.map(lambda x:extractIdAndBodyText(x[1],rxID,rxBodyText)) \
              .filter(lambda (iebook_id,txt): iebook_id is not "_" and txt is not "_")
              #.flatmap(lambda (id, text): [(id, word) for word in re.split('\W+',text)])

    return rdd



def one_a_to_d_by_file(textPath):
    '''
    part 1 (a-d) of the project
    processing the texts per file
    :param textPath: path to texts we want to classify
    :return:rdd of (textID[(word,count),(word,count)...]
    '''

    logTimeIntervalWithMsg("starting rddOfWholeTextFileRDDs")

    bigRDD = rddOfWholeTextFileRDDs(textPath)

    if count_intermediates:
        filePrint ("bigRDD.count {}".format(bigRDD.count())) #515

    logTimeIntervalWithMsg("starting rddWithHeadersRemovedIndexedByID")


    bigRDD = rddWithHeadersRemovedIndexedByID(bigRDD)
    if count_intermediates:
         filePrint ("rddWithHeadersRemovedIndexedByID.count {}".format(bigRDD.count())) #395
    #remove duplicates
    bigRDD = bigRDD.reduceByKey(lambda x,y: x)
    #filePrint ("reduceByKey.count {}".format(bigRDD.count()),filehandle) #395
    logTimeIntervalWithMsg("starting processedByFileRDD")

    processedByFileRDD = processRDD(bigRDD,stop_list)
    logTimeIntervalWithMsg("ended processedByFileRDD")

    #filePrint ("processedByFileRDD {}".format(processedByFileRDD.take(1)),filehandle)
    filePrint ("found texts {}".format(processedByFileRDD.count())) #161
    return processedByFileRDD

def pickle(rdd):
    tmp = NamedTemporaryFile(delete=True)
    tmp.close()
    rdd.saveAsPickleFile(tmp.name,3)
    return tmp.name

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
    global g_filehandle
    g_filehandle = open('out.txt', 'a')


    sparkConf = SparkConf()
    sparkConf.setMaster("local[4]").setAppName("project")
    print("spark.app.name {}".format(sparkConf.get('spark.app.name')))
    print("spark.master {}".format(sparkConf.get('spark.master')))
    print("spark.executor.extraJavaOptions {}".format(sparkConf.get('executor.extraJavaOptions')))
    print("sparkConf.toDebugString() {}".format(sparkConf.toDebugString()))

    sc = SparkContext(conf=sparkConf)


    '''END OF INITIALISATION'''

    parsed_args = parseArgs(sys.argv)
    print (parsed_args)
    textPath = parsed_args['t'] if 't' in parsed_args else sys.argv[1]
    line_processing = parsed_args['l'] if 'l' in parsed_args else None
    stop_list = parsed_args['s'] if 's' in parsed_args else []
    count_intermediates = parsed_args['c'] if 'c' in parsed_args else None


    '''
    a) Start by traversing the text-part directory ,
    and loading all text files using loadTextFile(),
    which loads the text as lines.

    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.

    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    d) Extract the list of Word Frequency pairs per file (as an RDD) and...

    '''

    if line_processing:
        rdd = one_a_to_d_by_line(textPath)

    else:
        rdd = one_a_to_d_by_file(textPath)


    '''... save it to disk for later use.'''

    pickle_of_word_counts_per_file = NamedTemporaryFile(delete=True)
    pickle_of_word_counts_per_file.close()
    rdd.saveAsPickleFile(pickle_of_word_counts_per_file.name, 3)

    print("tmpFile{}".format(pickle_of_word_counts_per_file))

    pickleRDD = sc.pickleFile(pickle_of_word_counts_per_file.name, 3)
    print ("tmpFile reread {}".format(pickleRDD.take(1)))

    '''

    e) Calculate the IDF values and save the list of (word,IDF) pairs for later use.
    f) Calculate the TF.IDF values and create a 10000 dimensional vector per document using the hashing trick.
    '''




    end_s_time = time()
    runtime_s = end_s_time - start_s_time
    filePrint("\ntotal running time:{}".format(runtime_s))




    g_filehandle.close()
