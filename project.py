from os import walk
from pprint import pprint
# from os.path import isfile, join
import re
# import numpy
from operator import add

# from datetime import datetime  #, time, timedelta
from time import time
from time import localtime
from pyspark import SparkContext
from pyspark.conf import SparkConf
import inspect
import os
import sys
import traceback
from tempfile import NamedTemporaryFile
import math

from unidecode import unidecode


'''DEBUGGING'''

def exceptionTraceBack(exctype, value, tb):
    print 'Jonathan\'s Python / Spark Errors...'
    print 'Type:', exctype
    print 'Value:', value
    traceback.print_tb(tb, limit=20, file=sys.stdout)


'''LOGGING TRACEBACKS'''
def timestring():
    cltime = localtime(time())
    return "{}:{}:{}".format(cltime.tm_hour,cltime.tm_min,cltime.tm_sec)
    #return "{}".format(cltime)

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
        string = "total time:{0[time_since_start]:7,.3f} time since:{0[time_since_last]:7,.3f}  {1:}".format(time_deltas, message)
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
    if len(sys.argv) < 2:
        printHelp()
        print >> sys.stderr, "Usage: spamPath <folder> (optional) stoplist<file>"
        exit(-1)

def printHelp():
    print ('''
        =Usage=

        Required:
        t=<path to text files from current directory>
        s=<path to stop file from current directory>

        Optional:
        l=1 # process by line
        c=1 # count intermediates (logging)
        n=1 # normalise word frequencies
        e=1 # english texts only

    ''')
    exit(-1)




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


def stopList(stop_file_path):
    """
    :param stop_file_path: path to file of stopwords
    :return:python array of stopwords
    """
    result = []
    if stop_file_path:
        stop_file_rdd = sc.textFile(stop_file_path)
        result = stop_file_rdd.flatMap (lambda x: re.split('\W+',x)).collect()
    return result

def numberOfInputFiles(path):
    count = 0
    (location, folders, files) = walk(path).next()
    count += len(files)
    for folder in folders:
        sub_path = os.path.join(location, folder)
        count += numberOfInputFiles(sub_path)
    return count

'''OUTPUT'''

def pickled(rdd):
    tmp = NamedTemporaryFile(delete=True)
    tmp.close()
    rdd.saveAsPickleFile(tmp.name,3)
    return tmp.name

def unpickled(sc,pickle_name):
    rdd = sc.pickleFile(pickle_name, 3)
    return rdd

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

def decode(decode_unicode,text):
    return unidecode(text) if decode_unicode else text
    #return text


def searchTextWithRegexes(txt,regex_array):
    """
    :param txt: text to search
    :param regex_array: regular expressions to search with
    :return:
    """
    if not txt:
        return None
    hits = []
    for regex in regex_array:
        hit = regex.search(txt)
        if hit:
            hits.append(hit)
    return hits if len(hits)> 0 else None

def regexFilters():
    """
    :return:dictionary of inclusion and exclusion regular expression filters
    """
    genome_regex = genomeRegex()
    ascii_regex = asciiRegex()
    english_regex=englishRegex()
    regex_filters = {}
    #exclude files matching any of these regexes
    regex_filters['exclusions'] = [genome_regex]
    #exclude files NOT matching any of these regexes
    regex_filters['inclusions'] = [ascii_regex,english_regex]
    return regex_filters


def englishRegex():
    """
    regex to match English texts
    uses 'Language:' attribution in header
    :return:
    """
    logfuncWithArgs()

    regex = re.compile(
        ur'^(Language: English)'
        ,flags=re.MULTILINE|re.UNICODE)
    return regex


def germanRegex():
    """
    regex to match German texts
    uses 'Language:' attribution in header
    :return:
    """
    logfuncWithArgs()

    regex = re.compile(
        ur'^(Language: German)'
        ,flags=re.MULTILINE|re.UNICODE)
    return regex

def frenchRegex():
    """
    regex to match French texts
    uses 'Language:' attribution in header
    :return:
    """
    logfuncWithArgs()

    regex = re.compile(
        ur"^(Language: French)"
        ,flags=re.MULTILINE|re.UNICODE)
    return regex

def esperantoRegex():
    """
    regex to match Esperanto texts
    uses 'Language:' attribution in header
    :return:
    """
    logfuncWithArgs()

    regex = re.compile(
        ur"^(Language: Esperanto)"
        ,flags=re.MULTILINE|re.UNICODE)
    return regex

def asciiRegex():
    """
    regex to match English texts
    uses 'Language:' attribution in header
    :return:
    """
    logfuncWithArgs()
    regex = re.compile(
        ur"^(Character set encoding: ASCII)"
        ,flags=re.MULTILINE|re.UNICODE)
    return regex

def genomeRegex():
    regex = re.compile(
        ur"^(Title: Human Genome Project)"
        ,flags=re.MULTILINE|re.UNICODE)
    return regex


def headerRegex():
     """
     regex to match header lines
     used in per-line processing
     :return: compiled regex
     """
     logfuncWithArgs()

     regex = re.compile(
        ur"^(\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
        ,flags=re.MULTILINE|re.UNICODE)
     return regex

def bodyTextRegex():
     """
     regex to match body text following header line
     used in per-file processing
     :return: compiled regex
     """
     logfuncWithArgs()

     regex = re.compile(
        ur"^\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3}"  #end of header
        ur"(.*)"  #anything
        #"(^\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG){0,1}"  #start of footer
        ,flags=re.DOTALL|re.MULTILINE|re.UNICODE)
     return regex

def idRegex():
    """
    regex to match book ID
    :return: compiled regex
    """
    logfuncWithArgs()
    regex = re.compile(
        ur"\[E(?:Book|text) (\#[\d]+)\]"  #the EBOOK id
        #"\[EBook (\#[\d]+)\]"  #the EBOOK id
        )
    return regex



def remPlural(word):
    """
    crude depluralisation
    :param word: string
    :return: string with final s removed
    """
    word = word.lower()
    return word[:-1] if word.endswith('s') else word


def searchWithRegex(txt,regex):
    """
    used by rddOfTextFilesByLine(path, rxID, rxBodyText): in per-line processing
    :param txt: text to search
    :param regex: regex of thing to extract
    :return: regext match object
    """
    result = "_"
    match = regex.search(txt)
    if match:
        result = match.group(1)
    return result

def extractIdAndBodyTextWithFilters(txt,rx_id,rx_body_text,regex_filters=None):
    """
    used by rddWithHeadersRemovedIndexedByID() in per-file processing
    :param txt: text to search (will be one file)
    :param rx_id: regex to extract the ebook id
    :param rx_body_text: regex to extract all text following the header
    :param rx_english: regex to flag English texts from header language info
    :param rx_ascii: regex to flag ASCII-encoded texts from header language info

    :return: tuple of (ebook id, text-with-header-removed)
    """
    id_text = "_"
    body_text = "_"
    id_match = rx_id.search(txt)
    body_match = rx_body_text.search(txt)

    #if we are filtering for language, we want to accept all ENGLISH texts and all ASCII-encoded texts
    # - some ASCII texts are flagged as language: (English and...), we want to keep these in
    # - english texts encoded as unicode will read correctly as unicode is a superset of ASCII
    #excluded = searchTextWithRegexes(txt,exclusions) if exclusions else None
    #if not excluded:
    included = 1
    #print("regex_filters {}".format(regex_filters))
    if regex_filters:
        included = 0
        excluded = searchTextWithRegexes(txt,regex_filters['exclusions']) if regex_filters['exclusions'] else 0
        if not excluded:
            included = searchTextWithRegexes(txt,regex_filters['inclusions']) if regex_filters['inclusions'] else 1

    if included:
        if id_match:
            id_text = id_match.group(1)
        if body_match:
            body_text = body_match.group(1)
    result = (id_text,body_text)
    return result



def extractIdAndBodyTextEnglish(txt,rx_id,rx_body_text,regex_filters=None):
    """
    used by rddWithHeadersRemovedIndexedByID() in per-file processing
    :param txt: text to search (will be one file)
    :param rx_id: regex to extract the ebook id
    :param rx_body_text: regex to extract all text following the header
    :param rx_english: regex to flag English texts from header language info
    :param rx_ascii: regex to flag ASCII-encoded texts from header language info

    :return: tuple of (ebook id, text-with-header-removed)
    """
    id_text = "_"
    body_text = "_"
    id_match = rx_id.search(txt)
    body_match = rx_body_text.search(txt)

    #if we are filtering for language, we want to accept all ENGLISH texts and all ASCII-encoded texts
    # - some ASCII texts are flagged as language: (English and...), we want to keep these in
    # - english texts encoded as unicode will read correctly as unicode is a superset of ASCII
    #excluded = searchTextWithRegexes(txt,exclusions) if exclusions else None
    #if not excluded:

    regex = englishRegex()
    regex1 = re.compile("^(Language: English)",re.MULTILINE)
    included = regex1.search(txt)
    print ("text {}".format(txt))

    print ("included {}".format(included))

    if included:
        if id_match:
            id_text = id_match.group(1)
        if body_match:
            body_text = body_match.group(1)
    result = (id_text,body_text)
    return result


def extractIdAndBodyText(txt,rxID,rxBodyText):
    """
    used by rddWithHeadersRemovedIndexedByID() in per-file processing
    :param txt: text to search (will be one file)
    :param rxID: regex to extract the ebook id
    :param rxBodyText: regex to extract all text following the header
    :return: tuple of (ebook id, text-with-header-removed)
    """
    id_text = "_"
    body_text = "_"
    id_match = rxID.search(txt)
    body_match = rxBodyText.search(txt)
    if id_match:
        id_text = id_match.group(1)
    if body_match:
        body_text = body_match.group(1)
    result = (id_text,body_text)

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

def normaliseWordFrequencies(rdd,smoothing=0.4):
    """
    used by wordCountperFile if we want to normalise our term frequencies
    see sec. 6.4.2, maximum tf normalization in 'An Introduction to Information Retrieval'
    (Manning, Raghavan and Schutze, CUP 2009)
    :param rdd of (file, [(word, count),(word, count)...]) tuples
    :return:rdd of (file, [(word, count),(word, count)...]) tuples where count is normalised
    """



    # now get the frequency of the most-frequent term (the maximal term frequency)
    # so that we can normalise our term frequencies per document

    max_term_freq = rdd.map (lambda x: (
                               (x[0], max(x[1], key = lambda y: y[1])[1])
                               ,x[1] )
                              )
    #print ("\n\nmaxTermFreq: {}".format(max_term_freq.take(1)))

    #zip it up wit the result

    result = max_term_freq.zip(rdd).map(lambda x:x[0])

    #nomalise each term frequency by division with maxFreq
    #print ("\n\nzipped: {}\n\n".format(result.take(4)))
    result = result.map (lambda x : (x[0][0],
                                      [(wf[0],
                                        smoothing+(1-smoothing)*wf[1]/float(x[0][1])
                                       )
                                        for wf in x[1]]
                                     )
                         )
    #print ("\n\nresult: {}\n\n".format(result.take(1)))

    return result


def wordCountPerFile(rdd):
    """
    :param rdd: rdd of (file,word) tuples
    :return:rdd of (file, [(word, count),(word, count)...]) tuples
    """
    #logfuncWithArgs()
    logTimeIntervalWithMsg ('starting wordCountPerFile')
    #logTimeIntervalWithMsg("##### BUILDING wordCountPerFile #####")
    wcf = rdd.map(lambda(x): ((x[0], x[1]), 1))

    #logTimeIntervalWithMsg('##### GETTING THE  ((file,word),n)\
    # WORDCOUNT PER (DOC, WORD) #####')
    result = wcf.reduceByKey(add)
    #print ("wcf: {}".format(result.take(1)))

    #logTimeIntervalWithMsg('##### REARRANGE AS  (file, [(word, count)])  #####')
    result = result.map(lambda (a, b): (a[0], [(a[1], b)]))
    #print ("wordcount: {}".format(result.take(1)))
    #logTimeIntervalWithMsg ('##### CONCATENATE (WORD,COUNT) LIST PER FILE \
    #       AS  (file, [(word, count),(word, count)...])  #####')
    result = result.reduceByKey(add)
    logTimeIntervalWithMsg ('finished wordCountPerFile')

    #print ("\n\nwordcount: {}".format(result.take(1)))


    return result

''' PROCESSING PER FILE'''''


def rddOfWholeTextFileRDDs(path):
    """
    read in textFiles using sc.wholeTextFiles
    this is faster than using sc.textFile() so we use this version
    unless we want to filter for file sizes
    :param path: path to text files
    :return: rdd of text files
    """
    rdd = sc.wholeTextFiles(path)
    (location, folders, files) = walk(path).next()
    for folder in folders:
        print '.',

        sub_path = os.path.join(location, folder)
        rdd = rdd.union(rddOfWholeTextFileRDDs(sub_path))
    return rdd

''' PROCESSING PER LINE'''''

def rddOfTextFilesByLine(path, rx_id, rx_header,regex_filters,max_file_size):
    """
    read in textFiles using sc.textFile()
    filter for max_file_size and regex filters as we read in the files
    this is slower than using sc.wholeTextFiles() in rddOfWholeTextFileRDDs()
    and we only use it if we wish to filter for file sizes
    :param path: path to text files
    :param rx_id: compiled regex to locat ebook ID
    :param rx_header: compiled regex to locate header
    :return:
    """
    rdd = sc.parallelize("")
    (location, folders, files) = walk(path).next()
    for filename in files:
        print '.',
        filepath = os.path.join(location, filename)
        filesize = os.path.getsize(filepath)
        if filename != '.DS_Store' and filesize < max_file_size:
            textfile_rdd = sc.textFile(filepath).zipWithIndex()
            id_rdd = textfile_rdd.map(lambda x: searchWithRegex(x[0],rx_id)).filter(lambda(ebookid):ebookid is not "_")
            header_rdd = textfile_rdd.map(lambda x: (searchWithRegex(x[0], rx_header), x[1])) \
                              .filter(lambda x: x[0] is not "_")
            text_with_header = textfile_rdd.cartesian(header_rdd)
            text_minus_header = text_with_header.filter(lambda x:x[0][1]>(x[1][1]+1)).map(lambda x:x[0][0])
            header_minus_text = text_with_header.filter(lambda x:x[0][1]<(x[1][1]+1)).map(lambda x:x[0])
            if regex_filters:
                excluded = None
                for exclusion_regex in regex_filters['exclusions']:
                    exclusion_rdd = header_minus_text.map(lambda x: searchWithRegex(x[0],exclusion_regex))\
                                                  .filter(lambda(result):result is not "_")
                    if exclusion_rdd.count()>0:
                        excluded = 1
                        break
                if excluded:
                    continue

                included = None
                for inclusion_regex in regex_filters['inclusions']:
                    inclusion_rdd = header_minus_text.map(lambda x: searchWithRegex(x[0],inclusion_regex))\
                                                  .filter(lambda(result):result is not "_")
                    if inclusion_rdd.count() > 0:
                        included = 1
                        break
                if not included:
                    continue

            text_minus_header_with_id = text_minus_header.cartesian(id_rdd).map(lambda x:(x[1],x[0]))
            text_combinded= text_minus_header_with_id.reduceByKey(lambda x,y: x+" "+y)
            rdd = rdd.union(text_combinded) if rdd else text_combinded

    for folder in folders:
         sub_path = os.path.join(location, folder)
         rdd = rdd.union(rddOfTextFilesByLine(sub_path,rx_id,rx_header,regex_filters,max_file_size))

    return rdd



def idfFromWordCountsPerFile(rdd):
    """
    :param rdd: array of (file,[(word,count),(word,count)...] )
    :return: idf: array of [( word,idf)] ) where idf = log (N/doc-frequency)
    """
    #print("pre: {}".format(rdd.collect()))
    number_of_documents = rdd.count()
    rdd = rdd.map (lambda (a,b) : (a,[(tuple[0],(a,tuple[1])) for tuple in b]))

    #print("\nintermediate_rdd: {}".format(rdd.collect()))
    rdd = rdd.flatMap(lambda a:a[1])
    #print("\nflatMap: {}".format(rdd.collect()))
    rdd = rdd.map(lambda x:(x[0],1))
    #print("\nmap: {}".format(rdd.collect()))

    rdd = rdd.reduceByKey(add)
    #print("\nreduce: {}".format(rdd.collect()))

    rdd = rdd.map(lambda x:(x[0],math.log(number_of_documents/float(x[1]),2)))
    #print("\nidf: {}".format(rdd.collect()))
    return rdd







def processRDD(rdd, stop_list_path,decode_unicode):
    """
    :param rdd:  rdd as read from filesystem ('filename','file_contents')
    :param stop_list_path: [list, of, stop, words]
    :return:wordCountPerFileRDD [(filename,[(word,count)][(word,count)]...)]
    """
    logfuncWithArgs()

    #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")
    stop_list = stopList(stop_list_path)
    wordsplit = re.compile('\W+',re.UNICODE)


    flatmapped_rdd = rdd.flatMap(lambda (iebook_id, words):
                                ([(iebook_id, remPlural(word))
                                 for word in re.split(wordsplit, decode(decode_unicode,words))
                                 if len(word) > 0
                                 and word not in stop_list
                                 and remPlural(word) not in stop_list]))

    #print ("flatmappedRDD {}".format(flatmappedRDD.take(1)))
    # logTimeIntervalWithMsg("flatmappedRDD {}".format(flatmappedRDD.take(1)))

    wordcount_per_file_rdd = wordCountPerFile(flatmapped_rdd)
    #print ("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))

    # logTimeIntervalWithMsg("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))
    return wordcount_per_file_rdd




def rddWithHeadersRemovedIndexedByID(rdd, rx_id, rx_body_text, regex_filters=None):
    """
    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.
    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.
    """
    rdd = rdd.map(lambda x:extractIdAndBodyTextWithFilters(x[1],rx_id,rx_body_text, regex_filters)) \
              .filter(lambda (iebook_id,txt): iebook_id is not "_" and txt is not "_")
    return rdd



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
    logfile = 'out.txt'
    logfile = os.path.abspath(logfile)
    g_filehandle = open(logfile, 'a')


    sparkConf = SparkConf()
    sparkConf.setMaster("local[4]").setAppName("project")
    #sparkConf.set("spark.local.dir","/Volumes/jScratch/_new_scratch/tmp")
    sparkConf.set("spark.logConf","true")
    sparkConf.set("spark.executor.memory","6g")

    sc = SparkContext(conf=sparkConf)

    '''END OF INITIALISATION'''

    parsed_args = parseArgs(sys.argv)
    if len(parsed_args) < 1:
        printHelp()
        exit(-1)

    filePrint("\n\nstarted run at {}".format(timestring()))
    logTimeIntervalWithMsg ("{}".format(parsed_args))
    logTimeIntervalWithMsg("sparkConf.toDebugString() {}".format(sparkConf.toDebugString()))

    text_path = parsed_args['t'] if 't' in parsed_args else sys.argv[1]
    stop_list_path = parsed_args['s'] if 's' in parsed_args else []
    normalise_word_frequencies = parsed_args['n'] if 'n' in parsed_args else None
    filter_files = parsed_args['f'] if 'f' in parsed_args else None
    max_file_size = int(parsed_args['max']) if 'max'in parsed_args else None
    decode_unicode = parsed_args['d'] if 'd' in parsed_args else None

    '''
     example usages
     spark-submit  --driver-memory 8G project.py t=data/text_party s=stopwords_en.txt f=1
     spark-submit project.py t=data/text_party s=stopwords_en.txt n=1 f=1 max=100000

    '''


    '''
    a) Start by traversing the text-part directory ,
    and loading all text files using loadTextFile(),
    which loads the text as lines.

    b)From the text files you need to remove the header.
    The last line of the header starts and ends with ***.

    c)You need to extract the ID of the text,
    which occurs in the header in the format [EBook #<ID>],
    where ID is a natural number.

    d) Extract the list of Word Frequency pairs per file (as an RDD) and
    save it to disk for later use.

    '''

    number_of_input_files = numberOfInputFiles(text_path)
    logTimeIntervalWithMsg ("input files: {} ".format(number_of_input_files)) #161

    if filter_files:
        logTimeIntervalWithMsg("filtering files...")
        regex_filters = regexFilters()
    else:
        logTimeIntervalWithMsg("not filtering files...")
        regex_filters=None

    rx_id = idRegex()



    if max_file_size:
        # line-by-line processing: this is slower but allows us to filter for maximum file sizes
        rx_header = headerRegex()
        rdd = rddOfTextFilesByLine(text_path,rx_id,rx_header,regex_filters,max_file_size)

    else:
        # file-by-file processing- faster but cannot filter for maximum file sizes
        rx_body_text = bodyTextRegex()
        rdd = rddOfWholeTextFileRDDs(text_path)
        print("\ninput_rdd: {}".format(rdd.collect()))

        rdd = rddWithHeadersRemovedIndexedByID(rdd, rx_id, rx_body_text, regex_filters)


    logTimeIntervalWithMsg ("about to start processRDD") #161

    # remove duplicates is implemented with this reduceByKey method.
    # if we encounter more than one text with
    # the same bookID we will only use the first. These texts are identical but
    # use different encodings (ASCII, utf8 etc).
    # a better implementation would select in order of preference: utf8,other_encoding, ascii
    # and ensure that other_encoding was transcoded to utf8.
    rdd = rdd.reduceByKey(lambda x,y: x)

    rdd = processRDD(rdd,stop_list_path,decode_unicode) #.persist(sc.StorageLevel.MEMORY_AND_DISK)
    word_count_per_file_pickle = pickled(rdd)
    logTimeIntervalWithMsg ("finished processRDD") #161
    print("word_count_per_file: {}".format(rdd.take(5)))

    #print("word_frequency_per_file_rdd: {}".format(word_frequency_per_file_rdd.take(1)))

    if normalise_word_frequencies:
        rdd = normaliseWordFrequencies(rdd)

    logTimeIntervalWithMsg ("finished normalising") #161

    logTimeIntervalWithMsg ("input files: {} found texts: {}".format(number_of_input_files,rdd.count())) #161


    word_frequency_per_file_pickle = pickled(rdd)
   # filePrint("tmpFile{}".format(pickle_file))

   # filePrint ("tmpFile reread {}".format(pickleRDD.collect()))
    rdd = unpickled(sc,word_count_per_file_pickle)

    idf_rdd = idfFromWordCountsPerFile(rdd)

    print("\nidf_rdd: {}".format(idf_rdd.collect()))


    '''
    e) Calculate the IDF values and save the list of (word,IDF) pairs for later use.

     Use the list of (file, [(word,count) ... (word,count)]) again to ,\
    \n create a new RDD by reorganising the tuples and lists such that\
    \n the words are the keys and count the occurrences of words per file\
    \n using map(). \n"

    from file [(word,count),(word,count),(word),coutn
    word [(file,count),(file,count),(file,count)]
   '''


    '''
    f) Calculate the TF.IDF values and create a 10000 dimensional vector per document using the hashing trick.
    '''




    end_s_time = time()
    runtime_s = end_s_time - start_s_time
    filePrint("\ntotal running time:{}".format(runtime_s))
    filePrint("ended run at {}\n".format(timestring()))




    g_filehandle.close()
