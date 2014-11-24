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
#from lxml import etree

from unidecode import unidecode

' THE MAIN LOOP '

if __name__ == "__main__":
    '''DEBUGGING'''

    def exceptionTraceBack(exctype, value, tb):
        print 'Jonathan\'s Python / Spark Errors...'
        print 'Type:', exctype
        print 'Value:', value
        traceback.print_tb(tb, limit=20, file=sys.stdout)


    '''LOGGING TRACEBACKS'''
    def timestring():
        cltime = localtime(time())
        return "{:02d}:{:02d}:{:02d}".format(cltime.tm_hour,cltime.tm_min,cltime.tm_sec)
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
            string = ":{0[time_since_start]:7,.3f} :{0[time_since_last]:7,.3f}  {1:}".format(time_deltas, message)
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

    def pickled(rdd, name=None):
        if not name:
            tmp = NamedTemporaryFile(delete=True)
            tmp.close()
            name = tmp.name
        rdd.saveAsPickleFile(name,3)
        return name

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
        #print "d",
       # try:
        #    print ("\nbefore unidecode\n {}".format(text))
       # except:
        #    print ("\nbefore unidecode\n {}".format(text.encode('utf8')))

        text=  unidecode(text) if decode_unicode else text
        #text = text.encode('utf8')
        #print ("\nafter unidecode\n {}".format(text))
        return text


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

    def encodingRegex():
        """
        regex to match character set encoding line in header
        :return:
        """
        logfuncWithArgs()

        regex = re.compile(
            ur'^Character set encoding: (\w+)'
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


    def footerRegex():
         """
         regex to match header lines
         used in per-line processing
         :return: compiled regex
         """
         logfuncWithArgs()

         regex = re.compile(
            ur"^(\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG[^\*]+\*{3})"  #end of header
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
        if random.randint(0,20000) == 1:
            print 'x',
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

    def extractIdAndBodyTextWithFilters(txt,max_file_size,rx_id,rx_body_text,rx_header, rx_footer, rx_encoding,regex_filters=None):
        """
        used by rddWithHeadersRemovedIndexedByID() in per-file processing
        :param txt: text to search (will be one file)
        :param rx_id: regex to extract the ebook id
        :param rx_body_text: regex to extract all text following the header
        :param rx_english: regex to flag English texts from header language info
        :param rx_ascii: regex to flag ASCII-encoded texts from header language info

        :return: tuple of (ebook id, text-with-header-removed)
        """
        if max_file_size and len(txt) > max_file_size:
            print '>',
            return ("_","_")
        print '<',
        id_text = "_"
        body_text = "_"
        header = None
        body = None
        encoding = 1
        id_match = None
        body_match = None
        split_txt = rx_header.split(txt)
        if len(split_txt) == 3:
            header = split_txt[0]
            body = split_txt[2]
            included = 1
            if regex_filters:
                included = 0
                excluded = searchTextWithRegexes(header,regex_filters['exclusions']) if regex_filters['exclusions'] else 0
                if not excluded:
                    included = searchTextWithRegexes(header,regex_filters['inclusions']) if regex_filters['inclusions'] else 1

            if included and body:
                id_match = rx_id.search(header)
                if id_match:
                    split_txt = rx_footer.split(body)
                    if len(split_txt) == 3:
                        body_text = split_txt[0]
                    else:
                        body_text = body
                    id_text = id_match.group(1)
                    encoding_match = rx_encoding.search(header)
                    if encoding_match:
                        encoding_txt = encoding_match.group(1)
                        if encoding_txt == 'utf':
                            encoding = 3
                        elif encoding_txt == 'ASCII':
                            encoding = 2
                        else:
                            encoding = 1
                            #decode ISO encodings otherwise the words break on non-ascii characters
                            body_text = decode(1,body_text)

        result = (id_text,(encoding,body_text))
        #print "result: {}".format(result)
        print '+',
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
        #print("\nwcf: {}".format(wcf.collect()))

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
        global g_counter
        g_counter += 1
        print ' '+str(g_counter)+' ',

        rdd = sc.wholeTextFiles(path)
        (location, folders, files) = walk(path).next()
        for folder in folders:

            sub_path = os.path.join(location, folder)
            rdd = rdd.union(rddOfWholeTextFileRDDs(sub_path))
        return rdd

    ''' PROCESSING PER LINE'''''

    def rddOfMetaFiles(path):
        (location, folders, files) = walk(path).next()
        for filename in files:
            print '.',
            filepath = os.path.join(location, filename)
            if filename != '.DS_Store':
                file = open(filepath)
                rdf = file.read()
                tree = ET.parse(filepath)
                root = tree.getroot()
                for child in root:
                    print("xml: {}".format(child.tag, child.attrib))

                for pgterms in root.findall('pgterms:ebook'):
                    print("pgterms: {}".format(child.tag, child.attrib))


        for folder in folders:
             sub_path = os.path.join(location, folder)
             rddOfMetaFiles(sub_path)


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
            if filename == '.DS_Store': continue
            filepath = os.path.join(location, filename)
            filesize = int(os.path.getsize(filepath))
            if  filesize < max_file_size:
                print '<',
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
                        if exclusion_rdd.take(1):
                            excluded = 1
                    if excluded:
                        continue

                    included = None
                    for inclusion_regex in regex_filters['inclusions']:
                        inclusion_rdd = header_minus_text.map(lambda x: searchWithRegex(x[0],inclusion_regex))\
                                                      .filter(lambda(result):result is not "_")
                        if inclusion_rdd.take(1):
                            included = 1
                            break
                    if not included:
                        continue

                text_minus_header_with_id = text_minus_header.cartesian(id_rdd).map(lambda x:(x[1],x[0]))
                text_combinded= text_minus_header_with_id.reduceByKey(lambda x,y: x+" "+y)
                rdd = rdd.union(text_combinded) if rdd else text_combinded
            else:
                print '>',


        for folder in folders:
             sub_path = os.path.join(location, folder)
             rdd = rdd.union(rddOfTextFilesByLine(sub_path,rx_id,rx_header,regex_filters,max_file_size))

        return rdd



    def idf(word_count_per_file_rdd,number_of_documents):
        """
        :param rdd: array of (file,[(word,count),(word,count)...] )
        :return: idf: array of [( word,idf)] ) where idf = log (N/doc-frequency)
        """
        logTimeIntervalWithMsg("idf: starting...")

        #print("pre: {}".format(word_count_per_file_rdd.collect()))
        rdd = word_count_per_file_rdd.map (lambda (a,b) : (a,[(tuple[0],(a,tuple[1])) for tuple in b]))

        #print("\nintermediate_rdd: {}".format(rdd.collect()))
        rdd = rdd.flatMap(lambda a:a[1])
        #print("\nflatMap: {}".format(rdd.collect()))
        rdd = rdd.map(lambda x:(x[0],1))
        #print("\nmap: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("idf: reduceByKey...")

        rdd = rdd.reduceByKey(add)
        #print("\nreduce: {}".format(rdd.collect()))

        rdd = rdd.map(lambda x:(x[0],math.log(number_of_documents/float(x[1]),2)))
        #print("oo\nidf: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("idf: finished")

        return rdd

    def wordFreqPerDoc(word_count_per_file_rdd):

        #print("\nwordFreqPerDoc input: {}".format(word_count_per_file_rdd.collect()))
        logTimeIntervalWithMsg("wordFreqPerDoc: starting")

        rdd = normaliseWordFrequencies(word_count_per_file_rdd)
        #print("\nnormalise: {}".format(rdd.collect()))

        rdd = rdd.flatMap(lambda x:[(tuple[0],[(x[0],tuple[1])]) for tuple in x[1]])
        #print("\nmap: {}".format(rdd.collect()))

        #rdd = rdd.flatMap()
        #print("\nflatMap: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("wordFreqPerDoc: reduceByKey...")
        rdd = rdd.reduceByKey(add)
        logTimeIntervalWithMsg("wordFreqPerDoc: finished")
        #print("\nreduceByKey: {}".format(rdd.collect()))
        return rdd

    def wfidfFromJoining(idf_rdd,wf_per_doc):
        logTimeIntervalWithMsg("wfidfFromJoining: starting...")
        rdd = idf_rdd.join(wf_per_doc)
        #print("\njoin: {}".format(rdd.collect()))

        rdd = rdd.map(lambda x:(x[0],[(tuple[0],(x[1][0]*tuple[1]))  for tuple in x[1][1]]))
        #print("\nmap: {}".format(rdd.collect()))

        rdd = rdd.flatMap(lambda x: [(tuple[0],[(x[0],tuple[1])]) for tuple in x[1] ])
        #print("\nflatMap: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("wfidfFromJoining: reduceByKey...")

        rdd = rdd.reduceByKey(add)
        #print("\nreduceByKey: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("wfidfFromJoining: finished")

        return rdd

    def sign_hash(x):
        return 1 if len(x) % 2 == 1 else -1


    def makeHashTable():
        counter = [0]
        def hashTable(tupleList, hashtable_size):
            '''
            :param tupleList: list of tuples [(word,count),(word,count)...]
            :param hashtable_size: int size of hash array
            :return:hashTable
            '''
            #logfuncWithArgs()
            #counter[0] += 1
            #print ('h_{}_{}'.format(counter[0],timestring()))
            hash_table = [0] * hashtable_size
            for (word, count)in tupleList:
                x = (hash(word) % hashtable_size) if hashtable_size else 0
                hash_table[x] = hash_table[x] + sign_hash(word) * count
            result = map(lambda x: abs(x), hash_table)
            #print("\nhashTable: {}".format(result))

            return result
        return hashTable

        # def hashTable(tupleList, hashtable_size,hashTable):
        # '''
        # :param tupleList: list of tuples [(word,count),(word,count)...]
        # :param hashtable_size: int size of hash array
        # :return:hashTable
        # '''
        # #logfuncWithArgs()
        # print 'h',
        # hash_table = [0] * hashtable_size
        # for (word, count)in tupleList:
        #     x = (hash(word) % hashtable_size) if hashtable_size else 0
        #     hash_table[x] = hash_table[x] + sign_hash(word) * count
        # result = map(lambda x: abs(x), hash_table)
        # #print("\nhashTable: {}".format(result))
        #
        # return result


    def vectorise(rdd,vec_size):
        '''
        :param wfidif_per_doc: rdd of (word,tf.idf) per doc [(docID, [(word,tf.idf),(word,tf.idf)...])
        :param vec_size: int size of hash array
        :return:hashTable per doc rdd   [(docID, [vector]...]

        '''
        hashTable = makeHashTable()
        logTimeIntervalWithMsg("vectorise...")
        rdd = rdd.map (lambda x:(x[0],hashTable(x[1],vec_size)))
        #rint("\nvectorised: {}".format(rdd.collect()))

        return rdd

    # def makeMakeSplitFilter():
    #     counter1=[0]
    #     def makeSplitFilter(stop_list,wordsplit,decode_unicode,counter = 0):
    #          counter1[0] += 1
    #          counter2 = [0]
    #          def splitfilter(iebook_id,words):
    #              print '^{}.{}'.format(counter1[0],counter2[0]),
    #              counter2[0] += 1
    #              return  ([(iebook_id, remPlural(word))
    #                                           for word in re.split(wordsplit, decode(decode_unicode,words))
    #                                           if len(word) > 0
    #                                           and word not in stop_list
    #                                           and remPlural(word) not in stop_list])
    #          return splitfilter
    #     return makeSplitFilter

    def makeSplitFilter(stop_list,wordsplit,decode_unicode,counter = 0):
         counter = [0]
         def splitfilter(iebook_id,words):
             #print '^{`}_{}'.format(timestring(),(counter[0])),
             #counter[0] += 1
             return  ([(iebook_id, remPlural(word))
                                          for word in re.split(wordsplit, words)
                                          if len(word) > 0
                                          and word not in stop_list
                                          and remPlural(word) not in stop_list])
         return splitfilter


    def processRDD(rdd, stop_list_path,decode_unicode):
        """
        :param rdd:  rdd as read from filesystem ('filename','file_contents')
        :param stop_list_path: [list, of, stop, words]
        :return:wordCountPerFileRDD [(filename,[(word,count)][(word,count)]...)]
        """
        logfuncWithVals()

        #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")
        stop_list = stopList(stop_list_path)
        wordsplit = re.compile('\W+',re.UNICODE)
        #makeSplitFilter = makeMakeSplitFilter()
        splitFilter = makeSplitFilter(stop_list,wordsplit,decode_unicode)
        flatmapped_rdd = rdd.flatMap(lambda (iebook_id, words):splitFilter(iebook_id,words))

        wordcount_per_file_rdd = wordCountPerFile(flatmapped_rdd)
        #print ("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))

        # logTimeIntervalWithMsg("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))
        return wordcount_per_file_rdd




    def rddWithHeadersRemovedIndexedByID(rdd,max_file_size, rx_id, rx_body_text,rx_header,rx_footer,rx_encoding, regex_filters=None):
        """
        b)From the text files you need to remove the header.
        The last line of the header starts and ends with ***.
        c)You need to extract the ID of the text,
        which occurs in the header in the format [EBook #<ID>],
        where ID is a natural number.
        return: [(book_id,(encoding_val,txt))...]
        """
        rdd = rdd.map(lambda x:extractIdAndBodyTextWithFilters(x[1],max_file_size,rx_id,rx_body_text,rx_header,rx_footer,rx_encoding, regex_filters)) \
                  .filter(lambda (iebook_id,txt_tuple): iebook_id is not "_")
        return rdd

    '''M E T A    D A T A '''

    def ebookID(ebook_node):
        for key in ebook_node.attrib:
            if re.search('about',key):
                id = ebook_node.attrib[key]
                #return id
                regex = re.compile(r'ebooks\/(\d+)')
                match = regex.match(id)
                if (match):
                    return match.group(1)
            else:
                return None

    def subjectTuple(subject_node):
        #print ("subject_node: {}".format(subject_node))
        #3print ("subject_node attrib: {}".format(subject_node.attrib))
        subject_id = ""
        subject_txt = ""
        for Description in subject_node:
            if re.search('Description',Description.tag):
                for nodeID in Description.attrib:
                    if re.search('nodeID',nodeID):
                        subject_id = Description.attrib[nodeID]
                        #print ("ID attrib: {}".format(Description.attrib))
                for value in Description:
                    if re.search('value',value.tag):
                        #print ("value text: {}".format(value.text))
                        subject_txt = value.text

        return (subject_id, subject_txt)


    def  subjectDict(subject_node):
        #print ("subject_node: {}".format(subject_node))
        #3print ("subject_node attrib: {}".format(subject_node.attrib))
        subject = {'subject_id':None,'subject_txt':None}
        for Description in subject_node:
            if re.search('Description',Description.tag):
                for nodeID in Description.attrib:
                    if re.search('nodeID',nodeID):
                        subject['subject_id'] = Description.attrib[nodeID]
                        #print ("ID attrib: {}".format(Description.attrib))
                for value in Description:
                    if re.search('value',value.tag):
                        #print ("value text: {}".format(value.text))
                        subject['subject_txt'] = value.text

        return subject

    def arrayOfMetadataArrays(path, metadata=[]):
        count = 0
        (location, folders, files) = os.walk(path).next()
        for filename in files:
            filepath = os.path.join(location, filename)
            if filename != '.DS_Store':
                count += 1
                print str(count)+' ',
                metaArray = []
                tree = ET.parse(filepath)
                root = tree.getroot()
                for ebook in root:
                    if re.search('ebook',ebook.tag):
                        metaArray.append(ebookID(ebook))
                        metadata.append(metaArray)
                        metaArray.append([])
                        for subject in ebook:
                           if re.search('subject',subject.tag):
                                  metaArray[1].append(subjectTuple(subject))
        for folder in folders:
             sub_path = os.path.join(location, folder)
             arrayOfMetadataArrays(sub_path,metadata)

        return metadata


    def arrayOfMetadata(path, metadata=[]):
        count = 0
        (location, folders, files) = os.walk(path).next()
        for filename in files:
            filepath = os.path.join(location, filename)
            if filename != '.DS_Store':
                print str(len(metadata))+' ',
                metadict = {}
                metadict['filename'] = filename
                tree = ET.parse(filepath)
                root = tree.getroot()
                for ebook in root:
                    if re.search('ebook',ebook.tag):
                        metadict['ebook_id'] = ebookID(ebook)
                        metadata.append(metadict)
                        metadict['subjects']=[]
                        for subject in ebook:
                           if re.search('subject',subject.tag):
                                  metadict['subjects'].append(subjectDict(subject))
        for folder in folders:
             sub_path = os.path.join(location, folder)
             arrayOfMetadata(sub_path,metadata)

        return metadata



    '''INITIALISATION'''
    sys.excepthook = exceptionTraceBack

    global s_time
    global is_time
    global g_start_time
    global g_counter
    global g_max_file_size

    s_time = time()
    start_s_time = s_time
    is_time = s_time
    g_start_time = time()
    g_counter = 0

    global found
    found = 0
    global not_found
    not_found = 0

    validateInput()
    global g_filehandle
    logfile = 'out.txt'
    logfile = os.path.abspath(logfile)
    g_filehandle = open(logfile, 'a')
    eventLogDir = 'data/eventLog'
    eventLogDir = os.path.abspath(eventLogDir)
    recoveryDir = 'data/recovery'
    recoveryDir = os.path.abspath(recoveryDir)


    '''END OF INITIALISATION'''

    filePrint("\n\nstarted run at {}".format(timestring()))


    parsed_args = parseArgs(sys.argv)
    if len(parsed_args) < 1:
        printHelp()
        exit(-1)


    line_processing = parsed_args['l'] if 'l' in parsed_args else None
    text_path = parsed_args['t'] if 't' in parsed_args else sys.argv[1]
    stop_list_path = parsed_args['s'] if 's' in parsed_args else []
    filter_files = parsed_args['f'] if 'f' in parsed_args else None
    max_file_size = int(parsed_args['max']) if 'max'in parsed_args else None
    decode_unicode = parsed_args['d'] if 'd' in parsed_args else None
    cores = int(parsed_args['c']) if 'c' in parsed_args else 2
    mem = parsed_args['m'] if 'm' in parsed_args else 8
    parrellelismMultiplier = int(parsed_args['p']) if 'p' in parsed_args else 8
    meta_path = parsed_args['meta'] if 'meta' in parsed_args else None
    pickling = parsed_args['pickle'] if 'pickle' in parsed_args else None
    number_of_documents = int(parsed_args['n']) if 'n' in parsed_args else None
    tmp_path = int(parsed_args['tmp']) if 'tmp' in parsed_args else 'data/tmp'
    delete_files = int(parsed_args['del']) if 'del' in parsed_args else None



    masterConfig = "local[{}]".format(cores)
    memoryConfig = "{}g".format(mem)
    parallelismConfig = "{}".format(cores*parrellelismMultiplier)
    sparkConf = SparkConf()
    sparkConf.setMaster(masterConfig).setAppName("project")
    sparkConf.set("spark.logConf","true")
    sparkConf.set("spark.executor.memory",memoryConfig)
    sparkConf.set("spark.default.parallelism",parallelismConfig)
    sparkConf.set("spark.eventLog.enabled","false")
    sparkConf.set("spark.eventLog.dir",eventLogDir)
    sparkConf.set("spark.ui.port","7171")
    #sparkConf.set("spark.deploy.recoveryMode","FILESYSTEM")
    #sparkConf.set("spark.deploy.recoveryDirectory",recoveryDir)

    sc = SparkContext(conf=sparkConf)


    logTimeIntervalWithMsg ("{}".format(parsed_args))
    logTimeIntervalWithMsg("sparkConf.toDebugString() {}".format(sparkConf.toDebugString()))

    '''
     example usages

     first ensure
        ulimit -a //lists current settings
     ulimit -n 4096
     spark-submit  --driver-memory 12g project.py t=data/text-tiny s=stopwords_en.txt

     spark-submit  --driver-memory 8g project.py t=data/text_party s=stopwords_en.txt f=1
     spark-submit project.py t=data/text_party s=stopwords_en.txt n=1 f=1 max=100000
     ensure ulimit is as high as possible


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


    meta_pickle = 'data/metapickle'
    if meta_path and not os.path.exists(meta_pickle):
        metadata = arrayOfMetadata(meta_path)
        logTimeIntervalWithMsg ("metalen: {}".format(len(metadata)))
        meta_pickle = pickled(sc.parallelize(metadata),meta_pickle)
        print ("meta_pickle: {}".format(meta_pickle))
        #meta_unpickled = unpickled(sc,meta_pickle)

    else:
        print ("metadata already pickled")


    #meta_unpickled = unpickled(sc,meta_pickle)
    #logTimeIntervalWithMsg ("meta_unpickled_size: {}".format(meta_unpickled.count()))
    text_basename =  os.path.splitext(text_path)[0]
    vector_pickle = "{}/{}_pickle".format(tmp_path,text_basename)
    if os.path.exists(vector_pickle) and delete_files:
        shutil.rmtree(vector_pickle)
    if os.path.exists(vector_pickle):
        number_of_input_files = 0
        print ("{} already pickled: {}".format(text_basename,vector_pickle))
        vectors = unpickled(sc,vector_pickle)
    else:
        number_of_input_files = numberOfInputFiles(text_path)
        logTimeIntervalWithMsg ("input files: {} ".format(number_of_input_files)) #161

        if filter_files:
            logTimeIntervalWithMsg("filtering files...")
            regex_filters = regexFilters()
        else:
            logTimeIntervalWithMsg("not filtering files...")
            regex_filters=None

        rx_id = idRegex()

        if line_processing:
            # line-by-line processing: this is slower but allows us to filter for maximum file sizes
            rx_header = headerRegex()
            rdd = rddOfTextFilesByLine(text_path,rx_id,rx_header,regex_filters,max_file_size)
            # remove duplicates is implemented with this reduceByKey method.
            # if we encounter more than one text with
            # the same bookID we will only use the first.
            rdd = rdd.reduceByKey(lambda x,y: x)

        else:
            # file-by-file processing- faster but cannot filter for maximum file sizes
            rx_body_text = bodyTextRegex()
            rx_header = headerRegex()
            rx_footer = footerRegex()
            rx_encoding = encodingRegex()
            rdd = rddOfWholeTextFileRDDs(text_path)
            rdd = rddWithHeadersRemovedIndexedByID(rdd,max_file_size, rx_id, rx_body_text,rx_header,rx_footer,rx_encoding, regex_filters)
            # remove duplicates is implemented with this reduceByKey method.
            # our input rdd is (id,(0,file),(id(1,file))... where 2 is best encoding (utf8) and 0 is worst(iso)
            rdd = rdd.reduceByKey(lambda x,y:  x if x[0] > y[0] else y)\
                                         .map(lambda x:(x[0],x[1][1]))
        logTimeIntervalWithMsg ("about to start processRDD") #161


        word_count_per_file_rdd = None
        word_count_per_file_pickle = None
        wf_per_doc = None
        wf_per_doc_pickle = None
        idf_rdd = None
        wfidf_rdd = None

        if pickling:
            word_count_per_file_pickle = pickled(processRDD(rdd,stop_list_path,decode_unicode)) #.persist(sc.StorageLevel.MEMORY_AND_DISK)
            number_of_documents = unpickled(sc,word_count_per_file_pickle).count() if number_of_documents is None else number_of_documents
            wf_per_doc_pickle = pickled(wordFreqPerDoc(unpickled(sc,word_count_per_file_pickle)))
            idf_rdd = idf(unpickled(sc,word_count_per_file_pickle),number_of_documents)
            wfidf_rdd = wfidfFromJoining(idf_rdd,unpickled(sc,wf_per_doc_pickle))

        else:
            word_count_per_file_rdd = processRDD(rdd,stop_list_path,decode_unicode).persist(StorageLevel.MEMORY_ONLY)
            wf_per_doc = wordFreqPerDoc(word_count_per_file_rdd)
            logTimeIntervalWithMsg("counting the docs...")
            number_of_documents = word_count_per_file_rdd.count() if number_of_documents is None else number_of_documents
            logTimeIntervalWithMsg("idf...")
            idf_rdd = idf(word_count_per_file_rdd,number_of_documents)
            wfidf_rdd = wfidfFromJoining(idf_rdd,wf_per_doc)

        vec_size = 10000
        vectors = vectorise(wfidf_rdd,vec_size)
        vector_pickle = pickled(vectors,vector_pickle)


    logTimeIntervalWithMsg ("input files: {} found texts: {}"
                            .format(number_of_input_files,vectors.count()))

    '''
    e) Calculate the IDF values and save the list of (word,IDF) pairs for later use.

    '''




    '''
    f) (1) Calculate the TF.IDF values
    '''


    '''
    f) (2) create a 10000 dimensional vector per document using the hashing trick.
    '''



    #print("\nidf_rdd: {}".format(idf_rdd.take(20)))

 #161

    '''

    SECTION 2
    2 Reading and preparing metadata from XML (15%)
    a) Read the metadata files from the meta directory as files (not RDDs).
    b) Parse the XML (will be covered after Reading week).
    '''




    '''



    c) Extract the subjects and the ID which is given in pgterms:ebook element as the rdf:about attribute in the form ebooks/<ID>. The subjects are given in the metadata, both in free text and using subject codes according to the Dublin Core standard (mostly in the Library of Congress
    1
    2 TILLMAN WEYDE, DANIEL WOLFF, SON N. TRAN
    Classification).
    d) Store the subject lists per file as an RDD for later use.

    3 Training classifiers (30%)
    a) Find the 10 most frequent subjects.
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



    end_s_time = time()
    runtime_s = end_s_time - start_s_time
    filePrint("\ntotal running time:{}".format(runtime_s))
    filePrint("ended run at {}\n".format(timestring()))




    g_filehandle.close()
