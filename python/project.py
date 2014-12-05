# -*- coding: utf-8 -*-
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



    def arrayOfInputPaths(path, array=[]):
        '''
        :param path:
        :param array:
        :return:
        '''
        for location, folder, files in walk(path):
            for file in files:
                array.append(os.path.join(location,file)) if file.endswith('.txt') else None
        #for folder in folders:
            #sub_path = os.path.join(location, folder)
            #array = array + arrayOfInputPaths(sub_path, array)
        return array




    def ebookIdFromFileName(path):
        name = os.path.split(path)[1]
        if name.endswith('.txt'):
            name = name[:-4]
            return re.split('-',name)[0]
        else:
            return None


    def dictOfInputGroups(path, dict={}, e_id=None):
        '''
        :param path:
        :param array:
        :return:
        '''

        for (location, folders, files) in walk(path):
            for file in files:
                key = ebookIdFromFileName(file)
                if key:
                    if key not in dict:
                        dict[key] = []
                    dict[key].append(os.path.join(location,file))

        return dict




    def arrayOfInputFiles(path, max_size=None):
        '''
        :param path:
        :param array:
        :return:
        '''
        counter = 0
        dict = {}
        if str(max_size).endswith('k') or str(max_size).endswith('K'):
            max_size = int(max_size[:-1])*1000
        elif str(max_size).endswith('m') or str(max_size).endswith('M'):
            max_size = int(max_size[:-1])*1000000
        elif str(max_size).endswith('g') or str(max_size).endswith('G'):
            max_size = int(max_size[:-1])*1000000
        elif max_size:
            max_size = int(max_size)

        def nameOfFile(filepath):
            return os.path.splitext(os.path.basename(path))[0]

        def valency(filename):
            name = os.path.splitext(os.path.basename(path))[0]
            if name.endswith('-0'): #utf-8
                return 3
            elif name.endswith('-8'): #iso-
                return 1
            else: #presumably ascii
                return 2


        human_genome_ids = ['11775','11776','11777','11778','11779','11780','11781','11782','11783','11784','11785','11786','11787','11788','11789','11790','11791','11792','11793','11794','11795','11796','11797','11798','11799','2201','2202','2203','2204','2205','2206','2207','2208','2209','2210','2211','2212','2213','2214','2215','2216','2217','2218','2219','2220','2221','2222','2223','2224','3501','3502','3503','3504','3505','3506','3507','3508','3509','3510','3511','3512','3513','3514','3515','3516','3517','3518','3519','3520','3521','3522','3523','3524']
        blacklist_ids = human_genome_ids
        for (location, folders, files) in walk(path):
            for file in files:
                if not file.endswith('.txt'):continue
                filepath = os.path.join(location,file)
                if max_size:
                    filesize = int(os.path.getsize(filepath))
                    if filesize > max_size:
                        #print "skipping file {} as size {} is greater than max: {}".format(filepath,filesize,max_size)
                        print '>',
                        continue
                key = ebookIdFromFileName(file)
                print '<',
                if key in blacklist_ids:
                        #print "excluding from blacklist: {}".format(filepath)
                        print '-'
                        continue
                if key not in dict:
                    dict[key] = {}
                    dict[key] = os.path.join(location,file)
                    counter +=1
                elif file == (os.path.basename(dict[key])):
                    #we have a dupe, so filter...
                    #compare filesizes to decide if this is a 'real' dupe
                    filesize1 = int(os.path.getsize(dict[key]))
                    filesize2 = int(os.path.getsize(filepath))
                    if filesize1 == filesize2:
                       #print ("DUPE file, so not storing:\n{} \n{}".format(dict[key],filepath ))
                       print '=',
                    else:
                        counter +=1
                        filekey = "{}_{}".format( ebookIdFromFileName(file), counter)
                        dict[filekey] = filepath
                        #print ("NOT dupe file, storing in separate key {}:\n{} \n{}".format(filekey,dict[key],filepath ))
                        print 'k',
                else:
                    #(1) preference is UTF8, then ASCII, then ISO
                    if valency(dict[key]) <  valency(file):
                        #print "valency: replacing file: {} with file: {}".format(dict[key],filepath)
                        print 'v',
                        dict[key] = filepath


                #dict[key][file].append(os.path.join(location,file))

        #print ("input files:")
        #pprint(dict)
        print ("number of files: {}".format(len(dict)))

        return dict.values()



    '''OUTPUT'''

    def pickle(rdd, name=None,delete_files=0):
        logfuncWithVals()

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


    def headerLongRegex():
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

    def headerShortRegex():
         """
         regex to match header lines
         used in per-line processing
         :return: compiled regex
         """
         logfuncWithArgs()

         regex = re.compile(
            ur"^(\*{3} *START OF TH(?:IS|E) PROJECT GUTENBERG)"  #end of header
            ,flags=re.MULTILINE|re.UNICODE)
         return regex

    def endOfSmallPrintRegex():
         """
         regex to match header lines by 'end of small print'
         used in per-line processing
         :return: compiled regex
         """
         logfuncWithArgs()

         regex = re.compile(
            ur"^(\*END[\* ]+THE SMALL PRINT\!)"  #end of header
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
            ur"^(\*{3} *END OF TH(?:IS|E) PROJECT GUTENBERG)"  #end of header
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
            ur"\[E(?:Book|text) \#([\d]+)\]"
            ,flags=re.IGNORECASE
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





    def extractIdAndBodyTextWithFilters(txt,max_file_size,rx_id,rx_body_text,rx_header,rx_smallprint, rx_footer, rx_encoding,regex_filters=None):
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
        if len(split_txt) != 3:
            split_txt = rx_smallprint.split(txt)
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
                            body_text =  unidecode(body_text)

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


        #logfuncWithVals()
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







    def wordCountPerFile(rdd,numPartitions=None):
        """
        :param rdd: rdd of (file,word) tuples
        :return:rdd of (file, [(word, count),(word, count)...]) tuples
        """
        #logfuncWithArgs()
        #logTimeIntervalWithMsg ('starting wordCountPerFile')
        #logTimeIntervalWithMsg("##### BUILDING wordCountPerFile #####")
        wcf = rdd.map(lambda(x): ((x[0], x[1]), 1))
        #print("\nwcf: {}".format(wcf.collect()))

        #logTimeIntervalWithMsg('##### GETTING THE  ((file,word),n)\
        # WORDCOUNT PER (DOC, WORD) #####')
        if numPartitions:result = wcf.reduceByKey(add,numPartitions)
        else: result = wcf.reduceByKey(add)

        #print ("wcf: {}".format(result.take(1)))

        #logTimeIntervalWithMsg('##### REARRANGE AS  (file, [(word, count)])  #####')
        result = result.map(lambda (a, b): (a[0], [(a[1], b)]))
        #print ("wordcount: {}".format(result.take(1)))
        #logTimeIntervalWithMsg ('##### CONCATENATE (WORD,COUNT) LIST PER FILE \
        #       AS  (file, [(word, count),(word, count)...])  #####')
        if numPartitions:result = result.reduceByKey(add,numPartitions)
        else:result = result.reduceByKey(add)
        #logTimeIntervalWithMsg ('finished wordCountPerFile')

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


    def rddOfSingleTextFileProcessedByFile(filepath,numPartitions=None):
        textfile_rdd = sc.textFile(filepath)
        #print ("\ntextfile_rdd \n{}".format(textfile_rdd.collect()))

        textfile_glom_rdd = textfile_rdd.glom().map(lambda line:"\n".join(line)).filter(lambda x:x is not '')
        #print ("\n\ntextfile_glom_rdd \n{}".format(textfile_glom_rdd.collect()))

        if numPartitions:
            result = textfile_glom_rdd.map(lambda x: (filepath,x)).reduceByKey(add,numPartitions)
        else:
            result = textfile_glom_rdd.map(lambda x: (filepath,x)).reduceByKey(add)
        #print ("\n\nresult \n{}".format(result.collect()))
        return result


    def rddOfSingleTextFileProcessedByLine(filepath, rx_id, rx_header,rx_smallprint,rx_footer,rx_encoding):


        textfile_rdd = sc.textFile(filepath)
        #print ("texfile_rdd collect {}".format(textfile_rdd.collect()))

        textfile_rdd = textfile_rdd.zipWithIndex()
        #print ("texfile_rdd zipWithIndex {}".format(textfile_rdd.collect()))

        header_list_item = textfile_rdd.map(lambda x: (searchWithRegex(x[0], rx_header), x[1])) \
                          .filter(lambda x: x[0] is not "_").take(1)
        if not header_list_item:
               header_list_item = textfile_rdd.map(lambda x: (searchWithRegex(x[0], rx_smallprint), x[1])) \
                          .filter(lambda x: x[0] is not "_").take(1)
        footer_list_item = textfile_rdd.map(lambda x: (searchWithRegex(x[0], rx_footer), x[1])) \
                          .filter(lambda x: x[0] is not "_").take(1)
        header_line_number = header_list_item[0][1] if header_list_item else None
        footer_line_number = footer_list_item[0][1] if footer_list_item else None
        #print ("header_line_number {} footer_line_number {}".format(header_line_number,footer_line_number))

        if not header_line_number:
            return None

        if (footer_line_number):
            text_minus_header = textfile_rdd.filter(lambda x:(footer_line_number)>x[1]>(header_line_number+1)).map(lambda x:x[0])
        else:
            text_minus_header = textfile_rdd.filter(lambda x:x[1]>(header_line_number+1)).map(lambda x:x[0])



        #print ("\ntext_minus_header {}".format(text_minus_header.collect()))
        #print ("\ntext_minus_header joined {}".format(text_minus_header.glom().map(lambda x:" ".join(x)).collect()))


        header_minus_text = textfile_rdd.filter(lambda x:x[1]<(header_line_number+1)).map(lambda x:x[0])
        #print ("\nheader_minus_text {}".format(header_minus_text.collect()))
        id_rdd = header_minus_text.map(lambda x: searchWithRegex(x,rx_id)).filter(lambda(ebookid):ebookid is not "_")

        if not id_rdd:
            print '-i',
            return None
        else:
            print ("id_rdd: {}".format(id_rdd.collect()))

        id_txt = id_rdd.take(1)[0]

        #print ("id_rdd {}".format(id_rdd.collect()))

        #text_combined= text_minus_header.reduce(lambda x,y: (y.encode('utf8')+u' '+x.encode('utf8')))
        text_combined= text_minus_header.reduce(lambda x,y: x+' '+y)
        textfile_glom_rdd = text_minus_header.glom().map(lambda line:" ".join(line)).filter(lambda x:x is not '')

        #print ("\ntextfile_glom_rdd {}".format(textfile_glom_rdd.collect()))



        result = textfile_glom_rdd.map(lambda x: (id_txt,x))

        #print "result: {}".format(result.collect())
        print '+',
        return result






    def rddOfTextFilesByLine(path, rx_id, rx_header,rx_smallprint,regex_filters,max_file_size):
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
                if not header_rdd:
                    header_rdd = textfile_rdd.map(lambda x: (searchWithRegex(x[0], rx_smallprint), x[1])) \
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



    def idf(word_count_per_file_rdd,number_of_documents, numPartitions=None):
        """
        :param rdd: array of (file,[(word,count),(word,count)...] )
        :return: idf: array of [( word,idf)] ) where idf = log (N/doc-frequency)
        """

        #print ("\n\nidf_rdd_input: number of docs {}\n{}".format(number_of_documents, word_count_per_file_rdd.collect()))

        #logTimeIntervalWithMsg("idf: starting...")

        #print("pre: {}".format(word_count_per_file_rdd.collect()))
        rdd = word_count_per_file_rdd.map (lambda (a,b) : (a,[(tuple[0],(a,tuple[1])) for tuple in b]))

        #print("\nintermediate_rdd: {}".format(rdd.collect()))
        rdd = rdd.flatMap(lambda a:a[1])
        #print("\nflatMap: {}".format(rdd.collect()))
        rdd = rdd.map(lambda x:(x[0],1))
        #print("\nmap: {}".format(rdd.collect()))
        #logTimeIntervalWithMsg("idf: reduceByKey...")
        if numPartitions:
            rdd = rdd.reduceByKey(add,numPartitions)
        else:
            rdd = rdd.reduceByKey(add)
        #print("\nreduce: {}".format(rdd.collect()))

        rdd = rdd.map(lambda x:(x[0],math.log(number_of_documents/float(x[1]),2)))
        #print("oo\nidf: {}".format(rdd.collect()))
        #logTimeIntervalWithMsg("idf: finished")
        #print ("\n\nidf_rdd\n{}".format(rdd.collect()))

        return rdd

    def wordFreqPerDoc(word_count_per_file_rdd, numPartitions=None):

        #print("\nwordFreqPerDoc input: {}".format(word_count_per_file_rdd.collect()))
        #logTimeIntervalWithMsg("wordFreqPerDoc: starting")

        rdd = normaliseWordFrequencies(word_count_per_file_rdd)
        #print("\nnormalise: {}".format(rdd.collect()))

        rdd = rdd.flatMap(lambda x:[(tuple[0],[(x[0],tuple[1])]) for tuple in x[1]])
        #print("\nmap: {}".format(rdd.collect()))

        #rdd = rdd.flatMap()
        #print("\nflatMap: {}".format(rdd.collect()))
        #logTimeIntervalWithMsg("wordFreqPerDoc: reduceByKey...")
        if numPartitions:
            rdd = rdd.reduceByKey(add,numPartitions)
        else:
            rdd = rdd.reduceByKey(add)
        #logTimeIntervalWithMsg("wordFreqPerDoc: finished")
        #print("\nreduceByKey: {}".format(rdd.collect()))
        return rdd

    def wfidfFromJoining(idf_rdd,wf_per_doc,numPartitions=None):
        logTimeIntervalWithMsg("wfidfFromJoining: starting...")
        if numPartitions:
            rdd = idf_rdd.join(wf_per_doc,numPartitions)
        else:
            rdd = idf_rdd.join(wf_per_doc)

        #print("\njoin: {}".format(rdd.collect()))

        rdd = rdd.map(lambda x:(x[0],[(tuple[0],(x[1][0]*tuple[1]))  for tuple in x[1][1]]))
        #print("\nmap: {}".format(rdd.collect()))

        rdd = rdd.flatMap(lambda x: [(tuple[0],[(x[0],tuple[1])]) for tuple in x[1] ])
        #print("\nflatMap: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("wfidfFromJoining: reduceByKey...")
        if numPartitions:
            rdd = rdd.reduceByKey(add,numPartitions)
        else:
            rdd = rdd.reduceByKey(add)

        #print("\nreduceByKey: {}".format(rdd.collect()))
        logTimeIntervalWithMsg("wfidfFromJoining: finished")

        return rdd

    def sign_hash(x):
        return 1 if len(x) % 2 == 1 else -1




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

        def makeHashTable():  #failed attempt to use a closure as a counter
            counter = [0]
            def hashTable(id, tupleList, hashtable_size):
                '''
                :param tupleList: list of tuples [(word,count),(word,count)...]
                :param hashtable_size: int size of hash array
                :return:hashTable
                '''
                #logfuncWithArgs()
                counter[0] += 1
                #print "#{} ".format(counter[0]),
                print "#{} ".format(id),
                hash_table = [0] * hashtable_size
                for (word, count)in tupleList:
                    x = (hash(word) % hashtable_size) if hashtable_size else 0
                    hash_table[x] = hash_table[x] + sign_hash(word) * count
                result = map(lambda x: abs(x), hash_table)
                #print("\nhashTable: {}".format(result))

                return result
            return hashTable


        vector = makeHashTable()
        #logTimeIntervalWithMsg("vectorise...")
        #print("size of rdd to vectorise:{}",format(rdd.count()))
        #pprint(rdd.take(3))
        print("vectorising... size:{}".format(rdd.count()))
        rdd = rdd.map (lambda x:(x[0],vector(x[0],x[1],vec_size))).cache()
        print("vector size:{}".format(rdd.count()))

        #print(rdd.sortByKey().take(1))
        #print("\nvectorised: {}".format(rdd.collect()))

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

    def makeSplitFilter(stop_list,wordsplit,counter = 0):
         counter = [0]
         def splitfilter(iebook_id,words):
             #print '^{`}_{}'.format(timestring(),(counter[0])),
             counter[0] += 1
             return  ([(iebook_id, remPlural(word))
                                          for word in re.split(wordsplit, words)
                                          if len(word) > 0
                                          and word not in stop_list
                                          and remPlural(word) not in stop_list])
         return splitfilter


    def processRDD(rdd, stop_list_path, numPartitions=None):
        """
        :param rdd:  rdd as read from filesystem ('filename','file_contents')
        :param stop_list_path: [list, of, stop, words]
        :return:wordCountPerFileRDD [(filename,[(word,count)][(word,count)]...)]
        """
        #logfuncWithVals()

        #logTimeIntervalWithMsg("##### BUILDING (file, word) tuples #####")
        stop_list = stopList(stop_list_path)
        wordsplit = re.compile('\W+',re.UNICODE)
        #makeSplitFilter = makeMakeSplitFilter()
        splitFilter = makeSplitFilter(stop_list,wordsplit)
        flatmapped_rdd = rdd.flatMap(lambda (iebook_id, words):splitFilter(iebook_id,words))

        wordcount_per_file_rdd = wordCountPerFile(flatmapped_rdd,numPartitions)
        #print ("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))

        # logTimeIntervalWithMsg("wordCountPerFileRDD {}".format(wordCountPerFileRDD.take(1)))
        return wordcount_per_file_rdd



    def rddWithHeadersRemovedIndexedByIDBatch(rdd, rx_id, rx_body_text,rx_header,rx_smallprint,rx_footer,rx_encoding):
        """
        b)From the text files you need to remove the header.
        The last line of the header starts and ends with ***.
        c)You need to extract the ID of the text,
        which occurs in the header in the format [EBook #<ID>],
        where ID is a natural number.
        return: [(book_id,(encoding_val,txt))...]
        """

        def isIsoEncoded(text,rx_encoding):
            encoding_match = rx_encoding.search(text)
            is_iso_encoded = True
            if encoding_match:
                encoding_txt = encoding_match.group(1)
                if encoding_txt == 'utf':
                    is_iso_encoded = False
                elif encoding_txt == 'ASCII':
                    is_iso_encoded = False

            return is_iso_encoded

        def extract(txt,rx_id,rx_body_text,rx_header,rx_smallprint, rx_footer):
            """
            used by rddWithHeadersRemovedIndexedByID() in per-file processing
            :param txt: text to search (will be one file)
            :param rx_id: regex to extract the ebook id
            :param rx_body_text: regex to extract all text following the header
            :param rx_english: regex to flag English texts from header language info
            :param rx_ascii: regex to flag ASCII-encoded texts from header language info

            :return: tuple of (ebook id, text-with-header-removed)
            """

            # id_text = "_"
            # body_text = "_"
            # header = None
            # body = None
            # id_match = None
            # body_match = None
            result = ("_","_")
            split_txt = rx_header.split(txt)
            if len(split_txt) != 3:
                split_txt = rx_smallprint.split(txt)
            if len(split_txt) == 3:
                header = split_txt[0]
                body = split_txt[2]
                if  body:
                    id_match = rx_id.search(header)
                    if id_match:
                        split_txt = rx_footer.split(body)
                        if len(split_txt) == 3:
                            body_text = split_txt[0]
                        else:
                            body_text = body
                        id_text = id_match.group(1)
                        if isIsoEncoded(txt,rx_encoding):
                            body_text = unidecode(body_text)

                        result = (id_text,body_text)
            #print "result: {}".format(result)
            print '+',
            #print "\n\nextract {}\n{}\n".format(id_match,result)
            return result


        rdd = rdd.map(lambda x:extract(x[1],rx_id,rx_body_text,rx_header,rx_smallprint,rx_footer)) \
            .filter(lambda (iebook_id,txt_tuple): iebook_id is not "_")

        return rdd


    def rddWithHeadersRemovedIndexedByID(rdd,max_size, rx_id, rx_body_text,rx_header,rx_smallprint,rx_footer,rx_encoding, regex_filters=None):
        """
        b)From the text files you need to remove the header.
        The last line of the header starts and ends with ***.
        c)You need to extract the ID of the text,
        which occurs in the header in the format [EBook #<ID>],
        where ID is a natural number.
        return: [(book_id,(encoding_val,txt))...]
        """

        if str(max_size).endswith('k') or str(max_size).endswith('K'):
            max_size = int(max_size[:-1])*1000
        elif str(max_size).endswith('m') or str(max_size).endswith('M'):
            max_size = int(max_size[:-1])*1000000
        elif str(max_size).endswith('g') or str(max_size).endswith('G'):
            max_size = int(max_size[:-1])*1000000
        elif max_size:
            max_size = int(max_size)

        rdd = rdd.map(lambda x:extractIdAndBodyTextWithFilters(x[1],max_size,rx_id,rx_body_text,rx_header,rx_smallprint,rx_footer,rx_encoding, regex_filters)) \
                  .filter(lambda (iebook_id,txt_tuple): iebook_id is not "_")
        return rdd



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
    eventlog_dir = 'data/_eventLog'
    eventlog_dir = os.path.abspath(eventlog_dir)
    recovery_dir = 'data/_recovery'
    recovery_dir = os.path.abspath(recovery_dir)
    temp_dir = 'data/_tmp'
    temp_dir = os.path.abspath(temp_dir)
    pickle_dir = 'data/_pickles'
    pickle_dir = os.path.abspath(pickle_dir)


    '''END OF INITIALISATION'''

    filePrint("\n\nstarted run at {}".format(timestring()))


    parsed_args = parseArgs(sys.argv)
    if len(parsed_args) < 1:
        printHelp()
        exit(-1)


    line_processing = parsed_args['l'] if 'l' in parsed_args else None
    text_path = parsed_args['t'] if 't' in parsed_args else None
    stop_list_path = parsed_args['s'] if 's' in parsed_args else []
    filter_files = parsed_args['f'] if 'f' in parsed_args else None
    max_file_size = parsed_args['max'] if 'max'in parsed_args else None

    decode_unicode = parsed_args['d'] if 'd' in parsed_args else None
    cores = int(parsed_args['c']) if 'c' in parsed_args else 2
    mem = parsed_args['m'] if 'm' in parsed_args else 8
    parrellelismMultiplier = int(parsed_args['p']) if 'p' in parsed_args else 8
    meta_path = parsed_args['meta'] if 'meta' in parsed_args else None
    pickle_name = parsed_args['pickle'] if 'pickle' in parsed_args else None
    batch_processing = int(parsed_args['batch']) if 'batch' in parsed_args else 1
    batch_start = int(parsed_args['b_start']) if 'b_start' in parsed_args else 0
    vector_size = int(parsed_args['vec']) if 'vec' in parsed_args else 10000

    number_of_documents = None
    #checkpoint_path = int(parsed_args['chk']) if 'chk' in parsed_args else 'data/tmp/chk'

    delete_files = int(parsed_args['del']) if 'del' in parsed_args else None


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
    sparkConf.set("spark.eventLog.dir",eventlog_dir)
    sparkConf.set("spark.local.dir",temp_dir)

    sparkConf.set("spark.ui.port","7171")
    sparkConf.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError")

    #sparkConf.set("spark.deploy.recoveryMode","FILESYSTEM")
    #sparkConf.set("spark.deploy.recoveryDirectory",recoveryDir)

    sc = SparkContext(conf=sparkConf)
    parallelism = None

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



    #logTimeIntervalWithMsg ("meta_unpickled_size: {}".format(meta_unpickled.count()))

    text_basename =  os.path.splitext(text_path)[0] if text_path else None
    text_collection_name = os.path.split(text_basename)[1] if text_basename else None
    print("text_path {} text_collection_name: {}".format(text_path,text_collection_name))
    if text_basename:
        vector_pickle = "{}/{}/vector_pickle/{}".format(pickle_dir,text_basename,vector_size)
    elif pickle_name:
        vector_pickle = "{}/data/{}/vector_pickle/{}".format(pickle_dir,pickle_name,vector_size)

    if os.path.exists(vector_pickle) and delete_files:
        shutil.rmtree(vector_pickle)
    if os.path.exists(os.path.join(vector_pickle,"_SUCCESS")):
        number_of_input_files = 0
        print ("already pickled: {}".format(vector_pickle))
        vectors = unpickle(sc,vector_pickle)
    else:
        number_of_input_files = numberOfInputFiles(text_path) if text_path else 0
        logTimeIntervalWithMsg ("input files: {} ".format(number_of_input_files)) #161

        if filter_files:
            logTimeIntervalWithMsg("filtering files...")
            regex_filters = regexFilters()
        else:
            logTimeIntervalWithMsg("not filtering files...")
            regex_filters=None

        rx_id = idRegex()
        rx_header = headerShortRegex()
        rx_footer = footerRegex()
        rx_encoding = encodingRegex()
        rx_body_text = bodyTextRegex()
        rx_smallprint = endOfSmallPrintRegex()
        batch_size=batch_processing
        if batch_processing > 0 and pickle_name is None:

            print ("batch_size {}".format(batch_size))
            input_files = arrayOfInputFiles(text_path,max_file_size)
            number_of_input_files = len(input_files)
            rdd_batch = sc.parallelize([])
            wcpf_pickle_names = []
            wfpd_pickle_names = []


            counter = 0
            batch_number = 0
            for file in input_files:
                print counter,
                batch_exists = 0
                wcpf_pickle_name = "{}/{}/wcpf/{}_{:05d}".format(pickle_dir,text_basename,batch_size,batch_number)
                wfpd_pickle_name = "{}/{}/wfpd/{}_{:05d}".format(pickle_dir,text_basename,batch_size,batch_number)

                #print("counter: {} batch: {} names:{}".format(counter,batch_number,wcpf_pickle_names))
                if batch_start > batch_number:
                    counter += 1

                else:
                    if os.path.exists(os.path.join(wcpf_pickle_name,"_SUCCESS")):
                        #print ("already pickled: {}".format(wcpf_pickle_name))
                        if os.path.exists(os.path.join(wfpd_pickle_name,"_SUCCESS")):
                            print '¢',

                            #print ("already pickled: {}".format(wfpd_pickle_name))
                        else:
                            #rebuild wfpd from wcpf
                            wf_per_doc_rdd = wordFreqPerDoc(unpickle(sc,wcpf_pickle_name))
                            wfpd_pickle_names.append(pickle(wf_per_doc_rdd,wfpd_pickle_name,delete_files))
                            print '¶',
                        batch_exists = 1
                        counter += 1


                    if not batch_exists:
                        print '+',

                        rdd = rddOfSingleTextFileProcessedByFile(file,parallelism) if not batch_exists else None
                        rdd = rddWithHeadersRemovedIndexedByIDBatch(rdd, rx_id, rx_body_text,rx_header,rx_smallprint,rx_footer,rx_encoding)
                        if rdd:
                            rdd_batch = rdd_batch.union(rdd) if rdd_batch else rdd
                            counter += 1
                            #print ("c+s {}".format(counter%batch_size))

            #else: print ("rdd is None: {}".format(file))
                if counter%batch_size == 0:
                    #print("counter%batch_size == 0")
                    if batch_start <= batch_number:
                        wcpf_pickle_names.append(wcpf_pickle_name)
                        wfpd_pickle_names.append(wfpd_pickle_name)

                        if not batch_exists:
                            word_count_per_file_rdd = processRDD(rdd_batch,stop_list_path,parallelism).cache()
                            wf_per_doc_rdd = wordFreqPerDoc(word_count_per_file_rdd,parallelism)
                            pickle(word_count_per_file_rdd,wcpf_pickle_name,delete_files)
                            pickle(wf_per_doc_rdd,wfpd_pickle_name,delete_files)

                    batch_number +=1
                    rdd = sc.parallelize([])
                    wcpf_pickle_name = None
                    wfpd_pickle_name = None
                    rdd_batch = None
            #last incomplete batch
            if wcpf_pickle_name:
                wcpf_pickle_names.append(wcpf_pickle_name)
            if wfpd_pickle_name:
                wfpd_pickle_names.append(wfpd_pickle_name)
                #print("counter: {} batch: {} names:{}".format(counter,batch_number,wcpf_pickle_names))
            if rdd_batch and not batch_exists:
                #print("\n\nlast batch rdd_batch: {}\n\n".format(rdd_batch.collect()))
                word_count_per_file_rdd = processRDD(rdd_batch,stop_list_path,parallelism).cache()
                wf_per_doc_rdd = wordFreqPerDoc(word_count_per_file_rdd,parallelism)
                pickle(word_count_per_file_rdd,wcpf_pickle_name,delete_files)
                pickle(wf_per_doc_rdd,wfpd_pickle_name,delete_files)
                #print ("\npicklenames:{}".format(pickle_names))


            number_of_documents = counter

            #print ("\n\nunpickled_rdd:{}".format(unpickled_rdd.take(1)))

        elif  pickle_name is None:
            print ("file processing")

            # file-by-file processing- faster but cannot filter for maximum file sizes
            rx_body_text = bodyTextRegex()
            rx_footer = footerRegex()
            rx_encoding = encodingRegex()
            rdd = rddOfWholeTextFileRDDs(text_path)
            #print ("rddOfWholeTextFileRDDs take(1):\n{}".format(rdd.take(1)))
            rdd = rddWithHeadersRemovedIndexedByID(rdd,max_file_size, rx_id, rx_body_text,rx_header,rx_smallprint,rx_footer,rx_encoding, regex_filters)
            #print ("rddWithHeadersRemovedIndexedByID take(1):\n{}".format(rdd.take(1)))
            # remove duplicates is implemented with this reduceByKey method.
            # our input rdd is (id,(0,file),(id(1,file))... where 2 is best encoding (utf8) and 0 is worst(iso)
            rdd = rdd.reduceByKey(lambda x,y:  x if x[0] > y[0] else y)\
                                         .map(lambda x:(x[0],x[1][1]))
            #print ("rddreduced take(1):\n{}".format(rdd.take(1)))

        logTimeIntervalWithMsg ("about to start processRDD") #161


        word_count_per_file_rdd = None
        word_count_per_file_pickle = None
        wf_per_doc = None
        wf_per_doc_pickle = None
        idf_rdd = None
        wfidf_rdd = None

        if batch_start > 0:
            print ("batch processing completed from {}".format(batch_start))
            end_s_time = time()
            runtime_s = end_s_time - start_s_time
            filePrint("\ntotal running time:{}".format(runtime_s))
            filePrint("ended run at {}\n".format(timestring()))
            exit()


        if batch_processing > 0:
            word_count_per_file_rdd = sc.parallelize([])
            if pickle_name:
                wcpf_dir = "{}/data/{}/wcpf/".format(pickle_dir,pickle_name)
                wcpf_pickle_names = pickleNames(wcpf_dir,batch_size)
                wfpd_dir = "{}/data/{}/wfpd/".format(pickle_dir,pickle_name)
                wfpd_pickle_names = pickleNames(wfpd_dir,batch_size)

            for name in wcpf_pickle_names:
                #print ("\nwcpfPickleName: \n{}".format(name))
                unpickled_rdd = unpickle(sc,name)
                #print ("\nunpickled_rdd: \n{}".format(unpickled_rdd.collect()))
                word_count_per_file_rdd = word_count_per_file_rdd.union(unpickled_rdd)
                #print ("\nword_count_per_file_rdd: \n{}".format(word_count_per_file_rdd.collect()))

            idf_rdd = idf(word_count_per_file_rdd,number_of_documents,parallelism)

            wf_per_doc_rdd = sc.parallelize([])
            for name in wfpd_pickle_names:
                #print ("\nwfpd_pickle_name: \n{}".format(name))
                unpickled_rdd = unpickle(sc,name)
                #print ("\nunpickled_rdd: \n{}".format(unpickled_rdd.collect()))
                wf_per_doc_rdd = wf_per_doc_rdd.union(unpickled_rdd)
                #print ("\nwf_per_doc_rdd: ")
                #pprint (wf_per_doc_rdd.sortByKey().collect())

            wf_per_doc_rdd = wf_per_doc_rdd.reduceByKey(add,parallelism)
            #print ("\nreduced wf_per_doc_rdd: ")
            #pprint (wf_per_doc_rdd.sortByKey().take(5))

            wfidf_rdd = wfidfFromJoining(idf_rdd,wf_per_doc_rdd,parallelism)



        else:
            word_count_per_file_rdd = processRDD(rdd,stop_list_path,parallelism).persist(StorageLevel.MEMORY_ONLY)
            wf_per_doc = wordFreqPerDoc(word_count_per_file_rdd)
            #print ("\nwf_per_doc_rdd: ")
            #pprint (wf_per_doc.sortByKey().take(5))
            logTimeIntervalWithMsg("counting the docs...")
            number_of_documents = word_count_per_file_rdd.count() if number_of_documents is None else number_of_documents
            logTimeIntervalWithMsg("idf...")
            idf_rdd = idf(word_count_per_file_rdd,number_of_documents,parallelism)
            wfidf_rdd = wfidfFromJoining(idf_rdd,wf_per_doc,parallelism)

        vectors = vectorise(wfidf_rdd,vector_size)

        vector_pickle = pickle(vectors,vector_pickle)

        #keys = vectors.keys().collect()
        #print (keys)
    logTimeIntervalWithMsg ("\ninput files: {} found texts: {}"
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



    meta_rdd = unpickled(sc,meta_pickle)
    print ("metapickle count:{}".format(meta_rdd.count()))
    frequent_subjects = arrayOfFrequentSubjects(meta_rdd)

    for subject in frequent_subjects:
        print("subject: {}".format(subject))




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




