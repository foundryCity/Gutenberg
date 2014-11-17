

import sys, os
import pprint
import re
import numpy
from operator import add

from pyspark import SparkContext

stopwords = ['some','jawbone','answerest','commencement']
romeo = ['romeo']
juliet = ['juliet']


def rempluralregex(string):
	"remove s from plurals"
	singular = re.sub('(\w+)s$', 'found plural: \0', string )
	return singular

def remPlural( word ):
    word = word.lower()
    if word.endswith('s'):
        return word[:-1]
    else:
        return word

def maxCount (list):
    currentMax = 0
    for i in list:
        currentMax = max(currentMax,i[1])
    return currentMax



romeoandjuliet = ['romeo', 'juliet']
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <file> <stoplist>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")

    print "\n 1 After you read in multiple the files with wholeTextFiles,\
    \n get the number of filesand store it in a variable fileNum.\n"

    #raw_input("Press Enter to continue...")


    textfiles = sc.wholeTextFiles(sys.argv[1], 1)
    fileNum = textfiles.count()

    print "\n 2 Load a list of English stopwords from the provided file stopwords en.txt. \
    \n The words are separated by commas in the file. The file should be added as\
    \n an additional argument to spark submit. Tokenise the file content, store it\
    \n in a Python list and use it to filter out stop words from the text file\n"

    stopfile = sc.textFile(sys.argv[2],1)
    stoplist = stopfile.flatMap (lambda x: re.split('\W+',x)).collect()

    #raw_input("Press Enter to continue...")

    #var = raw_input("press any key to continue")

    print "\n   (file, word) tuples excluding words in stoplist  \n"
   # count1 = textfiles.flatMap(lambda (f,x):([(f,remPlural(x)) for x in re.split('\W+',x)]))

    count1 = textfiles.flatMap(lambda (f,x):([(f[f.rfind("/")+1:],remPlural(x)) for x in re.split('\W+',x)]))


   # count1 = textfiles.flatMap(lambda (f,x):(f[f.rfind("/")+1:],remPlural(x)) for x in re.split('\W+',x))

    count1 = count1.filter (lambda x : x[1] not in stoplist  )
    pprint.pprint(count1.collect())

    print "\n   ((file,word),1) tuples\n"

    count2 = count1.map(lambda (x):((x[0],x[1]),  1))
    #pprint.pprint(count2.takeSample(True,4,0))
    pprint.pprint(count2.collect())


    print "\n   reduceByKey(add) - should give ((file,word),n) where n is wordcount \
           \n   the add in reduceByKey adds (sums) n's\n"

    count3 = count2.reduceByKey(add)
    pprint.pprint(count3.takeSample(True,4,0))
    print "\n   rearrange tuples as (file, [(word, count)])\n"
    count4 = count3.map (lambda (a,b) : (a[0],[(a[1],b)]))
    print "count4\n"
    pprint.pprint(count4.takeSample(True,4,0))
    print "\n   reduceByKey - should give (file, [(word,count),(word,count)...]) \
           \n   the add in reduceByKey adds (concatenates) [(word,count)] lists\n"

    count5 = count4.reduceByKey(add)
    print ("count5: {}".format(count5.take(1)))
    print "\n 3 After creating the list of (word,count) tuples per file,\
    \n calculate the maximal term frequency per file using map().\
    \n NB use SPARK sortBy here instead of maxCount"
   # count6 = count5.map (lambda x: (x[0],maxCount(x[1])) )

    count6 = count5.map (lambda x: (x[0], max(x[1], key = lambda y: y[1])))

    print "count6\n"
    pprint.pprint(count6.take(4))

    print "\n 4 Use the list of (file, [(word,count) ... (word,count)]) again to ,\
    \n create a new RDD by reorganising the tuples and lists such that\
    \n the words are the keys and count the occurrences of words per file\
    \n using map(). \n"

    count7 = count4.map (lambda (a,b) : (b[0][0],[((a,b[0][1]))]))

    print "count7\n"
    pprint.pprint(count7.takeSample(True,4,0))

    print "\nApply reduceByKey() to get the lists of term frequencies\n"
    count8 = count7.reduceByKey(add)

    print "count8\n"
    pprint.pprint(count8.takeSample(True,4,0))




    print "\n 5 With the output of 5) use map to create tuples of this form:\
    \n (word,nD, [(file,count),... , (file,count)]) (for calculating IDF).\n"

    count9 = count8.map (lambda (a,b) : (a,len(b),b))

    print "count9\n"
    pprint.pprint(count9.takeSample(True,4,0))

print "\n 6 Read chapter 2 up to section 2.3 (inclusive) of Lescovec et al (2014) \" \
 \n \"Mining of Massive Datasets\", and work out the answers to exercise 2.3.1 on page 40.\n"

sc.stop()
