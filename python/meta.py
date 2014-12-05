import os
import sys
import re
import xml.etree.ElementTree as ET
from time import time
from tempfile import NamedTemporaryFile
import shutil
from pyspark import SparkContext
from pyspark.conf import SparkConf



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

def printHelp():
    print ('''
        =Usage=


    ''')
    exit(-1)


def timeDeltas():
    global s_time
    global is_time
    ds_time = time()
    deltas_since_start = ds_time - s_time
    deltas_since_last = ds_time - is_time
    is_time = ds_time
    return {"time_since_start":deltas_since_start,'time_since_last':deltas_since_last}





def pickle(rdd, name=None,delete_files=0):

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

def filePrint(string):
    global g_filehandle
    if g_filehandle:
        g_filehandle.write("{}\n".format(string))
    print(string)

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

def arrayOfMetadataArrays(path, metadata=[]):
    count = 0
    (location, folders, files) = os.walk(path).next()
    for filename in files:
        filepath = os.path.join(location, filename)
        if filename != '.DS_Store':
            print str(len(metadata))+' ',
            #metadict['filename'] = filename
            tree = ET.parse(filepath)
            root = tree.getroot()
            for ebook in root:
                if re.search('ebook',ebook.tag):
                    ebook_id = ebookID(ebook)
                    subjects=[]
                    for subject in ebook:
                       if re.search('subject',subject.tag):
                              subjects.append(subjectTuple(subject))
                    metadata.append((ebook_id,subjects))
    for folder in folders:
         sub_path = os.path.join(location, folder)
         arrayOfMetadataArrays(sub_path,metadata)

    return metadata



def logTimeIntervalWithMsg(msg):
    if 1:
        time_deltas = timeDeltas()
        message = msg if msg else ""
        string = ":{0[time_since_start]:7,.3f} :{0[time_since_last]:7,.3f}  {1:}".format(time_deltas, message)
        filePrint(string)

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

    logfile = 'meta_py_log.txt'
    logfile = os.path.abspath(logfile)
    g_filehandle = open(logfile, 'a')


    parsed_args = parseArgs(sys.argv)
    if len(parsed_args) < 1:
        printHelp()
        exit(-1)


    cores = parsed_args['c'] if 'c' in parsed_args else 4
    mem = parsed_args['m'] if 'm' in parsed_args else 8
    parrellelismMultiplier = int(parsed_args['p']) if 'p' in parsed_args else 4
    meta_path = parsed_args['meta'] if 'meta' in parsed_args else '/data/extra/gutenberg/meta'
    pickle_name = parsed_args['name'] if 'name' in parsed_args else 'pickle'


    masterConfig = "local[4]"

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






    meta_pickle = os.path.join('data/_meta/_new/',pickle_name)

    if not os.path.exists(meta_path):
        print ("meta_path does not exist:{}".format(meta_path))
        exit(0)

    if os.path.exists(meta_pickle):
        print ("metadata already pickled")
        metadata = unpickle(sc,meta_pickle).take(101)
        logTimeIntervalWithMsg ("metalen: {}".format(len(metadata)))
        logTimeIntervalWithMsg ("metasample: {}".format(metadata[100]))
        exit(0)


    metadata = arrayOfMetadataArrays(meta_path)
    logTimeIntervalWithMsg ("metalen: {}".format(len(metadata)))
    logTimeIntervalWithMsg ("metasample: {}".format(metadata[100]))

    meta_pickle = pickle(sc.parallelize(metadata),meta_pickle)
    print ("meta_pickle: {}".format(meta_pickle))
    #meta_unpickled = unpickled(sc,meta_pickle)


