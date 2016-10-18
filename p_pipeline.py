import convertjson as queryfy
import multiprocessing
import createtable
import urllib
import runextractor as extract
import json
import happybase as db
import logging
import hbase
import os
import traceback
import datetime
from mpipe import UnorderedStage, Pipeline

class Bundle:
    def __init__(self,i):
        self.i=i
        self.filelist=''
        self.error=None
        self.logs=''
        self.starttime = datetime.datetime.now()

def fetch(i):
    try:
        folder='/mnt/HT_extractions/data'
        logs=''
        filelist = []
        bundle=Bundle(i)
        if (os.path.exists(folder + '_urllist/' + str(i)) == False):
            raise Exception('Partfile: ' + str(i) + ' ' + 'couldnt open folder or not found!')
        inputfile = open(folder + '_urllist/' + str(i), 'r')
        outfile = open(folder + '_imagelist/' + str(i), 'w')
        for line in inputfile:
            id = line.split(',')[0]
            url = line.split(',')[1]
            urllib.urlretrieve(url, folder + '_images/' + id + '.jpg')
            outfile.write(folder + '_images/' + id + '.jpg\n')
            filelist.append(folder + '_images/' + id + '.jpg')
            outfile.flush()
        outfile.close()
        bundle.filelist=filelist
    except Exception:
        error=traceback.format_exc()
        bundle.error=error

    return bundle

def extractandclean(bundle):
    if bundle.error == None:
        try:
            folder='/mnt/HT_extractions/data'
            extract.extract(folder + '_imagelist', folder + '_extracted', str(bundle.i))

            # remove the downloaded image files
            for file in bundle.filelist:
                if os.path.exists(file):
                    os.remove(file)
        except Exception:
            error = traceback.format_exc()
            bundle.error=error
    return bundle

def updatetable(bundle):
    # create a file with id and extraction and then send it to table
    # TODO: refresh connection
    # TODO: add timings
    folder = '/mnt/HT_extractions/data'
    logfile = open(folder + '_logs/' + str(bundle.i), 'w')
    if bundle.error == None:
        try:
            IP = '10.1.94.57'
            tablename = 'escorts_images_sha1_dev'
            extratedfile = open(folder + '_extracted/' + str(bundle.i), 'r')  # json dumped by parser indexer
            connection = db.Connection(IP)
            table = connection.table(tablename)

            linecount = 0
            for jsonstring in extratedfile:
                linecount += 1
                result = json.load(createtable.readablestring(jsonstring))
                uniquerowkey = result['id']
                query = queryfy.create_query_from_json(createtable.readablestring(jsonstring))
                table.put(uniquerowkey, query)

        except Exception:
            error = traceback.format_exc()
            bundle.error=error

    logfile.write('LOGS:'+bundle.logs+'\n'+'ERRORS:'+bundle.error+'\n'+'TIME TAKEN: '+str(datetime.datetime.now()-bundle.starttime))
    logfile.close()


if __name__ == '__main__':

    stage1 = UnorderedStage(fetch, 4)
    stage2 = UnorderedStage(extractandclean, 4)
    stage3 = UnorderedStage(updatetable, 4)
    stage1.link(stage2)
    stage2.link(stage3)
    pipe = Pipeline(stage1)


    for number in range(0,8):
        pipe.put(number)

    pipe.put(None)

