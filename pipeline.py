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

def split(outfolder,infile,split=500):
    filein=open(infile,'r')
    lines=filein.readlines()
    length= len(lines)



    if length%split==0:
        parts=length/split
    else:
        parts=(length/split)+1

    diff=length%split


    start=0
    for i in range (0,parts):
        if i==parts-1:
            end=start+split+diff
        else:
            end=start+split

        fileout=open(outfolder+'/'+str(i),'w')
        for j in range(start,end):
            fileout.write(lines[j])
            fileout.flush()
        fileout.close()
        start=end



def create_part_files(folder):
    logger = logging.getLogger('HTextractions')
    hdlr = logging.FileHandler(folder + '_logs/all.log')
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    try:
        #get all the ids and links from hbase
        recs=hbase.HbaseTable('10.1.94.57', 'escorts_images_sha1_infos').as_stream('', limit=None)
        hbase.dump_as_csv(recs, out_file=folder+'/all.txt', cols=['info:s3_url'])

    except Exception:
        logger.warn('ERROR: ' + traceback.format_exc())

    try:
        #divide all into part files
        split(folder,folder+'/all.txt')

    except Exception:
        logger.warn('ERROR: ' + traceback.format_exc())


def pipe(folder,i):
    logger = logging.getLogger('HTextractions'+str(i))
    hdlr = logging.FileHandler(folder+'_logs/'+str(i)+'.log')
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)
    process = str(i)
    logger.warn('Starting Process: '+process)


    while True:

        try:
            #open a part file id, url

            if (os.path.exists(folder+'_urllist/'+str(i))==False):
                break
            logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' ' + 'opening the part file for id and url')
            inputfile=open(folder+'_urllist/'+str(i),'r')
            outfile=open(folder+'_imagelist/'+str(i),'w')




            #fetch the images, name them as their id and create a part file of local file locations
            logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' ' + 'fetching image urls now..')
            filelist=[]
            for line in inputfile:
                id=line.split(',')[0]
                url=line.split(',')[1]
                logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' ' + 'url: '+url)
                urllib.urlretrieve(url, folder+'_images/'+id+'.jpg')
                outfile.write(folder+'_images/'+id+'.jpg\n')
                filelist.append(folder+'_images/'+id+'.jpg')
                outfile.flush()

            # run extraction by giving the list of files.
            logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' '+'Running extractions on the url list')
            outfile.close()
            extract.extract(folder+'_imagelist',folder+'_extracted',str(i))

            #remove the downloaded image files
            for file in filelist:
                os.remove(file)


            #create a file with id and extraction and then send it to table
            #TODO: refresh connection
            #TODO: add timings
            logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' ' + 'Connecting to the table')
            IP = '10.1.94.57'
            tablename = 'escorts_images_sha1_dev'
            extratedfile = open(folder+'_extracted/'+str(i),'r')  # json dumped by parser indexer
            connection = db.Connection(IP)
            table = connection.table(tablename)

            linecount=0
            for jsonstring in extratedfile:
                linecount+=1
                logger.warn('Process ' + process + ': Partfile: ' + str(i) + ' ' + 'uploading extracted jsons in line: '+str(linecount))
                result = json.load(createtable.readablestring(jsonstring))
                uniquerowkey=result['id']
                query = queryfy.create_query_from_json(createtable.readablestring(jsonstring))
                table.put(uniquerowkey, query)

        except Exception:
            logger.warn('ERROR: Process ' + process + ': Partfile: ' + str(i) + ' ' + traceback.format_exc())


        i+=8

if __name__ == '__main__':

    os.mkdir('/mnt/HT_extractions/data_urllist')
    os.mkdir('/mnt/HT_extractions/data_imagelist')
    os.mkdir('/mnt/HT_extractions/data_extracted')
    os.mkdir('/mnt/HT_extractions/data_logs')
    os.mkdir('/mnt/HT_extractions/data_images')
    create_part_files('/mnt/HT_extractions/data_urllist')
    jobs = []
    for i in range(0,8):
        p = multiprocessing.Process(target=pipe, args=('/mnt/HT_extractions/data',i,))
        jobs.append(p)
        p.start()