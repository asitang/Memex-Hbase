import happybase as db

connection = db.Connection('10.1.94.57')
outfile=open('/Users/asitangm/Desktop/JPL/files/ht_image_cdr_list','w')
table = connection.table('escorts_images_sha1_infos')
lastid=''

try:
    for key, data in table.scan():
        index=key
        link=table.row(key)['info:s3_url']
        lastid=key
        outfile.write(index+'\t'+link+'\n')
        outfile.flush()
except:
    connection.close()
    print 'error occured'

go=True

while(go):
    try:
        connection = db.Connection('10.1.94.57')
        table = connection.table('escorts_images_sha1_infos')
        for key, data in table.scan(row_start=lastid):
            index = key
            link = table.row(key)['info:s3_url']
            lastid = key
            outfile.write(index + '\t' + link + '\n')
            outfile.flush()
    except:
        connection.close()
        print 'error occured setting the index now to'+lastid
        continue

    go=False