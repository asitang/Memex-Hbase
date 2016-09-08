import happybase as db
import convertjson as queryfy

def deleteTable(IP, tablename):
    connection = db.Connection(IP)
    table=connection.table(tablename)
    connection.delete_table(tablename,disable=True)


#creates a table with 'tika' and 'ocr' column families
def createMyTable(IP,tablename):
    connection = db.Connection(IP)
    connection.create_table(
        tablename,
        {'tika': dict(),
         'ocr': dict(),
         }
    )
    connection.close()


def updateTable(IP,tablename,uniquerowkey,query):
    connection = db.Connection(IP)
    table=connection.table(tablename)
    print query
    table.put(uniquerowkey,query) #put-eg: table.put('1234',{"tika:cf1":"value1","ocr:cf2":"val2"})



def printTable(IP,tablename):
    connection = db.Connection(IP)
    table=connection.table(tablename)

    for key, data in table.scan():
        print data


class readablestring(object):
    def __init__(self, string):
        self.string=string

    def read(self):
        return self.string

if  __name__ == '__main__':
    IP='10.1.94.57'
    tablename='escorts_images_sha1_dev'
    uniquerowkey=1
    deleteTable(IP,tablename)
    createMyTable(IP, tablename)
    jsonfile = open('/Users/asitangm/Desktop/out6.json', 'r') #json dumped by parser indexer

    for jsonstring in jsonfile:
        uniquerowkey+=1
        query = queryfy.create_query_from_json(readablestring(jsonstring))
        updateTable(IP,tablename,str(uniquerowkey),query)


