from argparse import ArgumentParser
import happybase as db
from time import sleep, time
import csv


def curr_millitime():
    return int(time() * 1000)

class HbaseTable(object):
    def __init__(self, server, table_name):
        self.server = server
        self.table_name = table_name
        self.conn = None
        self.table = None
                
    def connect_with_retry(self, retries=10, retry_delay=10):
        attempts = 0
        while attempts < retries:
            try:
                attempts += 1
                print("Trying to connect to %s : %s" % (self.server, self.table_name))
                self.conn = db.Connection(self.server)
                self.table = self.conn.table(self.table_name)
                return True
            except Exception as e:
                print("Failed ... %s" % e)
                print("Sleeping ")
                sleep(retry_delay)
                continue

    def as_stream(self, row_start='', retry_delay=10, log_delay=2000, limit=None):
        ended = False
        count = 0
        st = curr_millitime()
        while not ended:
            try:
                self.connect_with_retry()
                for key, data in self.table.scan(row_start=row_start):
                    row_start = key
                    count += 1
                    yield key, data
                    if curr_millitime() - st > log_delay:
                        st = curr_millitime()
                        print("%d :: Count %d" % (st, count))
                    if limit and limit >= count:
                        break
                ended = True
            except Exception as e:
                print("Error %s, sleeping for a while before retry" % (e))
                sleep(retry_delay)
                continue
    
def dump_as_csv(stream, out_file, cols):
    count = 0
    with open(out_file, 'ab', 1) as out:
        out = csv.writer(out)
        for key, data in stream:
            row = [key]
            for c in cols:
                row.append(data.get(c))
            out.writerow(row)
            count += 1
    print("Appended %d docs to %s file" % (count, out_file))
    return count

if  __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--server", default="10.1.94.57", help="hbase server host")
    parser.add_argument("-o", "--out", help="output file path", required=True)
    parser.add_argument("-c", "--cols", default="info:s3_url", help="output column names (index key is included by default as first entry). Example: col1,col2")
    parser.add_argument("-t", "--table", default="escorts_images_sha1_infos", help="table name")
    parser.add_argument("-r", "--row_start", default="", help="Start row key. ignore to read from start.")
    parser.add_argument("-l", "--limit", default=None, help="Limit number of rows. Ignore to dump all", type=int)
    args = vars(parser.parse_args())
    print(args)
    cols = args['cols'].split(",")
    assert cols
    recs = HbaseTable(args['server'], args['table']).as_stream(args['row_start'], limit=args['limit'])
    dump_as_csv(recs, out_file=args['out'], cols=cols)
    print("Done")
