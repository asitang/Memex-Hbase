import json
import simplejson as sjson

def create_query_from_json(jsonstring):
    result=json.load(jsonstring)
    tika=result['metadata']
    ocr=result['content']
    query=dict()


    for key in tika:
        value=tika[key]
        if key not in 'x-parsed-by_ts_md':
            if ':' in key:
                key=key.replace(':','__')
            query['tika:'+key]=value

        query['ocr:content']=ocr

    #query=sjson.dumps(query)

    return query

