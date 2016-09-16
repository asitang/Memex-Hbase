import json

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
            query['tika:'+key]=value.encode('utf8')

        query['ocr:content']=ocr.encode('utf8')

    #query=sjson.dumps(query)

    return query

