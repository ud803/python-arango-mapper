import logging
import time

from pam import collection

def arango_converter(presto_conn, target_database, schema):
    """Presto 결과 커서를 받아서 주어진 schema에 따라 ArangoDB에 적재함

    Keyword arguments:
    presto_conn -- Presto 결과값이 저장된 커서
    target_database -- 대상 데이터베이스(업체명)
    schema -- 관계형 데이터를 ArangoDB에 맵핑하는 스키마
    """

    collections = {}

    #1. 스키마를 순회하면서 대상 컬렉션과 인덱스 생성
    for vertex_edge in schema:
        property = ARANGO_SCHEMA[vertex_edge]
        _type, _type2 = property['type']
        
        collection.create_and_get_collection(target_database, property['collection'], _type)

        collections[property['collection']] = {}

        ## Min Max Field configigure
        if property.get('condition'):
            cond = property['condition']
            if cond.get('min'):
                property['index'][('_key', 'min_time',)] = False
                property['fields']['min_time'] = 'rtk_timestamp'

            if cond.get('max'):
                property['index'][('_key', 'max_time',)] = False
                property['fields']['max_time'] = 'rtk_timestamp'

            if cond.get('min') and property.get('max'):
                property['index'][('_key', 'min_time', 'max_time',)] = False
        
        
        ## Index Initialization
        for index in property['index']:
            ## Ensure TTL Indexes - 45 days
            if index[0] == 'createdAt':
                collection.add_ttl_index(target_database, property['collection'], 3600 * 24 * 45)
            else:
                collection.add_persistent_index(target_database, property['collection'], index, property['index'][index])

    step_2 = time.time()
    #2. 프레스토 커서를 순회하면서 스키마별로 업데이트할 도큐먼트를 분류함. 
    fields = [i[0] for i in presto_conn.description]

    step_3 = time.time()
    for idx, row in enumerate(presto_conn):
        if idx == 0:
            step_3 = time.time()
            logging.warning("...presto execution time : {} secs".format(str(round(step_3 - step_2))))

        row = {k:v for k, v in zip(fields, row)}
        
        for vertex_edge in schema:
            property = ARANGO_SCHEMA[vertex_edge]

            doc = {}

            _type, _type2 = property['type']

            ### _key, _from, _to Init
            if _type2 == 'unique_vertex':
                doc['_key'] = '_'.join([str(row[i]) for i in property['unique_key']])
                if doc['_key'] == 'None':
                    continue
            elif _type2 == 'unique_edge_on_event':
                if not row.get(property['_from']):
                    continue
                if not row.get(property['_to']):
                    continue
                doc['_from'] = property['_from_collection'] + '/' + row[property['_from']]
                doc['_to'] = property['_to_collection'] + '/' + row[property['_to']]
                doc['_key'] = row[property['_from']] + '_' + row[property['_to']] + '_' + '_'.join([str(row[i]) for i in property['unique_key']])
            elif _type2 == 'unique_edge_btw_vertices':
                if not row.get(property['_from']):
                    continue
                if not row.get(property['_to']):
                    continue
                doc['_from'] = property['_from_collection'] + '/' + row[property['_from']]
                doc['_to'] = property['_to_collection'] + '/' + row[property['_to']]
                doc['_key'] = row[property['_from']] + '_' + row[property['_to']]

            elif _type2 == 'unique_edge_from_vertex':
                if not row.get(property['_from']):
                    continue
                if not row.get(property['_to']):
                    continue
                doc['_key'] = row[property['_from']]
                doc['_from'] = property['_from_collection'] + '/' + row[property['_from']]

                doc['_to'] = property['_to_collection'] + '/' + row[property['_to']]
            if doc['_key'] is None:
                continue
                
            ### fields Init
            for field in property['fields']:
                if row.get(property['fields'][field]):
                    doc[field] = row[property['fields'][field]]

            ### Add Creation Date
            doc['createdAt'] = round(time.time())

            ### Add Doc to Collection
            collections[property['collection']][doc['_key']] = doc

    step_4 = time.time()
    print("...cursor iter time : {} secs".format(str(round(step_4 - step_3))))

    #3. 각 스키마의 타입에 따라 기본 AQL을 세팅하고, CRUD 작업 실행
    tasks = []
    for vertex_edge in schema:
        property = ARANGO_SCHEMA[vertex_edge]

        print(property['collection'])

        base_query = ""

        if property.get('condition'):
            cond = property['condition']
            if cond.get('min'):
                base_query += """
                'min_time' : doc.min_time < OLD.min_time ? doc.min_time : OLD.min_time"""
                for field in cond.get('min'):
                    base_query += """,
                    '{field}' : doc.min_time < OLD.min_time ? doc.{field} : OLD.{field}
                    """.format(field=field)

            if cond.get('max'):
                if base_query:
                    base_query += ","
                base_query += """
                'max_time' : doc.max_time > OLD.max_time ? doc.max_time : OLD.max_time"""
                for field in cond.get('max'):
                    base_query += """,
                    '{field}' : doc.max_time > OLD.max_time ? doc.{field} : OLD.{field}
                    """.format(field=field)
            if cond.get('if'):
                if base_query:
                    base_query += ","
                base_query += cond.get('if')

        if base_query:
            op = """
            UPDATE {{
                {}
            }}
            """.format(base_query)

            target_aql = """
            FOR doc in {rows}

            UPSERT {{_key : doc._key}}
                INSERT doc
                {op}

            IN {collection} 

            OPTIONS {{
                ignoreErrors : true,
                exclusive : true
            }}"""

        else:
            op = 'INSERT doc'

            target_aql = """
            FOR doc in {rows}

            {op}

            IN {collection} 

            OPTIONS {{
                overwriteMode : "ignore",
                ignoreErrors : true,
                exclusive : true
            }}
            """
        if collections.get(property['collection']):
            params = {
                'rows' : collections[property['collection']],
                'op' : op,
                'collection' : property['collection']
            }
            #celery 통해 작업 분할
            arango_split_task(target_database, target_aql, params)
            
            del collections[property['collection']]
    
    step_5 = time.time()
    print("...Arango Op time : {} secs".format(str(round(step_5 - step_4, 3))))