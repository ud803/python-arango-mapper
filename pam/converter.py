import logging
import time
import uuid

from pam import collection
from pam import utils


def arango_converter(list_of_dict_data, database_obj, schemas, mapping_list):
    """ 
    """

    collections = {}
    
    #1. Loop mapping_list and initiate target collections & indices
    for mapping in mapping_list:
        print(mapping)
        coll_def = schemas[mapping]
        coll_name = coll_def['collection']
        _type, _type2 = coll_def['type']
        
        coll_obj = collection.create_and_get_collection(database_obj, coll_name, True if _type == 'edge' else False)

        collections[coll_name] = {}

        # Configuration for conditions
        if coll_def.get('condition'):
            cond = coll_def['condition']

            if cond.get('min_by'):
                for min_by_field in cond.get('min_by'):
                    coll_def['index'][('_key', min_by_field)] = False

            if cond.get('max_by'):
                for max_by_field in cond.get('max_by'):
                    coll_def['index'][('_key', max_by_field)] = False

            if cond.get('min_by') and coll_def.get('max_by'):
                coll_def['index'][('_key', min_by_field, max_by_field,)] = False
        
        # Index Initialization
        for index in coll_def['index']:
            print(index['field'])
            if index.get('ttl'):
                collection.add_ttl_index(coll_obj, index['field'], index['unique'], index['ttl'])
            else:
                collection.add_persistent_index(coll_obj, index['field'], index['unique'])

    step_2 = time.time()

    #2. 프레스토 커서를 순회하면서 스키마별로 업데이트할 도큐먼트를 분류함. 

    step_3 = time.time()
    for idx, row in enumerate(list_of_dict_data):
        if idx == 0:
            step_3 = time.time()
            logging.warning("...presto execution time : {} secs".format(str(round(step_3 - step_2))))
        
        for mapping in mapping_list:
            coll_def = schemas[mapping]

            doc = {}

            _type, _type2 = coll_def['type']

            ### _key, _from, _to Init
            if _type2 == 'unique_vertex':
                doc['_key'] = '_'.join([str(row[i]) for i in coll_def['unique_key']])
                if doc['_key'] == 'None':
                    continue

            elif _type2 == 'unique_edge_on_event':
                if not row.get(coll_def['_from']):
                    continue
                if not row.get(coll_def['_to']):
                    continue
                doc['_from'] = coll_def['_from_collection'] + '/' + row[coll_def['_from']]
                doc['_to'] = coll_def['_to_collection'] + '/' + row[coll_def['_to']]
                doc['_key'] = row[coll_def['_from']] + '_' + row[coll_def['_to']] + '_' + '_'.join([str(row[i]) for i in coll_def['unique_key']])

            elif _type2 == 'unique_edge_btw_vertices':
                if not row.get(coll_def['_from']):
                    continue
                if not row.get(coll_def['_to']):
                    continue
                doc['_from'] = coll_def['_from_collection'] + '/' + row[coll_def['_from']]
                doc['_to'] = coll_def['_to_collection'] + '/' + row[coll_def['_to']]
                doc['_key'] = row[coll_def['_from']] + '_' + row[coll_def['_to']]

            elif _type2 == 'unique_edge_from_vertex':
                if not row.get(coll_def['_from']):
                    continue
                if not row.get(coll_def['_to']):
                    continue
                doc['_key'] = row[coll_def['_from']]
                doc['_from'] = coll_def['_from_collection'] + '/' + row[coll_def['_from']]
                doc['_to'] = coll_def['_to_collection'] + '/' + row[coll_def['_to']]

            if doc['_key'] is None:
                continue
            
            for k in ['_key', '_from', '_to']:
                if doc.get(k):
                    doc[k] = doc[k].replace(' ', '_')

            if coll_def.get('condition'):
                doc['unique_identifier'] = doc['_key'] + str(uuid.uuid4())
            else:
                doc['unique_identifier'] = doc['_key']
            

            ### fields Init
            for field in coll_def['fields']:
                if row.get(coll_def['fields'][field]):
                    doc[field] = row[coll_def['fields'][field]]

            ### Add Doc to Collection
            collections[coll_def['collection']][doc['unique_identifier']] = doc

    step_4 = time.time()
    print("...cursor iter time : {} secs".format(str(round(step_4 - step_3))))

    #3. 각 스키마의 타입에 따라 기본 AQL을 세팅하고, CRUD 작업 실행
    tasks = []
    for mapping in mapping_list:
        coll_def = schemas[mapping]

        print(coll_def['collection'])

        base_query = ""

        if coll_def.get('condition'):
            cond = coll_def['condition']
            if cond.get('min_by'):
                for min_by_field in cond.get('min_by'):
                    base_query += """
                    '{min_by_field}' : doc.{min_by_field} < OLD.{min_by_field} ? doc.{min_by_field} : OLD.{min_by_field}""".format(min_by_field=min_by_field)
                    
                    for field in cond.get('min_by').get(min_by_field):
                        base_query += """,
                        '{field}' : doc.{min_by_field} < OLD.{min_by_field} ? doc.{field} : OLD.{field}
                        """.format(field=field, min_by_field=min_by_field)

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
                exclusive : true
            }}
            """
        if collections.get(coll_def['collection']):
            params = {
                'rows' : collections[coll_def['collection']],
                'op' : op,
                'collection' : coll_def['collection']
            }
            #celery 통해 작업 분할
            utils.arango_split_task(database_obj, target_aql, params)
            
            del collections[coll_def['collection']]
    
    step_5 = time.time()
    print("...Arango Op time : {} secs".format(str(round(step_5 - step_4, 3))))