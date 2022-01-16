import logging
import time
import uuid

from pam import collection
from pam import utils


def arango_converter(list_of_dict_data, database_obj, schemas, mapping_list, num_split=1000, show_query=False):
    """Get list of dict data, and map those data into ArangoDB according to the schemas provided. mapping_list should also be provided.

    :parameters:
    - `list_of_dict_data`: list of dictionaries that will be used as raw data (list of dicts)
    - `database_obj`: database object in which to execute aql (Arango Database Object)
    - `schemas`: schemas used to map raw data into Graph objects. Read the instructions in the github or PyPI. (dict of dicts)
    - `mapping_list`: list of schema to use. Used to use only specified schemas. (list of strings)
    - `num_split`: size of batch operation. recommended to use default size (int)
    - `show_query`: whether to show final AQL query used to upload data into ArangoDB. Used to debug queries. Use with small chunks. (boolean)
    """

    collections = {}
    
    #1. Loop mapping_list and initiate target collections & indices
    for mapping in mapping_list:
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
                    coll_def['index'].append({'field': ('_key', min_by_field), 'unique': False, 'ttl': False})

            if cond.get('max_by'):
                for max_by_field in cond.get('max_by'):
                    coll_def['index'].append({'field': ('_key', max_by_field), 'unique': False, 'ttl': False})

            if cond.get('min_by') and coll_def.get('max_by'):
                coll_def['index'].append({'field': ('_key', min_by_field, max_by_field,), 'unique': False, 'ttl': False})
        
        # Index Initialization
        for index in coll_def['index']:
            if index.get('ttl'):
                collection.add_ttl_index(coll_obj, index['field'], index['unique'], index['ttl'])
            else:
                collection.add_persistent_index(coll_obj, index['field'], index['unique'])

    step_2 = time.time()

    #2. Loop Data 

    step_3 = time.time()
    for idx, row in enumerate(list_of_dict_data):
        if idx == 0:
            step_3 = time.time()
            logging.warning("...init time : {} secs".format(str(round(step_3 - step_2))))
        
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
                for field in coll_def['_from']:
                    if not row.get(field):
                        continue
                for field in coll_def['_to']:
                    if not row.get(field):
                        continue

                doc['_from'] = coll_def['_from_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_from']])
                doc['_to'] = coll_def['_to_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_to']])
                doc['_key'] = '_'.join([str(row[i]) for i in coll_def['_from']]) + '_' + '_'.join([str(row[i]) for i in coll_def['_to']]) + '_' + '_'.join([str(row[i]) for i in coll_def['unique_key']])

            elif _type2 == 'unique_edge_btw_vertices':
                for field in coll_def['_from']:
                    if not row.get(field):
                        continue
                for field in coll_def['_to']:
                    if not row.get(field):
                        continue
                doc['_from'] = coll_def['_from_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_from']])
                doc['_to'] = coll_def['_to_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_to']])
                doc['_key'] = '_'.join([str(row[i]) for i in coll_def['_from']]) + '_' + '_'.join([str(row[i]) for i in coll_def['_to']])

            elif _type2 == 'unique_edge_from_vertex':
                for field in coll_def['_from']:
                    if not row.get(field):
                        continue
                for field in coll_def['_to']:
                    if not row.get(field):
                        continue
                doc['_key'] = '_'.join([str(row[i]) for i in coll_def['_from']])
                doc['_from'] = coll_def['_from_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_from']])
                doc['_to'] = coll_def['_to_collection'] + '/' + '_'.join([str(row[i]) for i in coll_def['_to']])

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
    logging.warning("...cursor iter time : {} secs".format(str(round(step_4 - step_3))))

    #3. Set Basic AQL
    for mapping in mapping_list:
        logging.warning(mapping)

        coll_def = schemas[mapping]

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

            if cond.get('max_by'):
                for max_by_field in cond.get('max_by'):
                    base_query += """
                    '{max_by_field}' : doc.{max_by_field} > OLD.{max_by_field} ? doc.{max_by_field} : OLD.{max_by_field}""".format(max_by_field=max_by_field)
                    
                    for field in cond.get('max_by').get(max_by_field):
                        base_query += """,
                        '{field}' : doc.{max_by_field} > OLD.{max_by_field} ? doc.{field} : OLD.{field}
                        """.format(field=field, max_by_field=max_by_field)

            if cond.get('if'):
                for if_clause in cond.get('if'):
                    if base_query:
                        base_query += ","
                    base_query += if_clause

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
                exclusive : true,
                ignoreErrors : true
            }}"""

        else:
            op = 'INSERT doc'

            target_aql = """
            FOR doc in {rows}

            {op}

            IN {collection} 

            OPTIONS {{
                overwriteMode : "ignore",
                exclusive : true,
                ignoreErrors : true

            }}
            """
        if collections.get(coll_def['collection']):
            # remove unique_identifier
            [v.pop('unique_identifier') for k, v in collections[coll_def['collection']].items()]

            params = {
                'rows' : collections[coll_def['collection']],
                'op' : op,
                'collection' : coll_def['collection']
            }
            utils.arango_split_task(database_obj, target_aql, params, num_split=num_split, show_query=show_query)
            
            del collections[coll_def['collection']]

    step_5 = time.time()
    logging.warning("...Arango Op time : {} secs".format(str(round(step_5 - step_4, 3))))