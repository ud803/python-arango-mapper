from pam import database


def create_and_get_collection(database_obj, collection_name, edge):
    """Returns collection object. If not exists, create a new one. 
    
    :parameters:
    - `database_obj`: database object in which to execute aql (Arango Database Object)
    - `collection_name`: collection name to create (string)
    - `edge` : which collection type to create, if True, makes edge, else makes vertex (boolean)
    """
        
    if not database_obj.has_collection(collection_name):
        database_obj.create_collection(collection_name, edge=edge)
    
    return database_obj.collection(collection_name)

    
def add_persistent_index(coll_obj, fields, unique, in_background=True):
    """Add persistent index to collection
    
    :parameters:
    - `coll_obj`: collection object in which to add index (Arango Collection Object)
    - `fields`: list of fields to make index (list of strings)
    - `unique`: whether field sets are unique (boolean)
    - `in_background`: whether to create index in background or not (boolean)
    """

    coll_obj.add_persistent_index(fields=fields, unique=unique, in_background=in_background)

    return


def add_ttl_index(coll_obj, field, unique, seconds, in_background=True):
    """Add ttl index to collection. 
    
    :parameters:
    - `coll_obj`: collection object in which to add index (Arango Collection Object)
    - `field`: ttl field name to create. raw data in ttl field should be datetime object (string)
    - `unique`: whether field sets are unique (boolean)
    - `seconds`: seconds until document expires (integer)
    - `in_background`: whether to create index in background or not (boolean)
    """

    coll_obj.add_ttl_index(fields=[field], expiry_time=seconds, unique=unique, in_background=in_background)

    return
