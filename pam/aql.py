import logging
from pam import database

def execute_aql(query, database_obj, show_query=False, bind_vars=None):
    """Execute AQL with bind_vars and return ArangoDB cursor
    
    :parameters:
    - `query`: AQL to Execute (string)
    - `database_obj`: database object in which to execute aql (Arango Database Object)
    - `show_query` (optional): whether to print full aql (boolean)
    - `bind_vars` (optional): bind variables to supply to AQL (dict)
    """

    temp = query
    if bind_vars:
        for var in bind_vars:
            if type(bind_vars[var]) == list:
                temp = temp.replace('@{}'.format(var), str(bind_vars[var]))
            else:
                temp = temp.replace('@{}'.format(var), "'{}'".format(str(bind_vars[var])))

    cursor = database_obj.aql.execute(query, bind_vars=bind_vars)

    if show_query:
        logging.warning(temp)

    logging.warning(str(cursor.statistics()))

    return cursor