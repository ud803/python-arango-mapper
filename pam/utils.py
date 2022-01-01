import itertools
from pam import aql, database


def dict_spliter(target_dict, num_split=1000):
    """Split dictionary into chunks and return as list
    
    :parameters:
    - `target_dict`: dictionary to split (dict)
    - `num_split` (optional): chunk size to split (integer)
    """
    
    result = []

    for i in range(len(target_dict)//n +1):
        result.append(itertools.islice(target_dict.items(), i * n, (i+1) * n))

    return result


def arango_split_task(database_name, partial_query, params, num_split=1000):
    """Split AQLs into chunks in order to adjust performance. 1000 worked best for me
    It is a system function used in converter. Users may not use this directly.
    
    :parameters:
    - `database_name`: database name in which to execute aql (string)
    - `partial_query`: partial AQL to split (string)
    - `params`: params to use (dict)
    - `num_split` (optional): chunk size to split (integer)
    """

    target_db = database.create_and_get_database(database_name)

    for partial_rows in dict_spliter(params['rows'], num_split):
        copied = params.copy()
        copied['rows'] = [i for i in dict(partial_rows).values()]
        final_query = partial_query.format(**copied)

        aql.execute_aql(final_query, target_db = target_db)
