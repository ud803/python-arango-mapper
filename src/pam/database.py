from pam import client


def create_and_get_database(arango_conn, database_name, username, password):
    """Returns database object. If not exists, create a new one. Requires username & passwords
    
    :parameters:
    - `database_name`: database name to create (string)
    - `username`: username with database creation role (string)
    - `password`: password with database creation role (string)
    """
    
    sys_db = arango_conn.db('_system', username=username, password=password)

    if not sys_db.has_database(database_name):
        sys_db.create_database(database_name)
    
    return arango_conn.db(database_name, username=username, password=password)