# python-arango-mapper
 Python Mapper for ArangoDB. Uses [Python-Arango](https://github.com/ArangoDB-Community/python-arango/blob/main/README.md) library as core engine.


## Requirements

- Python version 3.6+

## Installation

```shell
pip install python-arango-mapper
```

## WHAT PAM IS

PAM(`python-arango-mapper`) is an easy-to-use arangoDB mapper built upon great library ❤ : [Python-Arango](https://github.com/ArangoDB-Community/python-arango/blob/main/README.md).

With one-time schema declaration, your object-typed data can be easily converted into ArangoDB data. 

<img src="https://github.com/ud803/udamata/blob/main/python-arango-mapper.png" width="800" height="500" />

PAM currently has **4 schema types,** and the examples are like follows:


## GETTING STARTED
Usage examples:

```python
from pam import client, database, converter

# Example of Data. Intentionally put duplicated data.
data = [
    {'my_name_is': 'Scott', 'title': 'HarryPotter 1', 'score': 5, 'time': '2022-01-01'},
    {'my_name_is': 'Scott', 'title': 'HarryPotter 2', 'score': 8, 'time': '2022-01-02'},
    {'my_name_is': 'Scott', 'title': 'HarryPotter 2', 'score': 8, 'time': '2022-01-02'},
    {'my_name_is': 'Scott', 'title': 'HarryPotter 2', 'score': 8, 'time': '2022-01-02'},
    {'my_name_is': 'Scott', 'title': 'HarryPotter 3', 'score': 6, 'time': '2022-01-03'},
    {'my_name_is': 'Scott', 'title': 'HarryPotter 3', 'score': 6, 'time': '2022-01-03'},
    {'my_name_is': 'Marry', 'title': 'Starwars 1', 'score': 3, 'time': '2022-01-04'},
    {'my_name_is': 'Marry', 'title': 'Starwars 1', 'score': 3, 'time': '2022-01-04'},
    {'my_name_is': 'Marry', 'title': 'Starwars 1', 'score': 3, 'time': '2022-01-04'},
    {'my_name_is': 'Marry', 'title': 'Starwars 2', 'score': 5, 'time': '2022-01-05'},
    {'my_name_is': 'Marry', 'title': 'Starwars 2', 'score': 5, 'time': '2022-01-05'},
    {'my_name_is': 'Marry', 'title': 'Starwars 2', 'score': 5, 'time': '2022-01-05'}
]

schemas = {
    # Type 1
    'Person': {
        'type': ('vertex', 'unique_vertex'),
        'collection': 'Person',
        'unique_key': ('my_name_is',),
        'fields': {
            'name': 'my_name_is'
        },
        'index': [
            {'field' : ('name',), 'unique' : True, 'ttl' : False}
        ]
    },
    # Type 1
    'Movie': {
        'type': ('vertex', 'unique_vertex'),
        'collection': 'Movie',
        'unique_key': ('title',),
        'fields': {
            'title': 'title'
        },
        'index': [
            {'field' : ('title',), 'unique' : True, 'ttl' : False}
        ]
    },
    # Type 2
    'has_ever_watched': {
        'type': ('edge', 'unique_edge_btw_vertices'),
        'collection': 'has_ever_watched',
        '_from_collection': 'Person',
        '_from': ('my_name_is',),
        '_to_collection': 'Movie',
        '_to': ('title',),
        'fields': {
        },
        'index': []
    },
    # Type 3
    'watched': {
        'type': ('edge', 'unique_edge_on_event'),
        'collection': 'watched',
        'unique_key': ('time',),
        '_from_collection': 'Person',
        '_from': ('my_name_is',),
        '_to_collection': 'Movie',
        '_to': ('title',),
        'fields': {
            'time': 'time',
            'score': 'score'
        },
        'index': []
    },
    # Type 4
    'loves_most': {
        'type': ('edge', 'unique_edge_from_vertex'),
        'collection': 'loves_most',
        '_from_collection': 'Person',
        '_from': ('my_name_is',),
        '_to_collection': 'Movie',
        '_to': ('title',),
        'fields': {
            'time': 'time',
            'score': 'score'
        },
        'condition': {
            # If score has max value, change favorite movie
            'max_by': {
                'score': ['_to']
            }
        },
        'index': []
    },
    'some_redundant_schema_used_elsewhere': {


    }
}

arango_conn = client.get_arango_conn(hosts="http://localhost:8529")
database_obj = database.create_and_get_database(arango_conn, 'favorite_movie', 'root', 'password')
mapping_list = ['Person', 'Movie', 'has_ever_watched', 'watched', 'loves_most']
converter.arango_converter(data, database_obj, schemas, mapping_list)
```


## TYPES OF SCHEMA

There are yet no official documentation of this library. So for now, this would be the only documentation to follow. 🤢

### Type 1. unique_vertex
unique_vertex type is a vertex mapper usually used to make one unique vertex entity.

For example, it can be used to represent people, food names, job titles, city names in Korea, etc.

From below schema, PAM do the following things:
1) Creates vertex collection named Person, and created indices
2) For every row, the `_key` field is created using `unique_key` fields each element joinned with `_` string ; here, the `_key` would be `{name}_{national_id}`
3) For every row, insert or upsert `fields` property

```python
"""
unique_vertex Type Requirements
- `type` (required) -- Tuple, (type_1, type_2)
- `collection` (required) -- String, collection name to map data into
- `unique_key` (required) -- Tuple, fields used to distinguish as unique document
- `fields` (required) -- Dict, data to store in documents. Keys represent names to use in ArangoDB, values represent field names to get data from 
- `index` (required) -- List of Dicts, index to use
"""

schemas = {'Person': {
    'type': ("vertex", 'unique_vertex'),
    'collection'  : 'Person',
    'unique_key' : ('name', 'national_id', ),
    'fields': {
        'name' : 'name',
        'age' : 'age',
        'job' : 'job'
    },
    'index': [
        {'field' : ('name',), 'unique' : True, 'ttl' : False}
    ]
}}
```

### Type 2. unique_edge_btw_vertices
unique_edge_btw_vertices type is an edge collection and should have `_from` and `_to` fields.

It is used to represent unique edge between two vertices. 

For example, it ensures that between `Seoul`, which is city collection, and `Korea`, which is country collection, there only exists one `is_city_of` edge. Duplicate data would be ignored.

```python
"""
unique_edge_btw_vertices Type Requirements
- `type` (required) -- Tuple, (type_1, type_2)
- `collection` (required) -- String, collection name to map data into
- `_from_collection` (required) -- String, name of _from edge collection
- `_from` (required) -- Tuple, fields used to distinguish as unique document in _from collection 
- `_to_collection` (required) -- String, name of _to edge collection
- `_to` (required) -- Tuple, fields used to distinguish as unique document in _to_ collection
- `fields` (required) -- Dict, data to store in documents. Keys represent names to use in ArangoDB, values represent field names to get data from 
- `index` (required) -- List of Dicts, index to use
"""

schemas = {'is_city_of': {
    'type': ("edge", 'unique_edge_btw_vertices'),
    'collection'  : 'is_city_of',
    '_from_collection' : 'City',
    '_from' : ('city_name', 'country_name', ),
    '_to_collection' : 'Country',
    '_to' : ('country_name',),
    'fields': {
        'city_number' : 'city_number'
    },
    'index': [
    ]
}}
```


### Type 3. unique_edge_on_event
unique_edge_on_event type is an edge collection and should have `_from` and `_to` fields.

Unlike Type 2 which has unique edge between vertices, Type 3 ensures that numerous edges can be created between two vertices with `unique_id` constraints.

For example, say that I want to represent `visited` event between `Person` collection and `City` collection. But I want to distinguish these edges with visited datetime. Then one can use `datetime` in `unique_key` field. It would create document `_key` with `unique_key` constraints, and ignore duplicate data from being created.

```python
"""
unique_edge_on_event Type Requirements
- `type` (required) -- Tuple, (type_1, type_2)
- `collection` (required) -- String, collection name to map data into
- `unique_key` (required) -- Tuple, fields used to distinguish as unique document
- `_from_collection` (required) -- String, name of _from edge collection
- `_from` (required) -- Tuple, fields used to distinguish as unique document in _from collection 
- `_to_collection` (required) -- String, name of _to edge collection
- `_to` (required) -- Tuple, fields used to distinguish as unique document in _to_ collection
- `fields` (required) -- Dict, data to store in documents. Keys represent names to use in ArangoDB, values represent field names to get data from 
- `index` (required) -- List of Dicts, index to use
"""

schemas = {'visited': {
    'type': ("edge", 'unique_edge_on_event'),
    'collection'  : 'visited',
    'unique_key' : ('visit_datetime',),
    '_from_collection' : 'Person',
    '_from' : ('name', 'national_id', ),
    '_to_collection' : 'City',
    '_to' : ('city_name', 'country_name', ),
    'fields': {
        'visit_datetime' : 'visit_datetime',
        'depart_airport' : 'depart_airport',
        'flight_tailnum' : 'flight_tailnum'
    },
    'index': [
    ]
}}
```

### Type 4. unique_edge_from_vertex
unique_edge_from_vertex type is an edge collection and should have `_from` and `_to` fields.

It represents unique edge that stems from one vertex, but can conditionally change its `_to` vertex accordingly.

For example, let's assume there is a `Person` collection and a `Movie` collection. Let's say `loves_most` edge collection represents one's favorite movie, and it can be changed time after time. Then, one can use `condition` field to use `min_by`, `max_by`, or `if` conditions and change `_to` field.  

```python
"""
unique_edge_on_event Type Requirements
- `type` (required) -- Tuple, (type_1, type_2)
- `collection` (required) -- String, collection name to map data into
- `_from_collection` (required) -- String, name of _from edge collection
- `_from` (required) -- Tuple, fields used to distinguish as unique document in _from collection 
- `_to_collection` (required) -- String, name of _to edge collection
- `_to` (required) -- Tuple, fields used to distinguish as unique document in _to_ collection
- `fields` (required) -- Dict, data to store in documents. Keys represent names to use in ArangoDB, values represent field names to get data from 
- `condition` (required) -- Dict, used to change fields according to conditions. FIELDS used in conditions MUST BE DECLARED in `fields`
- `index` (required) -- List of Dicts, index to use
"""

schemas = {'loves_most': {
    'type': ("edge", 'unique_edge_from_vertex'),
    'collection'  : 'loves_most',
    '_from_collection' : 'Person',
    '_from' : ('name', 'national_id', ),
    '_to_collection' : 'Movie',
    '_to' : ('movie_id',),
    'fields': {
        'time' : 'time'
    },
    'condition': {
        # changes '_to' field, if new document's 'time' is greater than old documents' time field.
        # criteria field, which is 'time', would be automatically updated with new data by converter
        'max_by': {
            'time': ['_to']
        }
    }
    'index': [
    ]
}}
```

### Conditions Further Explained
In fact, `condition` field can be used in any type of PAM mapping if it fits the usage case.

Currently, there are 3 type sof conditions.

- `max_by` and `min_by` are used to change certain field depending on size comparison. 

Like below, you can set a key in `max_by` dict, and set a list of fields to change as values.

```python
'condition': {
    'max_by': {
        'field_to_set_as_comparison_criteria': ['field_to_change_1', 'field_to_change_2']
    }
}
```

- `if` condition is literally used as a conditional statement.

It should have list of condition strings, follow AQL syntax, and will be inserted as it is. 

```python
'condition': {
    'if': [
        """_to : CONTAINS(OLD._to, 'hi') ? doc._to : OLD._to""",
        """_to : CONTAINS(OLD._to, 'there') ? doc._to : OLD._to"""
        ]
}
```
