# About
This project is used for generating random valid NES queries based on given configurations. 

# Installation

Please make sure to install following dependencies before working with this project:

`pip install click`

# How To Run

`./main --configPath=<Location of configuration file>`

## Configurations

The configurations are provided in a YAML file with following parameters:

``` 
no_queries: (Int) Total number of queries to be generated
```

```
generate_equivalent_queries: (Bool) If equivalent queries needed to be generated "explicitly".
                             Note: It can happen that despite providing "false", 
                             some of the generated queries are equivalent.`
```

```
equivalence_config.no_of_equivalent_query_groups: (Int) If "generate_equivalent_queries=true" then how many distinct groups 
                                                  of equivalent queries neede to be generated
```

```
source_list: (Object[]) List of different source schemas to be used while generating the queries
  - stream_name: (String) Names of the logical stream
    int_fields: (String[]) Names of integer fields in the schema
    string_fields: (String[]) Names of string fields in the schema
    double_fields: (String[]) Names of double fields in the schema
    timestamp_fields: (String[]) Names of timestamp fields in the schema
```

#### Example Configuration:

```
no_queries: 1000
generate_equivalent_queries: true
equivalence_config:
  no_of_equivalent_query_groups: 100
source_list:
- stream_name: 'example'
  int_fields: [ 'A', 'B', 'C', 'D' ]
  string_fields: [ ]
  double_fields: [ ]
  timestamp_fields: [ ]
```
