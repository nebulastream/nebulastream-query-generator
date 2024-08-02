# Query Generator

## About
This project represents the Query Generator that generates large volumes of synthetic queries with different characteristics for NebulaStream.
We first describe the process of generating a synthetic query.
After that, we describe the process of generating a syntactically equivalent query based on a synthetic query.
In the end, we describe the process of generating a partially equivalent query based on a synthetic query.
After generating a synthetic query set, our Query Generator shuffles the query set.

This query gernerator was used in the EDBT 2023 publication, Incremental Stream Query Merging. Please use the trailing citation when refering to this work:

```citation
@inproceedings{DBLP:conf/edbt/ChaudharyZMK23,
  author       = {Ankit Chaudhary and
                  Steffen Zeuch and
                  Volker Markl and
                  Jeyhun Karimov},
  editor       = {Julia Stoyanovich and
                  Jens Teubner and
                  Nikos Mamoulis and
                  Evaggelia Pitoura and
                  Jan M{\"{u}}hlig and
                  Katja Hose and
                  Sourav S. Bhowmick and
                  Matteo Lissandrini},
  title        = {Incremental Stream Query Merging},
  booktitle    = {Proceedings 26th International Conference on Extending Database Technology,
                  {EDBT} 2023, Ioannina, Greece, March 28-31, 2023},
  pages        = {604--617},
  publisher    = {OpenProceedings.org},
  year         = {2023},
  url          = {https://doi.org/10.48786/edbt.2023.51},
  doi          = {10.48786/EDBT.2023.51},
  timestamp    = {Sat, 29 Apr 2023 13:06:22 +0200},
  biburl       = {https://dblp.org/rec/conf/edbt/ChaudharyZMK23.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```


### Generating A Synthetic Query

Our Query Generator contains a collection of [Distinct Operator Strategies](operator_generator_strategies/distinct_operator_strategies).
A Distinct Operator Strategy generates semantically distinct operator of a specific type for each successive call.
The Query Generator selects a set of Distinct Operator Strategies and compute a synthetic query in bottom-up fashion
using three steps.
First, the Generator constructs a _**Source**_ operator using the _Source_ Distinct Operator Strategy and an input schema.
Second, the Generator iterates over the remaining Distinct Operator Strategies and adds the generated operator
to the synthetic query.
In this step, the Generator excludes **_Sink_** and **_Source_** Distinct Operator Strategies.
Third, the Generator selects the _Sink_ Distinct Operator Strategy and completes the synthetic query.


### Generating Semantically Equivalent Query

Our Query Generator is capable of generating a collection of semantically equal queries based on a synthetic query.
These queries are semantically equal but syntactically distinct.
To this end, our Query Generator consists of several [Equivalent Operator Strategies](operator_generator_strategies/equivalent_operator_strategies).
An Equivalent Operator Strategy initializes itself based on an operator.
For each successive call, the Equivalent Operator Strategy produces a syntactically distinct but semantically equivalent
operator of same type.
For example, a _Filter_ Equivalent Operator Strategy initializes with the predicate **a>b** produces a
syntactically distinct but semantically equivalent _**Filter**_ operator with predicate **a*10>b*10** or **b<a**.
The Query Generator produces a semantically equal query in two steps.
First, the Query Generator selects for each operator in the base synthetic query an Equivalent Operator Strategy.
Second, the Generator iterates over the selected Equivalent Operator Strategies and constructs a new synthetic query.

### Generating Semantically Contained Query

Our Query Generator is capable of generating a collection of semantically contained queries based on a synthetic query.
These queries are semantically contained but syntactically distinct.
To this end, our Query Generator consists of [Containment Operator Strategies](operator_generator_strategies/containment_operator_strategies).
A Containment Operator Strategy initializes itself based on an operator.
For each successive call, the Containment Operator Strategy produces a syntactically distinct but semantically contained
operator of same type.
For example, a _Filter_ Containment Operator Strategy initializes with the predicate **a>3** produces a
syntactically distinct but semantically equivalent _**Filter**_ operator with predicate **a>7** or **9<a**.

### Generating A Partially Equivalent Query

<p style='text-align: justify;'>
Our Query Generator can generate a collection of partially equal queries based on a synthetic query.
To this end, our Query Generator follows a four-step process.
First, the Query Generator selects the upstream operators from the synthetic query for partial equivalence.
Second, the Generator initializes a set of Equivalent Operator Strategies for the selected operators.
Third, the Generator combines the Equivalent Operator Strategies with a set of Distinct Operator Strategies.
Fourth, the Generator iterates over the Operator Strategies and computes partially equivalent synthetic query.
</p>

### Generating A Contained Query

<p style='text-align: justify;'>
Our Query Generator can generate a collection of contained queries based on a synthetic query.
To this end, our Query Generator follows a five-step process.
First, the Query Generator selects the source operators from the synthetic query for partial containment.
Second, the Generator initializes a set of Equivalent Operator Strategies.
Third, the Generator initializes a set of Contained Operator Strategies.
Fourth, the Generator combines the Equivalent Operator Strategies and the Contained Operator Strategies 
with a set of Distinct Operator Strategies.
Fifth, the Generator iterates over the Operator Strategies and computes contained synthetic queries.
</p>

### Generating Synthetic Query Set

<p style='text-align: justify;'> The Query Generator takes as inputs the total number of queries to generate, a collection of distinct
sources to use, an enum indicating what kind of queries to produce, number of equivalent/contained query groups to produce, 
the percentage of semantically distinct queries to generate,
and a percentage of equivalence among semantically equivalent/contained queries.
The Query Generator then generates the queries using the following five-Step process: </p>

1. The Query Generator calculates the number of queries per source by dividing total number of queries by the number of sources.
2. For each distinct source, the Query Generator calculates number of queries in each equivalent/contained query group.
3. The generator computes semantically distinct queries based on the percentage of semantically distinct queries to generate for each group.
4. The generator calculates the number of equivalent/contained queries to generate within the group.
5. The generator based on the value of percentage of equivalence among semantically equivalent/contained queries generates the remaining queries in the group

## Installation

The [requirements.txt](requirements.txt) file should list all Python libraries that this project depend on, and they will be installed using

`pip install -r requirements.txt`

Additionally, you need to install git lsf utility to clone the repo.

`sudo apt install git-lfs`

## How To Run

`python3 main.py -cf=<Location of configuration file>`

## Configurations

The configurations are provided in a YAML file with following parameters:

``` 
noQueries: (Int) Total number of queries to be generated
```

```
workloadType: (String) Normal or BiasedForHybrid. 
              Normal workload generates combination of distinct and equivalent queries.
              BiasedForHybrid workload generates large number of syntactically equivalent queries. 
```

```
generateEquivalentQueries: (enum) equivalence, distinct, or containment; indicates what type of query sets we want to generate.
                           Note: It can happen that despite providing distinct, 
                           some of the generated queries are equivalent or contained.`
```

```
sourcesToUse: (Int) Number of distinct sources to use for generating queries. 
               Note: This will randomly select the source schemas define in sourceList configuration.
```

```
equivalenceConfig:
    noOfEquivalentQueryGroups: (Int) Number of distinct query groups within the overall query set. Such that, each query 
                                in a group is based on same source schema and are syntactically distinct but semantically
                                equivalent to each other. 
    percentageOfRandomQueries: (Int) value betwen 0-100 indicates the percentage of distinct queries within each query 
                                group such that those queries are semantically distinct to rest of the queries in the group.
    percentageOfEquivalence: (Int) value between 0-100 indicating the percentage of overlap among queries in a query group.
```

```
containmentConfig:
    noOfEquivalentQueryGroups:      (Int) Number of distinct query groups within the overall query set. Such that, each query 
                                    in a group is based on same source schema and are syntactically distinct but semantically
                                    equivalent to each other. 
    percentageOfRandomQueries:      (Int) value betwen 0-100 indicates the percentage of distinct queries within each query 
                                    group such that those queries are semantically distinct to rest of the queries in the group.
    percentageOfEquivalence:        (Int) value between 0-100 indicating the percentage of operator overlap among queries in a query group.
    percentageOfEquivalentQueries:  (Int) value between 0-100 indicating how many queries in the batch should be structurally
                                    distinct but semantically equivalent.
    noOfContainmentQueryGroups:     (Int) Number of distinct query groups within the overall query set. Such that, each query 
                                    in a group is based on the same source schema and all queries in a group are syntactically 
                                    distinct but semantically contained.
    allowMultiContainment:          (Boolean) If true, we add multiple containment cases of one kind (choice between filter, 
                                    projection and window aggregation containment) to a single query. If false, we add only one
                                    containment case.
    shuffleContainment:             (Boolean) If true, we shuffle the order of contained and equivalent operators withing a query. If false,
                                    we first add the equivalent operators and then the contained operators.
    allowMultipleWindows:           (Boolean) If true, we allow multiple window aggregations in a single query. If false, we allow only
                                    one window aggregation in a single query.
```

```
sourceList: (Object[]) List of different source schemas to be used while generating the queries
  - stream_name: (String) Names of the Stream
    int_fields: (String[]) Names of integer fields in the schema
    string_fields: (String[]) Names of string fields in the schema
    double_fields: (String[]) Names of double fields in the schema
    timestamp_fields: (String[]) Names of timestamp fields in the schema
```

### Example Configuration:

```
noQueries: 18000
workloadType: Normal
generateEquivalentQueries: true
sourcesToUse: 4
equivalenceConfig:
  noOfEquivalentQueryGroups: 100
  percentageOfRandomQueries: 0
  percentageOfEquivalence: 100
sourceList:
  - streamName: 'example1'
    intFields: [ 'a', 'b', 'c', 'd', 'e', 'f' ]
    stringFields: [ ]
    doubleFields: [ ]
    timestampFields: [ 'time1', 'time2' ]
```
