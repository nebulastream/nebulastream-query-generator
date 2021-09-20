import random
from typing import List

import click
import yaml

from operator_generator_strategies.equivalent_operator_strategies.filter_equivalent_filter_strategy import \
    FilterEquivalentFilterGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_substitute_map_expression_startegy import \
    FilterSubstituteMapExpressionGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.join_equivalent_strategy import \
    JoinEquivalentJoinGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_create_new_field_strategy import \
    MapCreateNewFieldGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_substitute_map_expression_strategy import \
    MapSubstituteMapExpressionGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_expression_reorder_strategy import \
    FilterExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_operator_reorder_strategy import \
    FilterOperatorReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_expression_reorder_strategy import \
    MapExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_operator_reorder_startegy import \
    MapOperatorReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.project_equivalent_strategy import \
    ProjectEquivalentProjectGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.aggregation_equivalent_aggregation_strategy import \
    AggregationEquivalentAggregationGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.union_equivalent_strategy import \
    UnionEquivalentUnionGeneratorStrategy
from query_generator.query import Query
from utils.contracts import Schema
from operator_generator_strategies.distinct_operator_strategies.distinct_filter_strategy import \
    DistinctFilterGeneratorStrategy
from query_generator.generator import QueryGenerator
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy1 import \
    DistinctMapGeneratorStrategy1
from operator_generator_strategies.distinct_operator_strategies.distinct_projection_strategy import \
    DistinctProjectionGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_aggregation_strategy import \
    DistinctAggregationGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_union_strategy import \
    DistinctUnionGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_join_strategy import \
    DistinctJoinGeneratorStrategy

from utils.utils import random_list_element


@click.command()
@click.option('-cf', '--config-file', help='Location of the configuration file.', type=click.STRING)
def run(config_file):
    print("Loading configurations")
    file = open(config_file, 'r')
    configuration = yaml.load(file, yaml.Loader)
    print(configuration)

    possibleSources = []
    for sourceConf in configuration['source_list']:
        source = Schema(name=sourceConf['stream_name'], int_fields=sourceConf['int_fields'],
                        string_fields=sourceConf['string_fields'],
                        timestamp_fields=sourceConf['timestamp_fields'], double_fields=sourceConf['double_fields'],
                        fieldNameMapping={})
        possibleSources.append(source)

    generateEquivalentQueries = configuration['generate_equivalent_queries']
    numOfDistinctSourcesToUse = configuration['sources_to_use']
    numberOfQueries = configuration['no_queries']

    queries: List[Query] = []
    if generateEquivalentQueries:
        percentageOfRandomQueries = configuration['equivalence_config']['percentage_of_random_queries']
        numberOfEquivalentQueryGroups = configuration['equivalence_config']['no_of_equivalent_query_groups']
        percentageOfEquivalence = configuration['equivalence_config']['percentage_of_equivalence']
        numberOfGroupsPerSource = int(numberOfEquivalentQueryGroups / numOfDistinctSourcesToUse)
        numberOfQueriesPerGroup = int(numberOfQueries / numberOfEquivalentQueryGroups)
        # Iterate over sources
        for i in range(numOfDistinctSourcesToUse):

            numOfSourceToUse = 1
            if numOfDistinctSourcesToUse > 1:
                numOfSourceToUse = 2

            distinctSourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
            distinctSourcesToUse.sort(key=lambda x: x.name, reverse=False)
            baseSource = distinctSourcesToUse[0]
            distinctSourcesToUse.remove(baseSource)
            for j in range(numberOfGroupsPerSource):
                # NOTE: this won't work when we need a binary operator in the query
                randomQueries = int((numberOfQueriesPerGroup * percentageOfRandomQueries) / 100)
                equivalentQueries = getEquivalentQueries(numberOfQueriesPerGroup - randomQueries,
                                                         percentageOfEquivalence,
                                                         baseSource, distinctSourcesToUse)
                distinctQueries = getDistinctQueries(randomQueries, baseSource, distinctSourcesToUse)
                queries.extend(equivalentQueries)
                queries.extend(distinctQueries)

        # Populate remaining queries
        remainingQueries = numberOfQueries - (
                numberOfQueriesPerGroup * numberOfGroupsPerSource * numOfDistinctSourcesToUse)
        if remainingQueries > 0:
            for i in range(int(remainingQueries / numberOfQueriesPerGroup)):
                numOfSourceToUse = random.randint(2, len(possibleSources))
                distinctSourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                distinctSourcesToUse.sort(key=lambda x: x.name, reverse=False)
                baseSource = distinctSourcesToUse[0]
                distinctSourcesToUse.remove(baseSource)
                equivalentQueries = getEquivalentQueries(numberOfQueriesPerGroup, percentageOfEquivalence,
                                                         baseSource, distinctSourcesToUse)
                queries.extend(equivalentQueries)
    else:
        numberOfDistinctQueriesPerSource = numberOfQueries / numOfDistinctSourcesToUse
        for i in range(numOfDistinctSourcesToUse):
            numOfSourceToUse = 1
            if numOfDistinctSourcesToUse > 1:
                numOfSourceToUse = 2
            distinctSourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
            distinctSourcesToUse.sort(key=lambda x: x.name, reverse=False)
            baseSource = distinctSourcesToUse[0]
            distinctSourcesToUse.remove(baseSource)
            distinctQueries = getDistinctQueries(numberOfDistinctQueriesPerSource, baseSource, distinctSourcesToUse)
            queries.extend(distinctQueries)

    # Write queries into file
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


def getDistinctQueries(numberOfQueriesToGenerate: int, baseSource: Schema, possibleSources: List[Schema]) -> \
        List[Query]:
    filter_generator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapGeneratorStrategy1()
    project_generator = DistinctProjectionGeneratorStrategy()
    aggregation_generator = DistinctAggregationGeneratorStrategy()
    union_generator = DistinctUnionGeneratorStrategy(possibleSources)
    join_generator = DistinctJoinGeneratorStrategy(possibleSources)
    distinctOperatorGeneratorStrategies = [filter_generator, map_generator, project_generator, union_generator,
                                           join_generator, aggregation_generator]
    return QueryGenerator(baseSource, numberOfQueriesToGenerate, [], distinctOperatorGeneratorStrategies).generate()


def getEquivalentQueries(numberOfQueriesPerGroup: int, percentageOfEquivalence: int, baseSource: Schema,
                         possibleSources: List[Schema]) -> List[Query]:
    # Initialize instances of each generator strategy
    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    map_create_new_field_strategy = MapCreateNewFieldGeneratorStrategy()
    map_substitute_map_expression_strategy = MapSubstituteMapExpressionGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    filter_equivalent_filter_strategy = FilterEquivalentFilterGeneratorStrategy()
    project_equivalent_project_strategy = ProjectEquivalentProjectGeneratorStrategy()
    aggregate_equivalent_aggregate_strategy = AggregationEquivalentAggregationGeneratorStrategy()

    # Remove the base source from possible sources for binary operator and initialize generator strategies
    union_equivalent_strategies = UnionEquivalentUnionGeneratorStrategy(possibleSources)
    join_equivalent_strategies = JoinEquivalentJoinGeneratorStrategy(possibleSources)

    equivalentOperatorGeneratorStrategies = [
        map_expression_reorder_strategy,
        map_operator_reorder_strategy,
        # map_create_new_field_strategy,
        # map_substitute_map_expression_strategy,
        filter_substitute_map_expression_strategy,
        filter_expression_reorder_strategy,
        filter_operator_reorder_strategy,
        filter_equivalent_filter_strategy,
        project_equivalent_project_strategy
    ]

    filterGenerator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    mapGenerator = DistinctMapGeneratorStrategy1()
    aggregateGenerator = DistinctAggregationGeneratorStrategy()
    projectGenerator = DistinctProjectionGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filterGenerator, mapGenerator, aggregateGenerator, projectGenerator,
                                           mapGenerator]

    distinctOperators = 5 - int((5 * percentageOfEquivalence) / 100)
    distinctOperatorGenerators = random.sample(distinctOperatorGeneratorStrategies, distinctOperators)
    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 5 - distinctOperators)

    if len(possibleSources) >= 1:
        unionPresent = False
        if random.randint(1, 2) % 2 == 0:
            equivalentOperatorGenerators.append(union_equivalent_strategies)
            unionPresent = True

        if not unionPresent:
            equivalentOperatorGenerators.append(join_equivalent_strategies)

    if random.randint(1, 5) % 5 == 0:
        equivalentOperatorGenerators.append(aggregate_equivalent_aggregate_strategy)

    return QueryGenerator(baseSource, numberOfQueriesPerGroup, equivalentOperatorGenerators,
                          distinctOperatorGenerators).generate()


if __name__ == '__main__':
    run()
