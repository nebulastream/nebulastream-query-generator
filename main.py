import random
from typing import List

import click
import yaml

from operator_generator_strategies.equivalent_operator_strategies.filter_equivalent_filter_strategy import \
    FilterEquivalentFilterGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_substitute_map_expression_startegy import \
    FilterSubstituteMapExpressionGeneratorStrategy
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
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy import \
    DistinctMapGeneratorStrategy
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
    sourcesToUse = configuration['sources_to_use']
    numberOfQueries = configuration['no_queries']
    percentageOfRandomQueries = configuration['percentage_of_random_queries']
    # initially the number of random queries equal to number of queries
    numberOfRandomQueries = (numberOfQueries * percentageOfRandomQueries) / 100

    binaryOperator = configuration['binaryOperator']

    queries: List[Query] = []
    if generateEquivalentQueries:
        numberOfEquivalentQueries = numberOfQueries - numberOfRandomQueries
        numberOfEquivalentQueryGroups = configuration['equivalence_config']['no_of_equivalent_query_groups']
        percentageOfEquivalence = configuration['equivalence_config']['percentage_of_equivalence']
        numberOfQueriesPerGroup = int(numberOfEquivalentQueries / numberOfEquivalentQueryGroups)
        numberOfGroupsPerSource = int(numberOfEquivalentQueryGroups / sourcesToUse)
        # Iterate over sources
        for i in range(sourcesToUse):
            for j in range(numberOfGroupsPerSource):
                # NOTE: this won't work when we need a binary operator in the query
                generatedQueries = getEquivalentQueries(numberOfQueriesPerGroup, percentageOfEquivalence,
                                                        binaryOperator, possibleSources[i], possibleSources)
                queries.extend(generatedQueries)

        # Populate remaining queries
        remainingQueries = numberOfEquivalentQueries - (
                numberOfQueriesPerGroup * numberOfGroupsPerSource * sourcesToUse)

        if remainingQueries > 0:
            # NOTE: this won't work when we need a binary operator in the query
            for i in range(int(remainingQueries / numberOfQueriesPerGroup)):
                _, sourceToUse = random_list_element(possibleSources)
                generatedQueries = getEquivalentQueries(numberOfQueriesPerGroup, percentageOfEquivalence,
                                                        binaryOperator, sourceToUse, possibleSources)
                queries.extend(generatedQueries)

    _, sourceToUse = random_list_element(possibleSources)
    generatedQueries = getRandomQueries(numberOfRandomQueries, binaryOperator, sourceToUse, possibleSources)
    queries.extend(generatedQueries)

    # Write queries into file
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


def getRandomQueries(numberOfQueries: int, binaryOperator: bool, sourceToUse: Schema,
                     possibleSources: List[Schema]) -> List[Query]:
    filter_generator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapGeneratorStrategy()
    project_generator = DistinctProjectionGeneratorStrategy()
    union_generator = DistinctUnionGeneratorStrategy()
    join_generator = DistinctJoinGeneratorStrategy()
    aggregation_generator = DistinctAggregationGeneratorStrategy()

    # 5 source Map Filter Sink
    distinctOperatorGeneratorStrategies = [filter_generator, map_generator, project_generator, aggregation_generator]

    return QueryGenerator(sourceToUse, numberOfQueries, [], distinctOperatorGeneratorStrategies).generate(
        binaryOperator)


def getEquivalentQueries(numberOfQueriesPerGroup: int, percentageOfEquivalence: int, binaryOperator: bool,
                         sourceToUse: Schema, possibleSources: List[Schema]) -> List[Query]:
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
    union_equivalent_strategies = UnionEquivalentUnionGeneratorStrategy(possibleSources)

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
    mapGenerator = DistinctMapGeneratorStrategy()
    aggregateGenerator = DistinctAggregationGeneratorStrategy()
    projectGenerator = DistinctProjectionGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filterGenerator, mapGenerator, aggregateGenerator, projectGenerator,
                                           mapGenerator]

    distinctOperators = 5 - int((5 * percentageOfEquivalence) / 100)
    distinctOperatorGenerators = random.sample(distinctOperatorGeneratorStrategies, distinctOperators)
    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 5 - distinctOperators)

    if random.randint(1, 10) % 10 == 0:
        equivalentOperatorGenerators.append(union_equivalent_strategies)

    if random.randint(1, 5) % 5 == 0:
        equivalentOperatorGenerators.append(aggregate_equivalent_aggregate_strategy)

    maxUnionLoc = -1
    for i in range(len(equivalentOperatorGenerators)):
        if isinstance(equivalentOperatorGenerators[i], ProjectEquivalentProjectGeneratorStrategy):
            maxUnionLoc = i
        elif isinstance(equivalentOperatorGenerators[i], UnionEquivalentUnionGeneratorStrategy):
            if i > maxUnionLoc and maxUnionLoc != -1:
                equivalentOperatorGenerators[maxUnionLoc], equivalentOperatorGenerators[i] = \
                    equivalentOperatorGenerators[i], equivalentOperatorGenerators[maxUnionLoc]
            break

    return QueryGenerator(sourceToUse, numberOfQueriesPerGroup, equivalentOperatorGenerators,
                          distinctOperatorGenerators).generate(binaryOperator)


if __name__ == '__main__':
    run()
