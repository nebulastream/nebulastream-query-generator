import random
from typing import List

import click
import yaml

from generator_config.config import GeneratorConfig
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_equivalent_filter_strategy import \
    FilterEquivalentFilterGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_substitute_map_expression_startegy import \
    FilterSubstituteMapExpressionGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_create_new_field_strategy import \
    MapCreateNewFieldGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_substitute_map_expression_strategy import \
    MapSubstituteMapExpressionGeneratorStrategy
from query_generator.query import Query
from utils.contracts import Schema
from operator_generator_strategies.distinct_operator_strategies.distinct_filter_strategy import \
    DistinctFilterGeneratorStrategy
from query_generator.generator import QueryGenerator
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy import \
    DistinctMapGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_expression_reorder_strategy import \
    FilterExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_operator_reorder_strategy import \
    FilterOperatorReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_expression_reorder_strategy import \
    MapExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_operator_reorder_startegy import \
    MapOperatorReorderGeneratorStrategy
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
                        timestamp_fields=sourceConf['timestamp_fields'], double_fields=sourceConf['double_fields'])
        possibleSources.append(source)

    generateEquivalentQueries = configuration['generate_equivalent_queries']
    numberOfQueries = configuration['no_queries']
    percentageOfRandomQueries = configuration['percentage_of_random_queries']
    # initially the number of random queries equal to number of queries
    numberOfRandomQueries = (numberOfQueries * percentageOfRandomQueries) / 100

    queries: List[Query] = []
    if generateEquivalentQueries:
        numberOfEquivalentQueries = numberOfQueries - numberOfRandomQueries
        numberOfQueryGroups = configuration['equivalence_config']['no_of_equivalent_query_groups']
        numberOfQueriesPerGroup = int(numberOfEquivalentQueries / numberOfQueryGroups)
        # Iterate over number of query groups
        for i in range(numberOfQueryGroups):
            # NOTE: this won't work when we need a binary operator in the query
            _, sourceToUse = random_list_element(possibleSources)
            generatedQueries = getEquivalentQueries(numberOfQueriesPerGroup, sourceToUse)
            queries.extend(generatedQueries)

        # Populate remaining queries
        numberOfQueriesPerGroup = numberOfEquivalentQueries - (numberOfQueriesPerGroup * numberOfQueryGroups)
        # NOTE: this won't work when we need a binary operator in the query
        _, sourceToUse = random_list_element(possibleSources)
        generatedQueries = getEquivalentQueries(numberOfQueriesPerGroup, sourceToUse)
        queries.extend(generatedQueries)

    _, sourceToUse = random_list_element(possibleSources)
    generatedQueries = getRandomQueries(numberOfRandomQueries, sourceToUse)
    queries.extend(generatedQueries)

    # Write queries into file
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


def getRandomQueries(numberOfQueries: int, sourceToUse: Schema) -> List[Query]:
    filter_generator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filter_generator, map_generator]

    return QueryGenerator(sourceToUse, numberOfQueries, [], distinctOperatorGeneratorStrategies).generate()


def getEquivalentQueries(numberOfQueriesPerGroup: int, sourceToUse: Schema) -> List[Query]:
    # Initialize instances of each generator strategy
    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    map_create_new_field_strategy = MapCreateNewFieldGeneratorStrategy()
    map_substitute_map_expression_strategy = MapSubstituteMapExpressionGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    filter_equivalent_filter_strategy = FilterEquivalentFilterGeneratorStrategy()
    equivalentOperatorGeneratorStrategies = [
        map_expression_reorder_strategy,
        map_operator_reorder_strategy,
        map_create_new_field_strategy,
        map_substitute_map_expression_strategy,
        filter_substitute_map_expression_strategy,
        filter_expression_reorder_strategy, filter_operator_reorder_strategy,
        filter_equivalent_filter_strategy
    ]

    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 4)

    return QueryGenerator(sourceToUse, numberOfQueriesPerGroup, equivalentOperatorGenerators,
                          []).generate()


if __name__ == '__main__':
    run()
