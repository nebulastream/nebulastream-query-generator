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


@click.command()
@click.option('-cf', '--config-file', help='Location of the configuration file.', type=click.STRING)
def generateQueries(config_file):
    print("Loading configurations")
    file = open(config_file, 'r')
    configuration = yaml.load(file, yaml.Loader)
    print(configuration)
    numberOfQueries = configuration['no_queries']

    possibleSources = []
    for sourceConf in configuration['source_list']:
        source = Schema(name=sourceConf['stream_name'], int_fields=sourceConf['int_fields'],
                        string_fields=sourceConf['string_fields'],
                        timestamp_fields=sourceConf['timestamp_fields'], double_fields=sourceConf['double_fields'])
        possibleSources.append(source)

    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    map_create_new_field_strategy = MapCreateNewFieldGeneratorStrategy()
    map_substitute_map_expression_strategy = MapSubstituteMapExpressionGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    filter_equivalent_filter_strategy = FilterEquivalentFilterGeneratorStrategy()

    filter_generator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filter_generator, map_generator]
    equivalentOperatorGeneratorStrategies = [filter_expression_reorder_strategy, filter_operator_reorder_strategy,
                                             map_expression_reorder_strategy, map_operator_reorder_strategy,
                                             map_create_new_field_strategy, map_substitute_map_expression_strategy,
                                             filter_substitute_map_expression_strategy,
                                             filter_equivalent_filter_strategy]

    generateEquivalentQueries = configuration['generate_equivalent_queries']

    numberOfQueryGroups = 1
    if generateEquivalentQueries:
        numberOfQueryGroups = configuration['equivalence_config']['no_of_equivalent_query_groups']

    queries: List[Query] = []

    numberOfQueries = numberOfQueries / numberOfQueryGroups
    for i in range(numberOfQueryGroups):

        equivalentOperatorGenerators: List[BaseGeneratorStrategy] = []
        if generateEquivalentQueries:
            equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 4)
            distinctOperatorGeneratorStrategies = []

        config = GeneratorConfig(possibleSources=possibleSources,
                                 equivalentOperatorGenerators=equivalentOperatorGenerators,
                                 distinctOperatorGenerators=distinctOperatorGeneratorStrategies,
                                 numberOfQueries=numberOfQueries)
        queries.extend(QueryGenerator(config).generate())

    # Write queries into file
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


if __name__ == '__main__':
    generateQueries()
