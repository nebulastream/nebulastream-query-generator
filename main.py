import click
import yaml

from operator_generator_strategies.equivalent_operator_strategies.map_rule import MapRulesInitializer
from generator_config.config import GeneratorConfig
from utils.contracts import Schema
from operator_generator_strategies.distinct_operator_strategies.distinct_filter_strategy import DistinctFilterStrategy
from query_generator.generator import QueryGenerator
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy import DistinctMapStrategy


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

    filter_generator = DistinctFilterStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapStrategy()
    config = GeneratorConfig(possibleSources=possibleSources,
                             equivalentOperatorGenerators=[filter_generator, map_generator],
                             distinctOperatorGenerators=[filter_generator, map_generator],
                             numberOfQueries=numberOfQueries)
    queries = QueryGenerator(config).generate()
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


if __name__ == '__main__':
    generateQueries()
