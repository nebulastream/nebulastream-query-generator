import click
import yaml

from equivalence_rule.map_rule import MapRulesInitializer
from query_generator.config import GeneratorConfig
from query_generator.contracts import Schema
from query_generator.filter import FilterFactory
from query_generator.generator import QueryGenerator
from query_generator.map import MapFactory


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

    mparuleInit = MapRulesInitializer(possibleSources[0])

    for i in range(1):
        output = mparuleInit._mapRule.new_field_with_same_expression()
        print(output.generate_code())

    for i in range(1):
        outputList = mparuleInit._mapRule.complex_arithmetic_expression()
        print(outputList[0].generate_code())
        print(outputList[1].generate_code())

    for i in range(1):
        outputList = mparuleInit._mapRule.map_operator_reordering()
        print(outputList[0].generate_code())
        print(outputList[1].generate_code())

    for i in range(11):
        map = mparuleInit._mapRule.map_expression_reordering()
        print(map.generate_code())

    filter_generator = FilterFactory(max_number_of_predicates=2)
    map_generator = MapFactory()
    config = GeneratorConfig(possibleSources=possibleSources, generators=[filter_generator, map_generator],
                             numberOfQueries=numberOfQueries)
    queries = QueryGenerator(config).generate()
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query)
            f.write("\n")


if __name__ == '__main__':
    generateQueries()
