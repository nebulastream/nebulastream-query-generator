import click
import yaml
from query_generator.config import GeneratorConfig
from query_generator.contracts import Schema
from query_generator.filter import FilterFactory
from query_generator.generator import QueryGenerator
from query_generator.map import MapFactory


@click.command()
@click.option('-cf','--config-file', help='Location of the configuration file.', type=click.STRING)
def generateQueries(config_file):
    print("Generating queries")
    file = open(config_file, 'r')
    configuration = yaml.load(file, yaml.Loader)
    print(configuration)


if __name__ == '__main__':
    generateQueries()
    car_source = Schema(name="car", int_fields=["speed", "distance_travelled"], string_fields=["name"],
                        timestamp_fields=["event_time"], double_fields=[])
    filter_generator = FilterFactory(max_number_of_predicates=2)
    map_generator = MapFactory()
    config = GeneratorConfig(possible_sources=[car_source], generators=[filter_generator, map_generator],
                             number_of_queries=100)
    queries = QueryGenerator(config).generate()
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query)
            f.write("\n")
