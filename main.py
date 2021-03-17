from query_generator.config import GeneratorConfig
from query_generator.contracts import Schema
from query_generator.filter import FilterFactory
from query_generator.generator import QueryGenerator

if __name__ == '__main__':
    car_source = Schema(name="car", int_fields=["speed", "distance_travelled"], string_fields=["name"],
                        timestamp_fields=["event_time"], double_fields=[])
    filter_generator = FilterFactory(max_number_of_predicates=2)
    config = GeneratorConfig(possible_sources=[car_source], generators=[filter_generator], number_of_queries=100)
    queries = QueryGenerator(config).generate()
    for query in queries:
        print(query)
