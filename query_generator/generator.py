import random
from copy import deepcopy
from typing import List

from generator_config.config import GeneratorConfig
from query_generator.query import Query
from operator_generator.sink_generator import SinkFactory
from operator_generator.source_generator import SourceOperator
from utils.utils import random_list_element


class QueryGenerator:
    def __init__(self, config: GeneratorConfig):
        self._config = config
        self._queries: List[Query] = []

    def generate(self) -> List[str]:
        self._inject_source_operators()
        while len(self._queries) < self._config.numberOfQueries:
            _, query = self._choose_query_for_modification()
            new_query = self._append_new_operators(query)
            self._queries.append(new_query)
        self._inject_sink_operator()
        return [query.generate_code() for query in self._queries]

    def _append_new_operators(self, query: Query) -> Query:
        new_query = deepcopy(query)
        for _ in range(random.randint(1, self._config.max_operator_per_iteration + 1)):
            new_query.add_operator(self._config.choose_random_generator().generate(query.output_schema()))
        return new_query

    def _inject_source_operators(self):
        for source in self._config.possibleSources:
            self._queries.append(Query().add_operator(SourceOperator(source)))

    def _choose_query_for_modification(self) -> (int, Query):
        return random_list_element(self._queries)

    def _randomly_remove_query(self, query_idx: int):
        if isinstance(self._queries[query_idx], SourceOperator):
            return
        if random.randint(0, 10) / 5 == 0:
            del self._queries[query_idx]

    def _inject_sink_operator(self):
        s = SinkFactory()
        for query in self._queries:
            query.add_operator(s.generate(query.output_schema()))
