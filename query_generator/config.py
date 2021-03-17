from dataclasses import dataclass
from typing import List

from query_generator.contracts import Schema, OperatorFactory
from query_generator.utils import random_list_element


@dataclass
class GeneratorConfig:
    possible_sources: List[Schema]
    generators: List[OperatorFactory]
    number_of_queries: int
    max_operator_per_iteration: int = 2

    def choose_random_generator(self) -> OperatorFactory:
        return random_list_element(self.generators)[1]
