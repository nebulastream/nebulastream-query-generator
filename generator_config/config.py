from dataclasses import dataclass
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from utils.contracts import Schema
from utils.utils import random_list_element


@dataclass
class GeneratorConfig:
    possibleSources: List[Schema]
    equivalentOperatorGenerators: List[BaseGeneratorStrategy]
    distinctOperatorGenerators: List[BaseGeneratorStrategy]
    numberOfQueries: int
    max_operator_per_iteration: int = 2

    def choose_random_generator(self) -> BaseGeneratorStrategy:
        return random_list_element(self.generators)[1]
