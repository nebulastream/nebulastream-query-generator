from dataclasses import dataclass
from typing import List

from utils.contracts import Schema, OperatorFactory
from utils.utils import random_list_element


@dataclass
class GeneratorConfig:
    possibleSources: List[Schema]
    generators: List[OperatorFactory]
    numberOfQueries: int
    max_operator_per_iteration: int = 2

    def choose_random_generator(self) -> OperatorFactory:
        return random_list_element(self.generators)[1]
