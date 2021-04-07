from dataclasses import dataclass
from typing import List

from operator_generator.generator_rule import GeneratorRule
from utils.contracts import Schema
from utils.utils import random_list_element


@dataclass
class GeneratorConfig:
    possibleSources: List[Schema]
    equivalentOperatorGenerators: List[GeneratorRule]
    distinctOperatorGenerators: List[GeneratorRule]
    numberOfQueries: int
    max_operator_per_iteration: int = 2

    def choose_random_generator(self) -> GeneratorRule:
        return random_list_element(self.generators)[1]
