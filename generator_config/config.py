from dataclasses import dataclass
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from utils.contracts import Schema


# Note: Currently not in use. We can change its usage in next issues
@dataclass
class GeneratorConfig:
    possibleSources: List[Schema]
    equivalentOperatorGenerators: List[BaseGeneratorStrategy]
    distinctOperatorGenerators: List[BaseGeneratorStrategy]
    numberOfQueries: int
