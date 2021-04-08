from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from utils.contracts import Schema, Operator


class EquivalentProjectOrderingGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        pass

    def generate(self, schema: Schema) -> List[Operator]:
        pass
