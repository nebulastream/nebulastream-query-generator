from typing import List

from operator_generator_strategies.base_strategy import BaseStrategy
from utils.contracts import Schema, Operator


class EquivalentProjectOrderingStrategy(BaseStrategy):

    def __init__(self):
        pass

    def generate(self, schema: Schema) -> List[Operator]:
        pass
