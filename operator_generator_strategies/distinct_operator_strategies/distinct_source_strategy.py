from operator_generator_strategies.base_strategy import BaseStrategy
from operators.source_operator import SourceOperator
from utils.contracts import Schema, Operator


class DistinctSourceStrategy(BaseStrategy):

    def generate(self, schema: Schema) -> Operator:
        return SourceOperator(schema)
