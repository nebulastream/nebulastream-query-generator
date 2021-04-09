from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.source_operator import SourceOperator
from utils.contracts import Schema, Operator


class DistinctSourceGeneratorStrategy(BaseGeneratorStrategy):

    def generate(self, schema: Schema) -> Operator:
        return SourceOperator(schema)
