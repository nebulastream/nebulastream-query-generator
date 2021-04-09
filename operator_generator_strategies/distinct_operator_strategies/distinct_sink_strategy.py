from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.sink_operator import SinkOperator
from utils.contracts import Schema, Operator


class DistinctSinkGeneratorStrategy(BaseGeneratorStrategy):

    def generate(self, schema: Schema) -> Operator:
        return SinkOperator(schema, "NullOutputSinkDescriptor::create()")
