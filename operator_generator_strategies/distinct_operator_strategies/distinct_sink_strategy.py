from operator_generator_strategies.base_strategy import BaseStrategy
from operators.sink_operator import SinkOperator
from utils.contracts import Schema, Operator


class DistinctSinkStrategy(BaseStrategy):

    def generate(self, schema: Schema) -> Operator:
        return SinkOperator(schema, "NullOutputSinkDescriptor::create()")
