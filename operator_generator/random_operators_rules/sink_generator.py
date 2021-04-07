from operator_generator.generator_rule import GeneratorRule
from operators.sink_operator import SinkOperator
from utils.contracts import Schema, Operator


class SinkGenerator(GeneratorRule):

    def generate(self, schema: Schema) -> Operator:
        return SinkOperator(schema, "NullOutputSinkDescriptor::create()")
