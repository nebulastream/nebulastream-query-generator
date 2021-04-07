from operator_generator.generator_rule import GeneratorRule
from operators.source_operator import SourceOperator
from utils.contracts import Schema, Operator


class SourceGenerator(GeneratorRule):

    def generate(self, schema: Schema) -> Operator:
        return SourceOperator(schema)
