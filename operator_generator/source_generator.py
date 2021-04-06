from operators.source_operator import SourceOperator
from utils.contracts import OperatorFactory, Schema, Operator


class SourceFactory(OperatorFactory):
    def __init__(self):
        pass

    def generate(self, input_schema: Schema) -> Operator:
        return SourceOperator(input_schema)
