from operators.sink_operator import SinkOperator
from utils.contracts import OperatorFactory, Schema, Operator


class SinkFactory(OperatorFactory):
    def __init__(self):
        pass

    def generate(self, input_schema: Schema) -> Operator:
        return SinkOperator(input_schema, "NullOutputSinkDescriptor::create()")
