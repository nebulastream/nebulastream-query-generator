from query_generator.contracts import OperatorFactory, Schema, Operator


class SinkOperator(Operator):
    def __init__(self, schema: Schema, sink_reference: str):
        self._schema = schema
        self._sink_reference = sink_reference

    def output_schema(self) -> Schema:
        return self._schema

    def generate_code(self) -> str:
        return f"addSink({self._sink_reference})"


class SinkFactory(OperatorFactory):
    def __init__(self):
        pass

    def generate(self, input_schema: Schema) -> Operator:
        return SinkOperator(input_schema, "NullOutputSinkDescriptor::create()")
