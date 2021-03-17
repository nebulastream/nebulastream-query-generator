from query_generator.contracts import OperatorFactory, Schema, Operator


class SourceOperator(Operator):
    def __init__(self, schema: Schema):
        self._stream_schema = schema

    def output_schema(self) -> Schema:
        return self._stream_schema

    def generate_code(self) -> str:
        return f"Query::from(\"{self._stream_schema.name}\")"


class SourceFactory(OperatorFactory):
    def __init__(self):
        pass

    def generate(self, input_schema: Schema) -> Operator:
        return SourceOperator(input_schema)
