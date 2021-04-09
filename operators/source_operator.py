from utils.contracts import Schema, Operator


class SourceOperator(Operator):
    def __init__(self, schema: Schema):
        super().__init__(schema)

    def generate_code(self) -> str:
        return f"Query::from(\"{self.output_schema.name}\")"
