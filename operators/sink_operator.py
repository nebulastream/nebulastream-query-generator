from utils.contracts import Schema, Operator


class SinkOperator(Operator):
    def __init__(self, schema: Schema, sink_reference: str):
        super().__init__(schema)
        self._sink_reference = sink_reference

    def generate_code(self) -> str:
        return f"sink({self._sink_reference});"
