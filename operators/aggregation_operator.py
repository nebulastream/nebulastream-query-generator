from utils.contracts import Operator, LogicalExpression, Schema
from operators.window_operator import WindowOperator


class AggregationOperator(Operator):
    def __init__(self, aggregation: str, alias: str, window: WindowOperator, schema: Schema):
        super().__init__(schema)
        self._aggregation = aggregation
        self._alias = alias
        self._window = window

    def generate_code(self) -> str:
        code = f"{self._window.generate_code()}.apply({self._aggregation}"
        if self._alias:
            code = f"{code}->as(Attribute(\"{self._alias}\"))"
        return f"{code})"
