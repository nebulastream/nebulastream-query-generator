from typing import List

from utils.contracts import Operator, Schema
from operators.window_operator import WindowOperator


class AggregationOperator(Operator):
    def __init__(self, aggregations: List[str], alias: str, window: WindowOperator, schema: Schema):
        super().__init__(schema)
        self._aggregations = aggregations
        self._alias = alias
        self._window = window

    def generate_code(self) -> str:
        code = f"{self._window.generate_code()}.apply({self._aggregations[0]}"
        if self._alias:
            code = f"{code}->as(Attribute(\"{self._alias}\"))"
        if len(self._aggregations) > 1:
            for i in range(1, len(self._aggregations)):
                code = f"{code}, {self._aggregations[i]}"
                if len(self._alias) > 1 and self._alias[i]:
                    code = f"{code}->as(Attribute(\"{self._alias[i]}\"))"
        return f"{code})"
