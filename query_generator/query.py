from typing import List

from utils.contracts import Operator, Schema


class Query():
    def __init__(self):
        self._operators: List[Operator] = []

    def add_operator(self, operator: Operator) -> "Query":
        self._operators.append(operator)
        return self

    def generate_code(self) -> str:
        code = ""
        for idx, operator in enumerate(self._operators):
            code += f"{operator.generate_code()}"
            if idx < len(self._operators) - 1:
                code += "."
        return code

    def output_schema(self) -> Schema:
        return self._operators[-1].get_output_schema()
