from dataclasses import dataclass
from typing import List

from query_generator.contracts import *
from query_generator.utils import random_list_element


class FilterOperator(Operator):
    def __init__(self, predicate: LogicalExpression, schema: Schema):
        super().__init__(schema)
        self._predicate = predicate

    def generate_code(self) -> str:
        return f"filter({self._predicate.generate_code()})"


class FilterFactory(OperatorFactory):
    def __init__(self, max_number_of_predicates: int = 1):
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, input_schema: Schema) -> Operator:
        _, field = random_list_element(input_schema.get_numerical_fields())
        _, operator = random_list_element(list(LogicalOperators))
        return FilterOperator(LogicalExpression(FieldAccessExpression(field), ConstantExpression("10"), operator),
                              input_schema)
