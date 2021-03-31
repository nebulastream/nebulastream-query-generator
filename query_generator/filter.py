from dataclasses import dataclass
from typing import List

from query_generator.contracts import OperatorFactory, Schema, Operator
from query_generator.utils import random_list_element


@dataclass
class Predicate:
    attribute: str
    condition: str
    value: str


class FilterOperator(Operator):
    def __init__(self, predicates: List[Predicate], schema: Schema):
        self._predicates = predicates
        self._output_schema = schema

    def output_schema(self) -> Schema:
        return self._output_schema

    def generate_code(self) -> str:
        predicate_str = ""
        for (idx, predicate) in enumerate(self._predicates):
            predicate_str += f'Attribute("{predicate.attribute}") {predicate.condition} {predicate.value}'
            if idx != len(self._predicates) - 1:
                predicate_str = f"{predicate_str} && "

        return f"filter({predicate_str})"


class FilterFactory(OperatorFactory):
    def __init__(self, max_number_of_predicates: int = 1):
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, input_schema: Schema) -> Operator:
        predicates = []
        for _ in range(self._max_number_of_predicates):
            _, field = random_list_element(input_schema.get_numerical_fields())
            predicates.append(Predicate(field, ">", "10"))
        return FilterOperator(predicates, input_schema)
