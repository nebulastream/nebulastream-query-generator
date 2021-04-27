from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import Schema, Operator, LogicalExpression, FieldAccessExpression, \
    ConstantExpression, LogicalOperators
from utils.utils import random_list_element, random_int_between


class DistinctFilterGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, max_number_of_predicates: int = 1):
        super().__init__()
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, schema: Schema) -> List[Operator]:
        _, field = random_list_element(schema.get_numerical_fields())
        constant = random_int_between(1, 1000)
        _, operator = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
        filter_operator = FilterOperator(
            LogicalExpression(FieldAccessExpression(field), ConstantExpression(constant), operator), schema)
        return [filter_operator]
