from typing import List

from operator_generator_strategies.base_strategy import BaseStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import Schema, Operator, LogicalExpression, FieldAccessExpression, \
    ConstantExpression, LogicalOperators
from utils.utils import random_list_element


class DistinctFilterStrategy(BaseStrategy):
    def __init__(self, max_number_of_predicates: int = 1):
        super().__init__()
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, schema: Schema) -> List[Operator]:
        _, field = random_list_element(schema.get_numerical_fields())
        _, operator = random_list_element(list(LogicalOperators))
        filter_operator = FilterOperator(
            LogicalExpression(FieldAccessExpression(field), ConstantExpression("10"), operator), schema)
        return [filter_operator]
