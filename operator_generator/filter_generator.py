from operators.filter_operator import FilterOperator
from utils.contracts import OperatorFactory, Schema, Operator, LogicalExpression, FieldAccessExpression, \
    ConstantExpression, LogicalOperators
from utils.utils import random_list_element


class FilterFactory(OperatorFactory):
    def __init__(self, max_number_of_predicates: int = 1):
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, input_schema: Schema) -> Operator:
        _, field = random_list_element(input_schema.get_numerical_fields())
        _, operator = random_list_element(list(LogicalOperators))
        return FilterOperator(LogicalExpression(FieldAccessExpression(field), ConstantExpression("10"), operator),
                              input_schema)
