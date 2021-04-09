from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import Operator, LogicalOperators, LogicalExpression, FieldAccessExpression, ConstantExpression, \
    Schema
from utils.utils import *


class FilterOperatorReorderGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._filterOperators: List[FilterOperator] = []

    def generate(self, schema: Schema) -> List[Operator]:
        if not self._filterOperators:
            self.__prepareEquivalentFilterExpressions(schema)
        return shuffle_list(self._filterOperators)

    def __prepareEquivalentFilterExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        fieldName1 = random_field_name(numFields)
        numFields.remove(fieldName1)
        fieldName2 = random_field_name(numFields)

        # only considering gt, lt, gte, lte, eq
        _, logicalOperator = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte, LogicalOperators.eq])

        constant = random_int_between(10, 100)

        expression1 = LogicalExpression(FieldAccessExpression(fieldName1), ConstantExpression(str(constant)),
                                        logicalOperator)
        filter1 = FilterOperator(expression1, schema)

        # only considering gt, lt, gte, lte, eq
        _, logicalOperator = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte, LogicalOperators.eq])
        constant = random_int_between(10, 100)
        expression2 = LogicalExpression(FieldAccessExpression(fieldName2), ConstantExpression(str(constant)),
                                        logicalOperator)
        filter2 = FilterOperator(expression2, schema)

        self._filterOperators = [filter1, filter2]
