from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import *
from utils.utils import *


class FilterExpressionReorderGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._equivalentFilters: List[FilterOperator] = []

    def generate(self, schema: Schema) -> List[Operator]:
        if not self._equivalentFilters:
            self.__prepareEquivalentFilterExpressions(schema)

        _, filterOp = random_list_element(self._equivalentFilters)
        return [filterOp]

    def __prepareEquivalentFilterExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        lhsFieldName = random_field_name(numFields)
        numFields.remove(lhsFieldName)
        rhsFieldName = random_field_name(numFields)

        # only considering gt, lt, gte, lte
        _, logicalOperator = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])

        expression1 = LogicalExpression(FieldAccessExpression(lhsFieldName), FieldAccessExpression(rhsFieldName),
                                        logicalOperator)
        filter1 = FilterOperator(expression1, schema)

        if logicalOperator == LogicalOperators.lte:
            logicalOperator = LogicalOperators.gte
        elif logicalOperator == LogicalOperators.gte:
            logicalOperator = LogicalOperators.lte
        elif logicalOperator == LogicalOperators.lt:
            logicalOperator = LogicalOperators.gt
        elif logicalOperator == LogicalOperators.gt:
            logicalOperator = LogicalOperators.lt

        expression2 = LogicalExpression(FieldAccessExpression(rhsFieldName), FieldAccessExpression(lhsFieldName),
                                        logicalOperator)
        filter2 = FilterOperator(expression2, schema)

        self._equivalentFilters = [filter1, filter2]

    def validation(self, schema: Schema) -> bool:
        if self._assignmentFieldName not in schema.get_numerical_fields():
            return False
        return True