from copy import deepcopy

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import *
from utils.utils import *


class FilterExpressionReorderGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._filterLHSFieldName = None
        self._filterRHSFieldName = None
        self._filter1logicalOp = None
        self._filter1logicalOp = None
        self._filter2logicalOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        if not self._filterLHSFieldName:
            self.__prepareEquivalentFilterExpressions(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        expression1 = LogicalExpression(FieldAccessExpression(self._filterLHSFieldName),
                                        FieldAccessExpression(self._filterRHSFieldName),
                                        self._filter1logicalOp)
        filter1 = FilterOperator(expression1, schema)

        expression2 = LogicalExpression(FieldAccessExpression(self._filterRHSFieldName),
                                        FieldAccessExpression(self._filterLHSFieldName),
                                        self._filter2logicalOp)
        filter2 = FilterOperator(expression2, schema)

        _, filterOp = random_list_element([filter1, filter2])
        return [filterOp]

    def __prepareEquivalentFilterExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        self._filterLHSFieldName = random_field_name(numFields)
        numFields.remove(self._filterLHSFieldName)
        self._filterRHSFieldName = random_field_name(numFields)

        # only considering gt, lt, gte, lte
        _, self._filter1logicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])

        if self._filter1logicalOp == LogicalOperators.lte:
            self._filter2logicalOp = LogicalOperators.gte
        elif self._filter1logicalOp == LogicalOperators.gte:
            self._filter2logicalOp = LogicalOperators.lte
        elif self._filter1logicalOp == LogicalOperators.lt:
            self._filter2logicalOp = LogicalOperators.gt
        elif self._filter1logicalOp == LogicalOperators.gt:
            self._filter2logicalOp = LogicalOperators.lt

        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._filterLHSFieldName not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filterLHSFieldName]:
                self._filterLHSFieldName = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filterRHSFieldName]:
                self._filterRHSFieldName = key
                break

        self._schema = schema
