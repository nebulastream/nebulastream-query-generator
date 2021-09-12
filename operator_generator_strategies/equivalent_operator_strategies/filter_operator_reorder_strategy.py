from copy import deepcopy

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from utils.contracts import Operator, LogicalOperators, LogicalExpression, FieldAccessExpression, ConstantExpression, \
    Schema
from utils.utils import *


class FilterOperatorReorderGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._filter1Field = None
        self._filter2Field = None
        self._filter1Const = None
        self._filter2Const = None
        self._filter1LogicalOp = None
        self._filter2LogicalOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        if not self._filter1Field:
            self.__prepareEquivalentFilterExpressions(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        expression1 = LogicalExpression(FieldAccessExpression(self._filter1Field),
                                        ConstantExpression(str(self._filter1Const)),
                                        self._filter1LogicalOp)
        filter1 = FilterOperator(expression1, schema)

        expression2 = LogicalExpression(FieldAccessExpression(self._filter2Field),
                                        ConstantExpression(str(self._filter2Const)),
                                        self._filter2LogicalOp)
        filter2 = FilterOperator(expression2, schema)

        return shuffle_list([filter1, filter2])

    def __prepareEquivalentFilterExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        self._filter1Field = random_field_name(numFields)
        numFields.remove(self._filter1Field)
        self._filter2Field = random_field_name(numFields)

        # only considering gt, lt, gte, lte, eq
        _, self._filter1LogicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte, LogicalOperators.eq])

        self._filter1Const = random_int_between(10, 100)

        # only considering gt, lt, gte, lte, eq
        _, self._filter2LogicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte, LogicalOperators.eq])
        self._filter2Const = random_int_between(10, 100)
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._filter1Field not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filter1Field]:
                self._filter1Field = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filter2Field]:
                self._filter2Field = key
                break

        self._schema = schema
