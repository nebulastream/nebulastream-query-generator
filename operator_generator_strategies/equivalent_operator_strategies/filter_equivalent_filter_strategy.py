from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.map_operator import MapOperator
from utils.contracts import *
from utils.utils import *


class FilterEquivalentFilterGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):

        self._filter1LhsField = None
        self._filter1RhsField = None
        self._filter1LogicalOp = None
        self._filter1ArithOp = None
        self._filter2LhsField = None
        self._sendMapAndFilter = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        Queries with similar filters (here y is an Integer value):
        Examples:
        1. map(y = y*10).filter(y<31) vs map(y = y*10).filter(y<35)
        2. filter(a*2>b*3) vs filter(a*4>b*6)
        :param schema:
        :return:
        """
        if not self._filter1LhsField:
            self.__initializeEquivalentFilters(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        equivalentFilterOperators = []
        if self._sendMapAndFilter:
            for i in range(10):
                contValue = random_int_between(1, 10)
                arithExpression1 = ArithmeticExpression(FieldAccessExpression(self._filter1LhsField),
                                                        ConstantExpression(str(contValue)), self._filter1ArithOp)
                arithExpression2 = ArithmeticExpression(FieldAccessExpression(self._filter1RhsField),
                                                        ConstantExpression(str(contValue)), self._filter1ArithOp)
                filterOp = FilterOperator(
                    LogicalExpression(arithExpression1, arithExpression2, self._filter1LogicalOp),
                    schema)
                equivalentFilterOperators.append([filterOp])
        else:

            contValue = 10
            arithExpression = ArithmeticExpression(ConstantExpression(str(contValue)),
                                                   FieldAccessExpression(self._filter2LhsField),
                                                   ArithmeticOperators.Mul)
            mapOp = MapOperator(
                FieldAssignmentExpression(FieldAccessExpression(self._filter2LhsField), arithExpression),
                schema)
            for i in range(31, 39):
                followUpFilter = FilterOperator(
                    LogicalExpression(FieldAccessExpression(self._filter2LhsField), ConstantExpression(str(i)),
                                      LogicalOperators.lt), schema)
                equivalentFilterOperators.append([mapOp, followUpFilter])

        _, filterOps = random_list_element(equivalentFilterOperators)
        return filterOps

    def __initializeEquivalentFilters(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        intFields = schemaCopy.int_fields

        self._filter2LhsField = random_field_name(intFields)
        self._filter1LhsField = random_field_name(intFields)
        intFields.remove(self._filter1LhsField)
        self._filter1RhsField = random_field_name(intFields)
        _, self._filter1ArithOp = random_list_element(list(ArithmeticOperators))
        _, self._filter1LogicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
        self._sendMapAndFilter = random.getrandbits(1)
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._filter1LhsField not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filter1LhsField]:
                self._filter1LhsField = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filter1RhsField]:
                self._filter1RhsField = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._filter2LhsField]:
                self._filter2LhsField = key
                break

        self._schema = schema
