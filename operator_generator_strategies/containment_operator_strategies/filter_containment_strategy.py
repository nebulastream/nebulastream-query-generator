from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.map_operator import MapOperator
from utils.contracts import *
from utils.utils import *


class FilterContainmentGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()
        self._filter1LhsField = None
        self._filter1RhsField = None
        self._filter1LogicalOp = None
        self._filter1ArithOp = None
        self._filter2LhsField = None
        self._containedFilterOperationCategory = None
        self._contValue = None
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

        containedFilterOperators = []
        if self._containedFilterOperationCategory == 0:
            # create a filter expression with an integrated map expression
            # e.g. filter(x*6 > y*6) arithmeticExpression1 would be x*6 and two y*6 here
            # random within a containment group
            for i in range(10):
                contValue = random_int_between(1, 10)
                arithExpression1 = ArithmeticExpression(FieldAccessExpression(self._filter1LhsField),
                                                        ConstantExpression(str(contValue)), self._filter1ArithOp)
                arithExpression2 = ArithmeticExpression(FieldAccessExpression(self._filter1RhsField),
                                                        ConstantExpression(str(contValue)), self._filter1ArithOp)
                if self._filter1LogicalOp == LogicalOperators.neq or self._filter1LogicalOp == LogicalOperators.eq:
                    _, self._filter1LogicalOp = random_list_element(
                        [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
                filterOp = FilterOperator(
                    LogicalExpression(arithExpression1, arithExpression2, self._filter1LogicalOp),
                    schema)
                containedFilterOperators.append([filterOp])
        elif self._containedFilterOperationCategory == 1:

            contValue = 10
            arithExpression = ArithmeticExpression(ConstantExpression(str(contValue)),
                                                   FieldAccessExpression(self._filter2LhsField),
                                                   ArithmeticOperators.Mul)
            # creates the same map operation for all equal query groups, e.g. .map(y = y*10)
            mapOp = MapOperator(
                FieldAssignmentExpression(FieldAccessExpression(self._filter2LhsField), arithExpression),
                schema)
            if self._filter1LogicalOp == LogicalOperators.neq or self._filter1LogicalOp == LogicalOperators.eq:
                _, self._filter1LogicalOp = random_list_element(
                    [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
            # adds a random filter operation to the attribute for which the map operation was created
            # e.g. .filter(y<31)
            for i in range(31, 39):
                followUpFilter = FilterOperator(
                    LogicalExpression(FieldAccessExpression(self._filter2LhsField), ConstantExpression(str(i)),
                                      self._filter1LogicalOp), schema)
                containedFilterOperators.append([mapOp, followUpFilter])

        elif self._containedFilterOperationCategory == 2:
            # create 10 randomly contained filter expressions with changing logical operation but same field and constant filter
            # e.g. filter(x < 10), filter(10 >= x)
            # generates one containment relationship per group
            for i in range(10):
                fieldAccess = FieldAccessExpression(self._filter1LhsField)
                constExpression = ConstantExpression(str(self._contValue))

                expressionOrder = random.getrandbits(1)
                filterOp = self._filter1LogicalOp
                if expressionOrder:
                    if self._filter1LogicalOp == LogicalOperators.lt:
                        _, filterOp = random_list_element([LogicalOperators.lt])
                    elif self._filter1LogicalOp == LogicalOperators.gt:
                        _, filterOp = random_list_element([LogicalOperators.gt])
                    elif self._filter1LogicalOp == LogicalOperators.lte:
                        _, filterOp = random_list_element(
                            [LogicalOperators.lte, LogicalOperators.eq, LogicalOperators.lt])
                    elif self._filter1LogicalOp == LogicalOperators.gte:
                        _, filterOp = random_list_element(
                            [LogicalOperators.gte, LogicalOperators.eq, LogicalOperators.gt])
                    elif self._filter1LogicalOp == LogicalOperators.eq:
                        _, filterOp = random_list_element(
                            [LogicalOperators.eq, LogicalOperators.lte, LogicalOperators.gte])
                    elif self._filter1LogicalOp == LogicalOperators.neq:
                        _, filterOp = random_list_element(
                            [LogicalOperators.lt, LogicalOperators.gt])
                    filterOp1 = FilterOperator(LogicalExpression(fieldAccess, constExpression, filterOp),
                                               schema)
                else:
                    if self._filter1LogicalOp == LogicalOperators.gt:
                        _, filterOp = random_list_element([LogicalOperators.lt])
                    elif self._filter1LogicalOp == LogicalOperators.lt:
                        _, filterOp = random_list_element([LogicalOperators.gt])
                    elif self._filter1LogicalOp == LogicalOperators.gte:
                        _, filterOp = random_list_element(
                            [LogicalOperators.lte, LogicalOperators.eq, LogicalOperators.lt])
                    elif self._filter1LogicalOp == LogicalOperators.lte:
                        _, filterOp = random_list_element(
                            [LogicalOperators.gte, LogicalOperators.eq, LogicalOperators.gt])
                    elif self._filter1LogicalOp == LogicalOperators.eq:
                        _, filterOp = random_list_element(
                            [LogicalOperators.eq, LogicalOperators.lte, LogicalOperators.gte])
                    elif self._filter1LogicalOp == LogicalOperators.neq:
                        _, filterOp = random_list_element(
                            [LogicalOperators.lt, LogicalOperators.gt])

                    filterOp1 = FilterOperator(LogicalExpression(constExpression, fieldAccess, filterOp),
                                               schema)
                containedFilterOperators.append([filterOp1])

        elif self._containedFilterOperationCategory == 3:
            # create 10 randomly contained filter expressions
            # e.g. filter(x < 5), filter(x < 10), filter(x <= 50), filter(40 >= x)
            # generates one containment relationship per group
            for i in range(10):
                contValue = random_int_between(1, 100)
                fieldAccess = FieldAccessExpression(self._filter1LhsField)
                constExpression = ConstantExpression(str(contValue))

                expressionOrder = random.getrandbits(1)
                filterOp = self._filter1LogicalOp
                if expressionOrder:
                    if self._filter1LogicalOp == LogicalOperators.lt or self._filter1LogicalOp == LogicalOperators.lte:
                        _, filterOp = random_list_element([LogicalOperators.lt, LogicalOperators.lte])
                    elif self._filter1LogicalOp == LogicalOperators.gt or self._filter1LogicalOp == LogicalOperators.gte:
                        _, filterOp = random_list_element([LogicalOperators.gt, LogicalOperators.gte])
                    elif self._filter1LogicalOp == LogicalOperators.neq or self._filter1LogicalOp == LogicalOperators.eq:
                        _, self._filter1LogicalOp = random_list_element(
                            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
                        filterOp = self._filter1LogicalOp
                    filterOp1 = FilterOperator(LogicalExpression(fieldAccess, constExpression, filterOp),
                                               schema)
                else:
                    if self._filter1LogicalOp == LogicalOperators.lt or self._filter1LogicalOp == LogicalOperators.lte:
                        _, filterOp = random_list_element([LogicalOperators.gt, LogicalOperators.gte])
                    elif self._filter1LogicalOp == LogicalOperators.gt or self._filter1LogicalOp == LogicalOperators.gte:
                        _, filterOp = random_list_element([LogicalOperators.lt, LogicalOperators.lte])
                    elif self._filter1LogicalOp == LogicalOperators.neq or self._filter1LogicalOp == LogicalOperators.eq:
                        _, self._filter1LogicalOp = random_list_element(
                            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
                        filterOp = self._filter1LogicalOp

                    filterOp1 = FilterOperator(LogicalExpression(constExpression, fieldAccess, filterOp),
                                               schema)
                containedFilterOperators.append([filterOp1])
        _, containedFilter = random_list_element(containedFilterOperators)
        return containedFilter

    def __initializeEquivalentFilters(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        intFields = schemaCopy.int_fields

        self._filter2LhsField = random_field_name(intFields)
        self._filter1LhsField = random_field_name(intFields)
        intFields.remove(self._filter1LhsField)
        if len(intFields) > 0:
            self._filter1RhsField = random_field_name(intFields)
            self._containedFilterOperationCategory = random_int_between(0, 3)
        else:
            self._containedFilterOperationCategory = random_int_between(1, 3)
        if self._containedFilterOperationCategory == 2:
            self._contValue = random_int_between(1, 100)
        _, self._filter1ArithOp = random_list_element(list(ArithmeticOperators))
        _, self._filter1LogicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._filter1LhsField not in schema.get_numerical_fields():
            return False
        elif self._filter1RhsField not in schema.get_numerical_fields():
            return False
        elif self._filter2LhsField not in schema.get_numerical_fields():
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
