from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.map_operator import MapOperator
from utils.contracts import *
from utils.utils import *


class FilterEquivalentFilterGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._equivalentFilterOperators: List[List[Operator]] = []

    def generate(self, schema: Schema) -> List[Operator]:
        """
        Queries with similar filters (here y is an Integer value):
        Examples:
        1. map(y = y*10).filter(y<31) vs map(y = y*10).filter(y<35)
        2. filter(a*2>b*3) vs filter(a*4>b*6)
        :param schema:
        :return:
        """
        if not self._equivalentFilterOperators:
            self.__initializeEquivalentFilters(schema)

        _, filterOps = random_list_element(self._equivalentFilterOperators)
        return filterOps

    def __initializeEquivalentFilters(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        option = random_int_between(1, 2)
        intFields = schemaCopy.int_fields

        if option == 1:
            assignmentFieldName1 = random_field_name(intFields)
            intFields.remove(assignmentFieldName1)
            assignmentFieldName2 = random_field_name(intFields)
            _, arithOperation = random_list_element(list(ArithmeticOperators))

            _, logicalOperation = random_list_element(
                [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])

            for i in range(10):
                contValue = random_int_between(1, 10)
                arithExpression1 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                                        ConstantExpression(str(contValue)), arithOperation)
                arithExpression2 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName2),
                                                        ConstantExpression(str(contValue)), arithOperation)
                filterOp = FilterOperator(
                    LogicalExpression(arithExpression1, arithExpression2, logicalOperation),
                    schema)
                self._equivalentFilterOperators.append([filterOp])
        else:

            assignmentField = random_field_name(intFields)
            contValue = 10
            arithExpression = ArithmeticExpression(ConstantExpression(str(contValue)),
                                                   FieldAccessExpression(assignmentField), ArithmeticOperators.Mul)
            mapOp = MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentField), arithExpression),
                                schema)
            for i in range(31, 39):
                followUpFilter = FilterOperator(
                    LogicalExpression(FieldAccessExpression(assignmentField), ConstantExpression(str(i)),
                                      LogicalOperators.lt), schema)
                self._equivalentFilterOperators.append([mapOp, followUpFilter])


    def validation(self, schema: Schema) -> bool:
        if self._assignmentFieldName not in schema.get_numerical_fields():
            return False
        return True