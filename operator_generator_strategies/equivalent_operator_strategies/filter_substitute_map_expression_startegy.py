from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.map_operator import MapOperator
from utils.contracts import Operator, Schema, FieldAssignmentExpression, FieldAccessExpression, ConstantExpression, \
    ArithmeticOperators, ArithmeticExpression, LogicalExpression, LogicalOperators
from utils.utils import random_list_element, random_int_between


class FilterSubstituteMapExpressionGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._mapToSubstitute: MapOperator = None
        self._substitutedFilterOperators: List[FilterOperator] = []

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
        Example:  map(y = 30).map(x = y* z) vs map(y = 30).map(x= 30*z)
        :param schema:
        :return:
        """
        if not self._substitutedFilterOperators:
            self.__initializeFiltersWithSubstitutedMapExpression(schema)
        _, followUpMap = random_list_element(self._substitutedFilterOperators)
        return [self._mapToSubstitute, followUpMap]

    def __initializeFiltersWithSubstitutedMapExpression(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, originalAssignmentFieldName = random_list_element(numFields)
        numFields.remove(originalAssignmentFieldName)
        contValue = random_int_between(1, 10)
        baseMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(originalAssignmentFieldName),
                                                        ConstantExpression(str(contValue))), schema)

        _, assignmentFieldName1 = random_list_element(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))

        arithExpression1 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                                FieldAccessExpression(originalAssignmentFieldName), arithOperation)
        arithExpression2 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                                ConstantExpression(str(contValue)), arithOperation)

        _, logicalOperation = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])

        followUpFilter1 = FilterOperator(
            LogicalExpression(FieldAccessExpression(assignmentFieldName1), arithExpression1, logicalOperation),
            schema)
        followUpFilter2 = FilterOperator(
            LogicalExpression(FieldAccessExpression(assignmentFieldName1), arithExpression2, logicalOperation),
            schema)

        self._substitutedFilterOperators = [followUpFilter1, followUpFilter2]
        self._mapToSubstitute = baseMap
