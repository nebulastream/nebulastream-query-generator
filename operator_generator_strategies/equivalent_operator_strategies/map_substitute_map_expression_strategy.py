from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, FieldAssignmentExpression, FieldAccessExpression, ConstantExpression, \
    ArithmeticOperators, ArithmeticExpression
from utils.utils import random_list_element, random_int_between


class MapSubstituteMapExpressionGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._mapToSubstitute: MapOperator = None
        self._substitutedMapOperators: List[MapOperator] = []

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
        Example:  map(y = 30).map(x = y* z) vs map(y = 30).map(x= 30*z)
        :param schema:
        :return:
        """
        if not self._substitutedMapOperators:
            self.__initializeMapsWithComplexArithmeticExpressions(schema)
        _, followUpMap = random_list_element(self._substitutedMapOperators)
        return [self._mapToSubstitute, followUpMap]

    def __initializeMapsWithComplexArithmeticExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, originalAssignmentFieldName = random_list_element(numFields)
        numFields.remove(originalAssignmentFieldName)
        contValue = random_int_between(1, 10)
        baseMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(originalAssignmentFieldName),
                                                        ConstantExpression(str(contValue))), schema)

        _, assignmentFieldName1 = random_list_element(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression1 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                           FieldAccessExpression(originalAssignmentFieldName), arithOperation)
        expression2 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                           ConstantExpression(str(contValue)), arithOperation)

        followUpMap1 = MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentFieldName1), expression1),
                                   schema)
        followUpMap2 = MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentFieldName1), expression2),
                                   schema)

        self._substitutedMapOperators = [followUpMap1, followUpMap2]
        self._mapToSubstitute = baseMap
