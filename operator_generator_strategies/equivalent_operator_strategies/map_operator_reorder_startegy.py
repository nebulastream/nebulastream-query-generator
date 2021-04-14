from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, ArithmeticOperators, FieldAccessExpression, ArithmeticExpression, \
    ConstantExpression, FieldAssignmentExpression
from utils.utils import random_list_element, random_int_between, shuffle_list, random_field_name


class MapOperatorReorderGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._mapOperators: List[MapOperator] = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for changing the order of two map operators
        Example:  map(y = 30).map(x = 30) vs map(x = 30).map(y= 30)
        :return:
        """
        if not self._mapOperators:
            self.__initializeRandomlyOrderedMaps(schema)

        return shuffle_list(self._mapOperators)

    def __initializeRandomlyOrderedMaps(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        firstFieldName = random_field_name(numFields)
        numFields.remove(firstFieldName)
        contValue1 = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression1 = ArithmeticExpression(FieldAccessExpression(firstFieldName),
                                           ConstantExpression(str(contValue1)), arithOperation)
        firstMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(firstFieldName),
                                                         expression1), schema)

        secondFieldName = random_field_name(numFields)
        contValue2 = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression2 = ArithmeticExpression(FieldAccessExpression(secondFieldName),
                                           ConstantExpression(str(contValue2)), arithOperation)
        secondMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(secondFieldName),
                                                          expression2), schema)
        self._mapOperators = [firstMap, secondMap]
