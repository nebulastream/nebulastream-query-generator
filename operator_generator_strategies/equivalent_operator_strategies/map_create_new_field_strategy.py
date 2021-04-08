from copy import deepcopy
from typing import List

from operator_generator_strategies.base_strategy import BaseStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, ArithmeticExpression, FieldAccessExpression, ArithmeticOperators, \
    FieldAssignmentExpression
from utils.utils import random_list_element, random_int_between


class MapCreateNewFieldStrategy(BaseStrategy):

    def __init__(self):
        self._mapOperators: List[MapOperator] = []

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for adding a map operator with a new field name with a fixed formula
        Example: map(a = b *2) vs map(x = b * 2)
        :param schema: the schema used for preparing the operator lists
        :return: one of the randomly selected map operator
        """
        if not self._mapOperators:
            self.__initializeMapsWithNewFieldAndSameExpression(schema)
        _, filterOp = random_list_element(self._mapOperators)
        return [filterOp]

    def __initializeMapsWithNewFieldAndSameExpression(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, originalAssignmentFieldName = random_list_element(numFields)
        numFields.remove(originalAssignmentFieldName)
        _, assignmentFieldName1 = random_list_element(numFields)
        _, assignmentFieldName2 = random_list_element(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))

        expression = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                          FieldAccessExpression(assignmentFieldName2), arithOperation)
        mapsWithNewFieldAndSameExpression = []
        for i in range(10):
            newFiledName = f"{originalAssignmentFieldName}{random_int_between(1, 10)}"
            updateSchema = deepcopy(schema)
            updateSchema.int_fields.append(newFiledName)
            mapWithNewFieldAndSameExpression = MapOperator(
                FieldAssignmentExpression(FieldAccessExpression(newFiledName), expression),
                updateSchema)
            mapsWithNewFieldAndSameExpression.append(mapWithNewFieldAndSameExpression)

        self._mapOperators = mapsWithNewFieldAndSameExpression
