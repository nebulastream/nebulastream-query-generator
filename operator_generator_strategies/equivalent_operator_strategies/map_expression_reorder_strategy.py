from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, ArithmeticOperators, ArithmeticExpression, FieldAccessExpression, \
    FieldAssignmentExpression
from utils.utils import random_list_element, shuffle_list, random_field_name


class MapExpressionReorderGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._assignmentFieldName: str = None
        self._arithmeticOp: ArithmeticOperators = None
        self._expressionFields: List[str] = []
        self._schema: Schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for changing the order of the map expression
        Example:  map(y = x * z) vs map(y = z * x)
        :return:
        """

        if not self._expressionFields:
            self.__initializeRandomlyOrderedMapExpression(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        shuffledExpressionField = shuffle_list(self._expressionFields)

        # Compute the arithmetic expression
        initialExpression = ArithmeticExpression(FieldAccessExpression(shuffledExpressionField[0]),
                                                 FieldAccessExpression(shuffledExpressionField[1]), self._arithmeticOp)
        if len(shuffledExpressionField) > 2:
            for i in range(2, len(shuffledExpressionField)):
                initialExpression = ArithmeticExpression(initialExpression,
                                                         FieldAccessExpression(shuffledExpressionField[i]),
                                                         self._arithmeticOp)

        mapOp = MapOperator(
            FieldAssignmentExpression(FieldAccessExpression(self._assignmentFieldName), initialExpression),
            self._schema)
        return [mapOp]

    def __initializeRandomlyOrderedMapExpression(self, schema: Schema):
        numFields = schema.get_numerical_fields()
        self._assignmentFieldName = random_field_name(numFields)
        _, self._arithmeticOp = random_list_element([ArithmeticOperators.Add, ArithmeticOperators.Mul])

        expressionFields: List[str] = []
        for numField in numFields:
            if not "NEW_" in numField:
                expressionFields.append(numField)

        self._expressionFields = expressionFields
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._assignmentFieldName not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._assignmentFieldName]:
                self._assignmentFieldName = key
                break

        updatedExpressionFields = []
        for field in self._expressionFields:
            for key, value in schema.get_field_name_mapping().items():
                if value == self._schema.get_field_name_mapping()[field]:
                    updatedExpressionFields.append(key)
                    break

        self._expressionFields = updatedExpressionFields
        self._schema = schema
