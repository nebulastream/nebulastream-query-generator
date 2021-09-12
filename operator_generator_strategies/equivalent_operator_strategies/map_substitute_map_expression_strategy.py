from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, FieldAssignmentExpression, FieldAccessExpression, ConstantExpression, \
    ArithmeticOperators, ArithmeticExpression
from utils.utils import random_list_element, random_int_between, random_field_name


class MapSubstituteMapExpressionGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._subMapField = None
        self._constantValue = None
        self._followUpAssignmentField = None
        self._arithOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
        Example:  map(y = 30).map(x = y* z) vs map(y = 30).map(x= 30*z)
        :param schema:
        :return:
        """

        if len(schema.get_numerical_fields()) == 1:
            print("Skipping MapSubstituteMapExpressionGeneratorStrategy as only 1 field is present in the schema")
            return []

        if not self._subMapField:
            self.__initializeMapsWithComplexArithmeticExpressions(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        baseMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(self._subMapField),
                                                        ConstantExpression(str(self._constantValue))), schema)

        expression1 = ArithmeticExpression(FieldAccessExpression(self._followUpAssignmentField),
                                           FieldAccessExpression(self._subMapField), self._arithOp)
        expression2 = ArithmeticExpression(FieldAccessExpression(self._followUpAssignmentField),
                                           ConstantExpression(str(self._constantValue)), self._arithOp)

        followUpMap1 = MapOperator(
            FieldAssignmentExpression(FieldAccessExpression(self._followUpAssignmentField), expression1),
            schema)
        followUpMap2 = MapOperator(
            FieldAssignmentExpression(FieldAccessExpression(self._followUpAssignmentField), expression2),
            schema)

        _, followUpMap = random_list_element([followUpMap1, followUpMap2])
        return [baseMap, followUpMap]

    def __initializeMapsWithComplexArithmeticExpressions(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        originalAssignmentFieldName = random_field_name(numFields)
        numFields.remove(originalAssignmentFieldName)
        self._subMapField = originalAssignmentFieldName
        self._constantValue = random_int_between(1, 10)
        self._followUpAssignmentField = random_field_name(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        self._arithOp = arithOperation
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._subMapField not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._subMapField]:
                self._subMapField = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._followUpAssignmentField]:
                self._followUpAssignmentField = key
                break

        self._schema = schema
