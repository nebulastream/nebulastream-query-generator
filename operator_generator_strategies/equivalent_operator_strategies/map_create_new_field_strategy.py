from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, ArithmeticExpression, FieldAccessExpression, ArithmeticOperators, \
    FieldAssignmentExpression
from utils.utils import random_list_element, random_int_between


class MapCreateNewFieldGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._assigneeField = None
        self._assignmentField1 = None
        self._assignmentField2 = None
        self._arithOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for adding a map operator with a new field name with a fixed formula
        Example: map(a1 = b *2) vs map(a2 = b * 2)
        :param schema: the schema used for preparing the operator lists
        :return: one of the randomly selected map operator
        """
        if not self._assigneeField:
            self.__initializeMapsWithNewFieldAndSameExpression(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        expression = ArithmeticExpression(FieldAccessExpression(self._assignmentField1),
                                          FieldAccessExpression(self._assignmentField2), self._arithOp)
        mapOperators = []
        for i in range(10):
            newFiledName = f"NEW_{self._assigneeField}{random_int_between(1, 10)}"
            updateSchema = deepcopy(self._schema)
            updateSchema.int_fields.append(newFiledName)
            mapWithNewFieldAndSameExpression = MapOperator(
                FieldAssignmentExpression(FieldAccessExpression(newFiledName), expression),
                updateSchema)
            mapOperators.append(mapWithNewFieldAndSameExpression)

        _, filterOp = random_list_element(mapOperators)
        return [filterOp]

    def __initializeMapsWithNewFieldAndSameExpression(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, assigneeFieldName = random_list_element(numFields)
        self._assigneeField = assigneeFieldName
        numFields.remove(assigneeFieldName)
        _, assignmentFieldName1 = random_list_element(numFields)
        self._assignmentField1 = assignmentFieldName1
        _, assignmentFieldName2 = random_list_element(numFields)
        self._assignmentField2 = assignmentFieldName2
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        self._arithOp = arithOperation
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._assigneeField not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._assignmentField1]:
                self._assignmentField1 = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._assignmentField2]:
                self._assignmentField2 = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._assigneeField]:
                self._assigneeField = key
                break

        self._schema = schema
