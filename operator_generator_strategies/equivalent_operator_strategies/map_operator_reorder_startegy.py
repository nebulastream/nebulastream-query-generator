from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import Schema, Operator, ArithmeticOperators, FieldAccessExpression, ArithmeticExpression, \
    ConstantExpression, FieldAssignmentExpression
from utils.utils import random_list_element, random_int_between, shuffle_list, random_field_name


class MapOperatorReorderGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._map1firstFieldName = None
        self._map1Constant = None
        self._map1ArithOp = None
        self._map2firstFieldName = None
        self._map2Constant = None
        self._map2ArithOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for changing the order of two map operators
        Example:  map(y = 30).map(x = 30) vs map(x = 30).map(y= 30)
        :return:
        """
        if not self._map1firstFieldName:
            self.__initializeRandomlyOrderedMaps(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        expression1 = ArithmeticExpression(FieldAccessExpression(self._map1firstFieldName),
                                           ConstantExpression(str(self._map1Constant)), self._map1ArithOp)
        firstMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(self._map1firstFieldName),
                                                         expression1), schema)

        expression2 = ArithmeticExpression(FieldAccessExpression(self._map2firstFieldName),
                                           ConstantExpression(str(self._map2Constant)), self._map2ArithOp)
        secondMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(self._map2firstFieldName),
                                                          expression2), schema)
        mapOperators = [firstMap, secondMap]
        return shuffle_list(mapOperators)

    def __initializeRandomlyOrderedMaps(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        self._map1firstFieldName = random_field_name(numFields)
        numFields.remove(self._map1firstFieldName)
        self._map1Constant = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        self._map1ArithOp = arithOperation

        self._map2firstFieldName = random_field_name(numFields)
        self._map2Constant = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        self._map2ArithOp = arithOperation

        self._schema = schema


    def validation(self, schema: Schema) -> bool:
        if self._map1firstFieldName not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._map1firstFieldName]:
                self._map1firstFieldName = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._map2firstFieldName]:
                self._map2firstFieldName = key
                break

        self._schema = schema

