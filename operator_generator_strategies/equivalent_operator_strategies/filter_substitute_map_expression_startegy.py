from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.map_operator import MapOperator
from utils.contracts import Operator, Schema, FieldAssignmentExpression, FieldAccessExpression, ConstantExpression, \
    ArithmeticOperators, ArithmeticExpression, LogicalExpression, LogicalOperators
from utils.utils import random_list_element, random_int_between, random_field_name


class FilterSubstituteMapExpressionGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):

        self._mapFieldName = None
        self._contValue = None
        self._mapAssignmentFieldName = None
        self._arithOp = None
        self._logicalOp = None
        self._schema = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
        Example:  map(y = 30).filter(x > 30) vs map(y = 30).filter(x > y)
        :param schema: schema to be used for generating the operators
        :return: the list of operators
        """
        if len(schema.get_numerical_fields()) == 1:
            print("Skipping FilterSubstituteMapExpressionGeneratorStrategy as only 1 field is present in the schema")
            return []

        if not self._mapFieldName:
            self.__initializeFiltersWithSubstitutedMapExpression(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        baseMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(self._mapFieldName),
                                                        ConstantExpression(str(self._contValue))), schema)
        arithExpression1 = ArithmeticExpression(FieldAccessExpression(self._mapAssignmentFieldName),
                                                FieldAccessExpression(self._mapFieldName), self._arithOp)
        arithExpression2 = ArithmeticExpression(FieldAccessExpression(self._mapAssignmentFieldName),
                                                ConstantExpression(str(self._contValue)), self._arithOp)

        followUpFilter1 = FilterOperator(
            LogicalExpression(FieldAccessExpression(self._mapAssignmentFieldName), arithExpression1, self._logicalOp),
            schema)
        followUpFilter2 = FilterOperator(
            LogicalExpression(FieldAccessExpression(self._mapAssignmentFieldName), arithExpression2, self._logicalOp),
            schema)

        _, followUpFilter = random_list_element([followUpFilter1, followUpFilter2])
        return [baseMap, followUpFilter]

    def __initializeFiltersWithSubstitutedMapExpression(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        self._mapFieldName = random_field_name(numFields)
        numFields.remove(self._mapFieldName)
        self._contValue = random_int_between(1, 10)
        self._mapAssignmentFieldName = random_field_name(numFields)
        _, self._arithOp = random_list_element(list(ArithmeticOperators))
        _, self._logicalOp = random_list_element(
            [LogicalOperators.lt, LogicalOperators.gt, LogicalOperators.lte, LogicalOperators.gte])
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._mapFieldName not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._mapFieldName]:
                self._mapFieldName = key
                break

        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._mapAssignmentFieldName]:
                self._mapAssignmentFieldName = key
                break

        self._schema = schema
