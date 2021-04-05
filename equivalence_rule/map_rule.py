from typing import List

from query_generator.contracts import *
from query_generator.map import MapOperator
from query_generator.map import Expression
from query_generator.map import Formula
from query_generator.utils import random_list_element, random_int_between


class MapRulesInitializer:

    def __init__(self, schema: Schema):
        _, leftOperand = random_list_element(schema.int_fields)
        _, rightOperand = random_list_element(schema.int_fields)
        _, operator = random_list_element(["+", "-", "*"])
        formula = Formula(leftOperand, operator, rightOperand)
        self._mapRule = MapRules(schema, formula);
        pass


class MapRules:
    """This class is responsible for applying different rules that generate equivalent queries with map operator"""

    def __init__(self, input_schema: Schema, formula: Formula):
        self._input_schema = input_schema
        self._formula = formula
        self._complexArithmeticExpressions = []

    def new_field_with_same_expression(self) -> Operator:
        """
        This method is responsible for adding a map operator with a new field name with a fixed formula
        Example: map(a = b *2) vs map(x = b * 2)
        :return:
        """
        return MapOperator(FieldAssignmentExpression(FieldAccessExpression(""), ConstantExpression("")),
                           self._input_schema)

    def complex_arithmetic_expression(self) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
         Example:  map(y = 30).map(x = y* z) vs map(y = 30).map(x= 30*z)
        :return:
        """

        _, operand = random_list_element(self._input_schema.int_fields)
        random_int = random_int_between(10, 40)
        _, operator = random_list_element(["+", "-", "*"])
        assignerMap = MapOperator(
            FieldAssignmentExpression(FieldAccessExpression(operand), ConstantExpression(random_int)),
            self._input_schema)
        mapOptrs = [assignerMap]
        return mapOptrs

    def map_operator_reordering(self) -> List[Operator]:
        """This method is responsible for changing the order of two map operators

                Example:  map(y = 30).map(x = 30) vs map(x = 30).map(y= 30)
        """
        pass

    def map_expression_reordering(self) -> Operator:
        """This method is responsible for changing the order of the map expression

                Example:  map(y = x * z) vs map(y = z * x)
        """
        pass
