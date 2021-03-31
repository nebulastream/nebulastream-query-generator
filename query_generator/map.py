from copy import deepcopy
from dataclasses import dataclass

from query_generator.contracts import OperatorFactory, Schema, Operator
from query_generator.utils import random_list_element


@dataclass
class Formula:
    left_operand: str
    operator: str
    right_operand: str


@dataclass
class Expression:
    result_attribute: str
    formula: Formula


class MapOperator(Operator):
    def __init__(self, expression: Expression, schema: Schema):
        self._expression = expression
        self._output_schema = schema

    def output_schema(self) -> Schema:
        return self._output_schema

    def generate_code(self) -> str:
        formula = self._expression.formula
        expression_str = f'Attribute("{self._expression.result_attribute}") = Attribute("{formula.left_operand}") {formula.operator} Attribute("{formula.right_operand}")'
        return f"map({expression_str})"


class MapFactory(OperatorFactory):
    def generate(self, input_schema: Schema) -> Operator:
        output_schema = deepcopy(input_schema)
        _, left_operand = random_list_element(input_schema.int_fields)
        _, right_operand = random_list_element(input_schema.int_fields)
        _, operator = random_list_element(["+", "-", "*"])
        result_variable = f"{left_operand}_{operator}_{right_operand}"
        output_schema.int_fields.append(result_variable)
        return MapOperator(Expression(result_variable, Formula(left_operand, operator, right_operand)), output_schema)
