from copy import deepcopy

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.map_operator import MapOperator
from utils.contracts import *
from utils.utils import random_list_element, random_int_between


class DistinctMapGeneratorStrategy(BaseGeneratorStrategy):

    def generate(self, schema: Schema) -> List[Operator]:
        output_schema = deepcopy(schema)
        _, left_operand = random_list_element(output_schema.get_numerical_fields())
        _, right_operand = random_list_element(output_schema.int_fields)
        _, operator = random_list_element(list(ArithmeticOperators))
        constant = random_int_between(0, 10000)

        if constant % 2 == 0:
            result_variable = f"{left_operand}_{operator}_{right_operand}"
            output_schema.int_fields.append(result_variable)
            map_operator = MapOperator(FieldAssignmentExpression(FieldAccessExpression(result_variable),
                                                                 ArithmeticExpression(
                                                                     FieldAccessExpression(left_operand),
                                                                     FieldAccessExpression(right_operand),
                                                                     operator)), output_schema)
        else:
            map_operator = MapOperator(FieldAssignmentExpression(FieldAccessExpression(left_operand),
                                                                 ArithmeticExpression(
                                                                     ConstantExpression(constant),
                                                                     FieldAccessExpression(right_operand),
                                                                     operator)), output_schema)
        return [map_operator]
