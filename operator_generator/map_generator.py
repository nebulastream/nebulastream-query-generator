from copy import deepcopy

from operators.map_operator import MapOperator
from utils.contracts import *
from utils.utils import random_list_element


class MapGenerator(OperatorFactory):

    def generate(self, input_schema: Schema) -> Operator:
        output_schema = deepcopy(input_schema)
        print(input_schema.get_numerical_fields())
        _, left_operand = random_list_element(output_schema.get_numerical_fields())
        _, right_operand = random_list_element(output_schema.int_fields)
        _, operator = random_list_element(list(ArithmeticOperators))
        result_variable = f"{left_operand}_{operator}_{right_operand}"
        output_schema.int_fields.append(result_variable)
        return MapOperator(FieldAssignmentExpression(FieldAccessExpression(result_variable),
                                                     ArithmeticExpression(FieldAccessExpression(left_operand),
                                                                          FieldAccessExpression(right_operand),
                                                                          operator)),
                           output_schema)
