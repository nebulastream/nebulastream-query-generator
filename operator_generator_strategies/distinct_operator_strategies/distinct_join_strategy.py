from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_window_strategy import \
    DistinctWindowGeneratorStrategy
from operators.join_operator import JoinOperator
from operators.window_operator import WindowOperator
from query_generator.query import Query
from utils.contracts import Schema, Operator, LogicalExpression, FieldAccessExpression, \
    ConstantExpression, LogicalOperators
from utils.utils import random_list_element, random_int_between, shuffle_list


class DistinctJoinGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()

    def generate(self, subQueries: List[Query]) -> List[Operator]:
        shuffledSubQueries = shuffle_list(subQueries)
        leftSchema = shuffledSubQueries[0].output_schema()
        rightSchema = shuffledSubQueries[1].output_schema()
        _, leftCol = random_list_element(leftSchema.get_numerical_fields())
        _, rightCol = random_list_element(rightSchema.get_numerical_fields())

        window = DistinctWindowGeneratorStrategy().generate(leftSchema)[0]
        schema = Schema(name=leftSchema.name + "$" + rightSchema.name,
                        int_fields=leftSchema.get_numerical_fields().append(rightSchema.get_numerical_fields()),
                        double_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields, string_fields=[])
        joinOperator = JoinOperator(schema=schema, leftSubQuery=shuffledSubQueries[0],
                                    rightSubQuery=shuffledSubQueries[1],
                                    leftCol=leftCol, rightCol=rightCol, window=window)
        return [joinOperator]
