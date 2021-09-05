from typing import List

from utils.utils import shuffle_list
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.union_operator import UnionOperator
from query_generator.query import Query
from utils.contracts import Operator


class UnionEquivalentUnionGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()

    def generate(self, subQueries: List[Query]) -> List[Operator]:
        shuffledSubQueries = shuffle_list(subQueries)
        schema = shuffledSubQueries[0].output_schema()
        union = UnionOperator(schema=schema, leftSubQuery=shuffledSubQueries[0], rightSubQuery=shuffledSubQueries[1])
        return [union]