from typing import List

from utils.utils import shuffle_list
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.union_operator import UnionOperator
from query_generator.query import Query
from utils.contracts import Operator


class DistinctUnionGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()

    def generate(self, subQueries: List[Query]) -> List[Operator]:
        shuffledSubQueries = shuffle_list(subQueries)
        schema = shuffledSubQueries[0].output_schema()
        union = UnionOperator(schema=schema, leftSubQuery=shuffledSubQueries[0], rightSubQuery=shuffledSubQueries[1])
        if len(subQueries) > 2:
            for i in range(2, len(subQueries)):
                subQuery = Query().add_operator(union)
                union = UnionOperator(schema=subQuery.output_schema(), leftSubQuery=subQuery,
                                      rightSubQuery=shuffledSubQueries[i])

        return [union]
