import random
from typing import List
from copy import deepcopy

from operators.source_operator import SourceOperator
from utils.utils import shuffle_list
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.union_operator import UnionOperator
from query_generator.query import Query
from utils.contracts import Operator, Schema


class UnionEquivalentUnionGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, schemas: List[Schema]):
        super().__init__()
        self._schemas = schemas
        self._selectedSchemas = None

    def generate(self, subQuery: Query) -> List[Operator]:
        if not self._selectedSchemas:
            self.initializeStartegy()

        shuffledSelectedSchemas = shuffle_list(self._selectedSchemas)

        union = None
        for i in range(len(shuffledSelectedSchemas)):
            schema = shuffledSelectedSchemas[i]
            rightSubQuery = Query().add_operator(SourceOperator(schema))
            union = UnionOperator(schema=subQuery.output_schema(), leftSubQuery=deepcopy(subQuery),
                                  rightSubQuery=rightSubQuery)
            subQuery = Query().add_operator(union)

        return [union]

    def initializeStartegy(self):
        numberOfSchema = random.randint(1, 3)
        self._selectedSchemas = random.sample(self._schemas, numberOfSchema)
