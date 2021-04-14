from query_generator.query import Query
from utils.contracts import Operator, Schema


class UnionOperator(Operator):
    def __init__(self, schema: Schema, leftSubQuery: Query, rightSubQuery: Query):
        super().__init__(schema)
        self._leftSubQuery = leftSubQuery
        self._rightSubQuery = rightSubQuery

    def generate_code(self) -> str:
        return f"{self._leftSubQuery.generate_code()}.unionWith({self._rightSubQuery.generate_code()})"
