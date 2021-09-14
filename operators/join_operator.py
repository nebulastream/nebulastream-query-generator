from operators.window_operator import WindowOperator
from query_generator.query import Query
from utils.contracts import Operator, Schema


class JoinOperator(Operator):
    def __init__(self, schema: Schema, leftSubQuery: Query, rightSubQuery: Query, rightCol: str, leftCol: str,
                 window: WindowOperator):
        super().__init__(schema)
        self._leftSubQuery = leftSubQuery
        self._rightSubQuery = rightSubQuery
        self._rightCol = rightCol
        self._leftCol = leftCol
        self._window = window

    def generate_code(self) -> str:
        code = f"{self._leftSubQuery.generate_code()}.joinWith({self._rightSubQuery.generate_code()})"
        return f"{code}.where(Attribute(\"{self._leftCol}\")).equalsTo(Attribute(\"{self._rightCol}\")).{self._window.generate_code()}"
