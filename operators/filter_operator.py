from utils.contracts import Operator, LogicalExpression, Schema


class FilterOperator(Operator):
    def __init__(self, predicate: LogicalExpression, schema: Schema):
        super().__init__(schema)
        self._predicate = predicate

    def generate_code(self) -> str:
        return f"filter({self._predicate.generate_code()})"
