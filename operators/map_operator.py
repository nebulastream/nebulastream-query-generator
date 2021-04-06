from utils.contracts import Operator, FieldAssignmentExpression, Schema


class MapOperator(Operator):
    def __init__(self, expression: FieldAssignmentExpression, schema: Schema):
        super(MapOperator, self).__init__(schema)
        self._expression = expression

    def generate_code(self) -> str:
        return f"map({self._expression.generate_code()})"
