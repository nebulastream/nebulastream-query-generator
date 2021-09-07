from utils.contracts import Schema, Operator


class WindowOperator(Operator):
    def __init__(self, windowType: str, windowKey: str, schema: Schema):
        super().__init__(schema)
        self._windowType = windowType
        self._windowKey = windowKey

    def generate_code(self) -> str:
        code = f"window({self._windowType})"
        if self._windowKey:
            code = f"{code}.byKey(Attribute(\"{self._windowKey}\"))"
        return code
