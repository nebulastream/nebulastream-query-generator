from typing import List

from utils.contracts import Operator, Schema


class ProjectOperator(Operator):
    def __init__(self, fieldsToProject: List[str], newFieldNames: List[str], schema: Schema):
        super().__init__(schema)
        self._fieldsToProject = fieldsToProject
        self._newFieldNames = newFieldNames

    def generate_code(self) -> str:
        code = f"project("
        if self._newFieldNames:
            for i in range(len(self._fieldsToProject)):
                code = code + f"Attribute(\"{self._fieldsToProject[i]}\").rename(\"{self._newFieldNames[i]}\"),"
        else:
            for i in range(len(self._fieldsToProject)):
                code = code + f"Attribute(\"{self._fieldsToProject[i]}\"),"

        tsFields = self.get_output_schema().get_timestamp_fields()
        for i in range(len(tsFields)):
            code = code + f"Attribute(\"{tsFields[i]}\"),"

        return code[:-1] + ")"
