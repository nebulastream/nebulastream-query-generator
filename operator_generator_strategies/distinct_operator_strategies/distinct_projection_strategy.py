from typing import List
import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.project_operator import ProjectOperator
from utils.contracts import Schema, Operator
from utils.utils import random_int_between, random_name


class DistinctProjectionGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, max_number_of_predicates: int = 1):
        super().__init__()
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, schema: Schema) -> List[Operator]:
        numericalFields = schema.get_numerical_fields()
        noOfFieldsToProject = 1
        if len(numericalFields) > 1:
            noOfFieldsToProject = random_int_between(2, len(numericalFields))
        fields = []
        for i in range(noOfFieldsToProject):
            fields.append(numericalFields[i])

        newFiledNames = []

        if bool(random.getrandbits(1)):
            fieldMapping = {}
            for i in range(len(fields)):
                newFieldName = random_name()
                fieldMapping[newFieldName] = fields[i]
                newFiledNames.append(newFieldName)

            schema = Schema(name=schema.name, int_fields=newFiledNames, double_fields=schema.double_fields,
                            string_fields=schema.string_fields,
                            timestamp_fields=schema.timestamp_fields, fieldNameMapping=fieldMapping)
        else:
            schema = Schema(name=schema.name, int_fields=fields, double_fields=schema.double_fields,
                            string_fields=schema.string_fields,
                            timestamp_fields=schema.timestamp_fields, fieldNameMapping=schema.get_field_name_mapping())
        project = ProjectOperator(fieldsToProject=fields, newFieldNames=newFiledNames, schema=schema)
        return [project]
