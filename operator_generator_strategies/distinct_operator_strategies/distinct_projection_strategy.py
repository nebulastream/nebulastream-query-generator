from typing import List
from random import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.project_operator import ProjectOperator
from utils.contracts import Schema, Operator
from utils.utils import random_int_between, random_name


class DistinctProjectionGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, max_number_of_predicates: int = 1):
        super().__init__()
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, schema: Schema) -> List[Operator]:
        noOfFieldsToProject = random_int_between(2, len(schema.get_numerical_fields()))
        fields = random.sample(schema.get_numerical_fields(), noOfFieldsToProject)
        newFiledNames = []

        if bool(random.getrandbits(1)):
            for i in range(len(fields)):
                newFiledNames.append(random_name())

            schema = Schema(name=schema.name, int_fields=newFiledNames, double_fields=schema.double_fields,
                            string_fields=schema.string_fields,
                            timestamp_fields=schema.timestamp_fields)
        else:
            schema = Schema(name=schema.name, int_fields=fields, double_fields=schema.double_fields,
                            string_fields=schema.string_fields,
                            timestamp_fields=schema.timestamp_fields)

        project = ProjectOperator(fieldsToProject=fields, newFieldNames=newFiledNames, Schema=schema)
        return [project]
