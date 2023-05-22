from typing import List
import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.project_operator import ProjectOperator
from utils.contracts import Schema, Operator
from utils.utils import random_int_between, random_name


def create_projection(schema):
    numericalFields = schema.get_numerical_fields()
    noOfFieldsToProject = 1
    if len(numericalFields) > 1:
        noOfFieldsToProject = random_int_between(2, len(numericalFields))
    fields = random.sample(range(noOfFieldsToProject), numericalFields)
    newFiledNames = []
    schema = Schema(name=schema.name, int_fields=fields, double_fields=schema.double_fields,
                    string_fields=schema.string_fields,
                    timestamp_fields=schema.timestamp_fields, fieldNameMapping=schema.get_field_name_mapping())
    baseProjection = ProjectOperator(fieldsToProject=fields, newFieldNames=newFiledNames, schema=schema)
    return baseProjection


class ProjectionContainmentGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, max_number_of_predicates: int = 1):
        super().__init__()
        self._max_number_of_predicates = max_number_of_predicates

    def generate(self, schema: Schema) -> List[Operator]:
        """
        generate projection containment cases
        Example:
        base projection case: project(a, b, c, d)
        containment cases: project(a, b, c), project(a, b), project(c, d)
        :param schema:
        :return:
        """
        baseProjection = create_projection(schema)
        containmentCases = []
        for i in range(0,10):
            containmentCases.append(create_projection(baseProjection.output_schema))

        return [baseProjection, containmentCases]
