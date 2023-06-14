from typing import List
import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.project_operator import ProjectOperator
from utils.contracts import Schema, Operator
from utils.utils import random_list_element, random_int_between


def create_projection(schema):
    numericalFields = schema.get_numerical_fields()
    noOfFieldsToProject = 1
    if len(numericalFields) > 1:
        noOfFieldsToProject = random_int_between(2, len(numericalFields))
    fields = random.sample(numericalFields, noOfFieldsToProject)
    newFiledNames = []
    schema = Schema(name=schema.name, int_fields=fields, double_fields=schema.double_fields,
                    string_fields=schema.string_fields,
                    timestamp_fields=schema.timestamp_fields, fieldNameMapping=schema.get_field_name_mapping())
    projection = ProjectOperator(fieldsToProject=fields, newFieldNames=newFiledNames, schema=schema)
    return projection


class ProjectionContainmentGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()
        self._base_containment = None

    def generate(self, schema: Schema) -> List[Operator]:
        """
        generate projection containment cases
        Example:
        base projection case: project(a, b, c, d)
        containment cases: project(a, b, c), project(a, b), project(c, d)
        :param schema:
        :return:
        """
        if not self._base_containment:
            self._base_containment = create_projection(schema)
        containmentCases = []
        for i in range(0,10):
            if schema.__eq__(self._base_containment.output_schema):
                case = create_projection(self._base_containment.output_schema)
            else:
                case = create_projection(schema)
            containmentCases.append(case)
        if schema.__eq__(self._base_containment.output_schema):
            containmentCases.append(self._base_containment)
        _, containmentCase = random_list_element(containmentCases)
        return [containmentCase]
