from copy import deepcopy

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.project_operator import ProjectOperator
from utils.contracts import *
from utils.utils import *


class ProjectEquivalentProjectGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        self._schema = None
        self._fieldsToProject: List[str] = []

    def generate(self, schema: Schema) -> List[Operator]:
        """
        Queries with similar project operators:
        Examples:
        1. project(a,b) vs project(a.as("new_a"), b.as("new_b"))
        2. project(a.as("new_a"), b.as("new_b")) vs project(a.as("a1"), b.as("b1"))
        :param schema:
        :return:
        """
        if not self._fieldsToProject:
            self.__initializeFieldsToProject(schema)

        fields = self._fieldsToProject
        newFiledNames = []

        # Uncomment to rename the fields
        # if bool(random.getrandbits(1)):
        #     for i in range(len(fields)):
        #         newFiledNames.append(random_name())
        #
        #     fieldMap = dict()
        #     for i in range(len(newFiledNames)):
        #         fieldMap[newFiledNames[i]] = fields[i]
        #     schema = Schema(name=schema.name, int_fields=newFiledNames, double_fields=schema.double_fields,
        #                     string_fields=schema.string_fields,
        #                     timestamp_fields=schema.timestamp_fields, fieldNameMapping=fieldMap)
        # else:
        fieldMap = dict()
        for i in range(len(fields)):
            fieldMap[fields[i]] = fields[i]
        schema = Schema(name=schema.name, int_fields=fields, double_fields=schema.double_fields,
                        string_fields=schema.string_fields,
                        timestamp_fields=schema.timestamp_fields, fieldNameMapping=fieldMap)

        project = ProjectOperator(fieldsToProject=fields, newFieldNames=newFiledNames, schema=schema)
        return [project]

    def __initializeFieldsToProject(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        numericalFields = schemaCopy.get_numerical_fields()

        filteredFields = list(filter(lambda x: "NEW_" not in x, numericalFields))

        noOfFieldsToProject = random_int_between(2, len(filteredFields))
        fieldsToProject = []
        for i in range(noOfFieldsToProject):
            fieldsToProject.append(filteredFields[i])
        self._fieldsToProject = fieldsToProject
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._fieldsToProject[0] not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):

        updatedFieldsToProject = []
        for field in self._fieldsToProject:
            for key, value in schema.get_field_name_mapping().items():
                if value == self._schema.get_field_name_mapping()[field]:
                    updatedFieldsToProject.append(key)
                    break

        self._fieldsToProject = updatedFieldsToProject
        self._schema = schema
