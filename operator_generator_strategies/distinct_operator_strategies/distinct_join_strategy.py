from copy import deepcopy
import random
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_window_strategy import \
    DistinctWindowGeneratorStrategy
from operators.join_operator import JoinOperator
from operators.source_operator import SourceOperator
from query_generator.query import Query
from utils.contracts import Schema, Operator
from utils.utils import random_list_element, shuffle_list


class DistinctJoinGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, schemas: List[Schema]):
        super().__init__()
        self._selectedSchemas = schemas

    def generate(self, subQuery: Query) -> List[Operator]:
        shuffledSelectedSchemas = random.sample(self._selectedSchemas, k=1)
        # shuffledSelectedSchemas = shuffle_list(self._selectedSchemas)

        join = None
        for i in range(len(shuffledSelectedSchemas)):
            rightSchema = shuffledSelectedSchemas[i]
            leftSchema = subQuery.output_schema()
            _, rightCol = random_list_element(rightSchema.get_numerical_fields())
            _, leftCol = random_list_element(leftSchema.get_numerical_fields())

            fieldMapping = leftSchema.get_field_name_mapping().copy()
            fieldMapping.update(rightSchema.get_field_name_mapping())

            intFields = leftSchema.get_numerical_fields()
            intFields.extend(rightSchema.get_numerical_fields())

            window = DistinctWindowGeneratorStrategy().generate(leftSchema, False)[0]

            timeStampFields = []
            for timeStampField in window.get_output_schema().timestamp_fields:
                if timeStampField != "start" and timeStampField != "end":
                    timeStampFields.append(timeStampField)

            schema = Schema(name=leftSchema.name + "$" + rightSchema.name,
                            int_fields=intFields,
                            double_fields=[],
                            timestamp_fields=timeStampFields, string_fields=[],
                            fieldNameMapping=fieldMapping)

            rightSubQuery = Query().add_operator(SourceOperator(rightSchema))

            join = JoinOperator(schema=schema, leftSubQuery=subQuery,
                                rightSubQuery=rightSubQuery,
                                leftCol=leftCol, rightCol=rightCol, window=window)
            subQuery = Query().add_operator(join)

        return [join]
