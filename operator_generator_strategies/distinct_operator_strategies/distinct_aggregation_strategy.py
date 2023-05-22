import random
from typing import List

from operator_generator_strategies.distinct_operator_strategies.distinct_window_strategy import \
    DistinctWindowGeneratorStrategy
from utils.contracts import Aggregations
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.aggregation_operator import AggregationOperator
from utils.contracts import Schema, Operator
from utils.utils import random_list_element, random_name


class DistinctAggregationGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()

    def generate(self, schema: Schema) -> List[Operator]:

        window = DistinctWindowGeneratorStrategy().generate(schema)[0]

        fields = schema.get_numerical_fields()
        if window._windowKey:
            fields.remove(window._windowKey)
        _, field = random_list_element(fields)
        _, aggregationOperation = random_list_element(
            [Aggregations.avg, Aggregations.min, Aggregations.max, Aggregations.sum, Aggregations.median, Aggregations.count])

        outputField = field
        alias = ""
        aggregation = f"{aggregationOperation.value}(Attribute(\"{field}\"))"
        if bool(random.getrandbits(1)):
            alias = random_name()
            outputField = alias

        schema = Schema(name=schema.name, int_fields=[outputField], double_fields=[], string_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields,
                        fieldNameMapping=schema.get_field_name_mapping())
        aggregationOperator = AggregationOperator(aggregations=[aggregation], alias=alias, window=window, schema=schema)
        return [aggregationOperator]
