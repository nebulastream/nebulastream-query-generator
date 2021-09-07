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
        fields = schema.get_numerical_fields()
        _, field = random_list_element(fields)
        _, aggregationOperation = random_list_element(
            [Aggregations.avg, Aggregations.min, Aggregations.max, Aggregations.sum, Aggregations.count])

        aggregation = ""
        if aggregationOperation == Aggregations.count:
            aggregation = f"{aggregationOperation.value}()"
        else:
            aggregation = f"{aggregationOperation.value}(Attribute(\"{field}\"))"

        outputField = field
        alias = ""
        if bool(random.getrandbits(1)):
            alias = random_name()
            outputField = alias

        window = DistinctWindowGeneratorStrategy().generate(schema)[0]
        schema = Schema(name=schema.name, int_fields=[outputField], double_fields=[], string_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields)
        aggregationOperator = AggregationOperator(aggregation=aggregation, alias=alias, window=window, schema=schema)
        return [aggregationOperator]
