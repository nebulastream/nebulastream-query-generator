from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_window_strategy import \
    DistinctWindowGeneratorStrategy
from operators.window_operator import WindowOperator
from operators.aggregation_operator import AggregationOperator
from utils.contracts import *
from utils.utils import *


class AggregationEquivalentAggregationGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._equivalentFilterOperators = None
        self._field = None
        self._windowKey = None
        self._aggregationOperation = None
        self._intervalInMin = None
        self._timestampField = None

    def generate(self, schema: Schema) -> AggregationOperator:
        """
        Queries with similar Window Aggregation Operators:
        Examples:
        1. map(y = y*10).filter(y<31) vs map(y = y*10).filter(y<35)
        2. filter(a*2>b*3) vs filter(a*4>b*6)
        :param schema:
        :return:
        """
        if not self._equivalentFilterOperators:
            self.__initializeEquivalentFilters(schema)

        _, windowType = random_list_element([WindowType.tumbling, WindowType.sliding])
        _, timeUnit = random_list_element([TimeUnit.minutes, TimeUnit.seconds])

        windowLength = 0
        if timeUnit == TimeUnit.seconds:
            windowLength = self._intervalInMin * 60

        if windowType == WindowType.sliding:
            windowType = f"{windowType}::of(EventTime(Attribute({self._timestampField})), {timeUnit}({windowLength}), {timeUnit}({windowLength}))"
        else:
            windowType = f"{windowType}::of(EventTime(Attribute({self._timestampField})), {timeUnit}({windowLength}))"

        windowKey = ""
        if bool(random.getrandbits(1)):
            windowKey = random_field_name(schema.get_numerical_fields())

        schema = Schema(name=schema.name, int_fields=[windowKey], double_fields=[], string_fields=[],
                        timestamp_fields=[self._timestampField])
        window = WindowOperator(windowType=windowType, windowKey=windowKey, schema=schema)

        aggregation = ""
        if self._aggregationOperation == Aggregations.count:
            aggregation = f"{self._aggregationOperation}()"
        else:
            aggregation = f"{self._aggregationOperation}(Attribute({self._field}))"

        outputField = self._field
        alias = ""
        if bool(random.getrandbits(1)):
            alias = random_name()
            outputField = alias

        schema = Schema(name=schema.name, int_fields=[outputField], double_fields=[], string_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields)
        aggregationOperator = AggregationOperator(aggregation=aggregation, alias=alias, window=window, schema=schema)
        return aggregationOperator

    def __initializeEquivalentFilters(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        fields = schemaCopy.get_numerical_fields()
        _, field = random_list_element(fields)
        _, windowKey = random_list_element(fields)
        _, aggregationOperation = random_list_element(
            [Aggregations.avg, Aggregations.min, Aggregations.max, Aggregations.sum, Aggregations.count])

        self._field = field
        self._windowKey = windowKey
        self._aggregationOperation = aggregationOperation
        self._intervalInMin = random_int_between(1, 100)
        self._timestampField = random_field_name(schema.get_timestamp_fields())
