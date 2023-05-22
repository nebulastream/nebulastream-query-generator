from copy import deepcopy

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.window_operator import WindowOperator
from operators.aggregation_operator import AggregationOperator
from utils.contracts import *
from utils.utils import *


class AggregationEquivalentAggregationGeneratorStrategy(BaseGeneratorStrategy):

    def __init__(self):
        self._field = None
        self._windowKey = None
        self._aggregationOperation = None
        self._intervalInMin = None
        self._timestampField = None
        self._schema = None

    def generate(self, schema: Schema) -> [AggregationOperator]:
        """
        Queries with similar Window Aggregation Operators:
        Examples:
        1. Window(Tumbling::of(ts), seconds(10)).apply(Sum("a")) vs Window(Sliding::of(ts), seconds(10), seconds(10)).apply(Sum("a"))
        2. Window(Tumbling::of(ts), minutes(1)).apply(Sum("a")) vs Window(Tumbling::of(ts), seconds(60)).apply(Sum("a"))
        3. Window(Tumbling::of(ts), seconds(10)).apply(Sum("a")) vs Window(Sliding::of(ts), seconds(10), seconds(10)).apply(Sum("a")->as("xyz"))
        :param schema:
        :return:
        """
        if not self._field:
            self.__initializeEquivalentFilters(schema)

        if not self.validation(schema):
            self.update_columns(schema)

        _, windowType = random_list_element([WindowType.tumbling, WindowType.sliding])
        _, timeUnit = random_list_element([TimeUnit.minutes, TimeUnit.seconds])

        windowLength = self._intervalInMin
        if timeUnit == TimeUnit.seconds:
            windowLength = self._intervalInMin * 60

        if windowType == WindowType.sliding:
            windowType = f"{windowType.value}::of(EventTime(Attribute(\"{self._timestampField}\")), {timeUnit.value}({windowLength}), {timeUnit.value}({windowLength}))"
        else:
            windowType = f"{windowType.value}::of(EventTime(Attribute(\"{self._timestampField}\")), {timeUnit.value}({windowLength}))"

        schema = Schema(name=schema.name, int_fields=[self._windowKey], double_fields=[], string_fields=[],
                        timestamp_fields=[self._timestampField], fieldNameMapping=schema.get_field_name_mapping())
        window = WindowOperator(windowType=windowType, windowKey=self._windowKey, schema=schema)

        aggregation = f"{self._aggregationOperation.value}(Attribute(\"{self._field}\"))"

        outputField = self._field
        alias = ""
        # Uncomment if you want to generate as field
        # if bool(random.getrandbits(1)):
        # alias = random_name()
        # outputField = alias

        schema = Schema(name=schema.name, int_fields=[outputField], double_fields=[], string_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields,
                        fieldNameMapping={outputField: self._field})
        aggregationOperator = AggregationOperator(aggregations=[aggregation], alias=alias, window=window, schema=schema)
        return [aggregationOperator]

    def __initializeEquivalentFilters(self, schema: Schema):
        schemaCopy = deepcopy(schema)
        fields = schemaCopy.get_numerical_fields()
        self._field = random_field_name(fields)
        fields.remove(self._field)
        self._windowKey = ""
        if bool(random.getrandbits(1)):
            self._windowKey = random_field_name(fields)
        _, aggregationOperation = random_list_element(
            [Aggregations.avg, Aggregations.min, Aggregations.max, Aggregations.sum])
        self._aggregationOperation = aggregationOperation
        self._intervalInMin = random_int_between(1, 4)
        self._timestampField = random_field_name(schema.get_timestamp_fields())
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._field not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._schema.get_field_name_mapping()[self._field]:
                self._field = key
                break

        if self._windowKey:
            for key, value in schema.get_field_name_mapping().items():
                if value == self._schema.get_field_name_mapping()[self._windowKey]:
                    self._windowKey = key
                    break

        self._schema = schema
