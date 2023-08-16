from copy import deepcopy
from typing import List
import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.aggregation_operator import AggregationOperator
from operators.window_operator import WindowOperator
from utils.contracts import Schema, Operator, WindowType, TimeUnit, Aggregations
from utils.utils import random_field_name, random_list_element, random_int_between


class WindowAggregationContainmentGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()
        self._baseWindowTimeLength = None
        self._baseWindowTimeSlide = None
        self._baseWindowTimeStamp = None
        self._baseWindowKey = None
        self._baseAggregationOperations: List[Aggregations] = []
        self._fields: List[str] = []
        self._base_containment = None
        self._base_schema = None

    def generate(self, schema: Schema, generateKey: bool = True) -> List[AggregationOperator]:
        """
        generate a containment cases for window aggregations
        Examples:
        Base Window Aggregation: Window(Tumbling::of(ts), seconds(10)).apply(Sum("a"), Min("a"), Max("b"))
        Containment Cases:
        1. Window(Sliding::of(ts), seconds(100), seconds(100)).apply(Sum("a"))
        2. Window(Tumbling::of(ts), minutes(1)).apply(Sum("a"), Min("a"))
        3. Window(Tumbling::of(ts), seconds(60)).apply(Min("a"), Max("b"))
        :param schema:
        :param generateKey:
        :return: [Base Window Aggregation, List with 10 containment cases]
        """
        if not self._baseWindowTimeLength:
            self.__initializeContainmentWindow(schema, generateKey)

        if not self.validation(schema):
            self.update_columns(schema)
        # create the base window aggregation
        if self._base_containment is None:
            self._base_containment = self.createWindowAggregation(schema, self._baseWindowTimeLength, self._baseWindowTimeSlide, self._baseAggregationOperations, self._fields)
        if self._base_schema is None:
            self._base_schema = schema

        containmentCases = []
        # create 10 random containment cases based off of the base containment case
        for i in range(0, 10):
            time_factor = random_int_between(1, 10)
            numberOfAggregates = random_int_between(1, len(self._fields))
            random_indices = random.sample(range(len(self._fields)), numberOfAggregates)
            aggregationOperations = [self._baseAggregationOperations[j] for j in random_indices]
            aggregationFields = [self._fields[j] for j in random_indices]
            for agg in aggregationFields:
                if agg not in schema.get_numerical_fields():
                    aggregationFields.remove(agg)
            if len(aggregationFields) == 0:
                return []
            containmentCases.append(self.createWindowAggregation(schema, self._baseWindowTimeLength * time_factor, self._baseWindowTimeSlide, aggregationOperations, aggregationFields))
        if schema == self._base_schema:
            containmentCases.append(self._base_containment)
        _, containmentCase = random_list_element(containmentCases)
        return [containmentCase]

    def createWindowAggregation(self, schema, currentWindowLength, currentWindowSlide, currentAggregationOperations, currentFields):
        """
        create a window aggregation operator
        :param schema: schema to use
        :param currentWindowLength:
        :param currentWindowSlide:
        :param currentAggregationOperations:
        :param currentFields:
        :return: window aggregation operation
        """
        _, timeUnit = random_list_element([TimeUnit.minutes, TimeUnit.seconds])
        windowLength = currentWindowLength
        windowSlide = currentWindowSlide
        if timeUnit == TimeUnit.seconds:
            windowLength = windowLength * 60
            windowSlide = windowSlide * 60

        _, baseWindowType = random_list_element([WindowType.tumbling, WindowType.sliding])
        if baseWindowType == WindowType.sliding:
            baseWindowType = f"{baseWindowType.value}::of(EventTime(Attribute(\"{self._baseWindowTimeStamp}\")), {timeUnit.value}({windowLength}), {timeUnit.value}({windowSlide}))"
        else:
            baseWindowType = f"{baseWindowType.value}::of(EventTime(Attribute(\"{self._baseWindowTimeStamp}\")), {timeUnit.value}({windowLength}))"
        schema = Schema(name=schema.name, int_fields=[self._baseWindowKey], double_fields=[], string_fields=[],
                        timestamp_fields=["start", "end"],
                        fieldNameMapping=schema.get_field_name_mapping())
        window = WindowOperator(windowType=baseWindowType, windowKey=self._baseWindowKey, schema=schema)
        outputFields = {}
        outputFieldList = []
        aggregations = []
        for agg, field in zip(currentAggregationOperations, currentFields):
            aggregations.append(f"{agg.value}(Attribute(\"{field}\"))")
            outputFields[field] = field
            outputFieldList.append(field)
        schema = Schema(name=schema.name, int_fields=outputFieldList, double_fields=[], string_fields=[],
                        timestamp_fields=window.get_output_schema().timestamp_fields,
                        fieldNameMapping=outputFields)
        aggregationOperator = AggregationOperator(aggregations=aggregations, alias="", window=window, schema=schema)
        return aggregationOperator

    def __initializeContainmentWindow(self, schema: Schema, generateKey: bool):
        schemaCopy = deepcopy(schema)
        intFields = schemaCopy.int_fields
        numericalFields = schemaCopy.get_numerical_fields()
        # obtain the number of fields to aggregate
        noOfFieldsToAggregate = 1
        if len(numericalFields) > 1:
            noOfFieldsToAggregate = random_int_between(1, len(numericalFields))
        # obtain the fields to aggregate and the aggregation operations
        for i in range(noOfFieldsToAggregate):
            fieldName = random_field_name(numericalFields)
            self._fields.append(fieldName)
            _, aggregationOperation = random_list_element(
                [Aggregations.min, Aggregations.max, Aggregations.sum])
            self._baseAggregationOperations.append(aggregationOperation)
        # obtain the timestamp field
        self._baseWindowTimeStamp = random_field_name(schemaCopy.get_timestamp_fields())
        self._baseWindowKey = ""
        if generateKey and bool(random.getrandbits(1)):
            self._baseWindowKey = random_field_name(intFields)
        # generate random window length and slide in multiples of 10
        self._baseWindowTimeLength = random_int_between(1, 10) * 10
        self._baseWindowTimeSlide = self._baseWindowTimeLength
        self._schema = schema

    def validation(self, schema: Schema) -> bool:
        if self._baseWindowTimeStamp not in schema.get_timestamp_fields():
            return False
        if self._baseWindowKey:
            if self._baseWindowKey not in self._schema.get_field_name_mapping().values():
                return False
        if self._fields:
            for field in self._fields:
                if field not in schema.get_numerical_fields():
                    return False
        return True

    def update_columns(self, schema: Schema):
        if self._baseWindowTimeStamp not in schema.get_timestamp_fields():
            self._baseWindowTimeStamp = random_field_name(schema.get_timestamp_fields())

        if self._baseWindowKey:
            if self._baseWindowKey not in self._schema.get_field_name_mapping().values():
                self._baseWindowKey = ""
        if self._fields:
            for field in self._fields:
                if field not in schema.get_numerical_fields():
                   self._fields.remove(field)
        self._schema = schema
