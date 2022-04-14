from copy import deepcopy
from typing import List

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.filter_operator import FilterOperator
from operators.join_operator import JoinOperator
from operators.map_operator import MapOperator
from operators.source_operator import SourceOperator
from operators.window_operator import WindowOperator
from query_generator.query import Query
from utils.contracts import *
from utils.utils import *


class JoinEquivalentJoinGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self, schemas: List[Schema]):
        super().__init__()
        self._schemas = schemas
        self._rightSchema = None
        self._rightSubQuery = None
        self._rightCol = None
        self._leftSchema = None
        self._leftCol = None
        self._joinKey = None
        self._intervalInMin = None
        self._timestampField = None

    def generate(self, leftSubQuery: Query) -> List[Operator]:
        """
        Queries with similar Joins:
        Examples:

            Query::from("y").joinWith(Query::from("x")).where("y-a").equalsTo("x-a")
                            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(10)))
                            .byKey(Attribute("user_id"))

                          Vs

            Query::from("y").joinWith(Query::from("x")).where("y-a").equalsTo("x-a")
                            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(600)))
                            .byKey(Attribute("user_id"))
        :param schema:
        :return:
        """

        schema = leftSubQuery.output_schema()
        if not self._leftCol:
            self.__initializeEquivalentJoins(schema)

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

        schema = Schema(name=schema.name, int_fields=[self._joinKey], double_fields=[], string_fields=[],
                        timestamp_fields=[self._timestampField], fieldNameMapping=schema.get_field_name_mapping())
        window = WindowOperator(windowType=windowType, windowKey=self._joinKey, schema=schema)

        outputIntFields = self._leftSchema.get_numerical_fields() + self._rightSchema.get_numerical_fields()

        timeStampFields =[]
        for timeStampField in window.get_output_schema().timestamp_fields:
            if timeStampField != "start" and timeStampField != "end":
                timeStampFields.append(timeStampField)

        schema = Schema(name=self._leftSchema.name + "$" + self._rightSchema.name, int_fields=outputIntFields,
                        double_fields=[], string_fields=[],
                        timestamp_fields=timeStampFields,
                        fieldNameMapping={})

        joinOperator = None
        if random.getrandbits(1):
            joinOperator = JoinOperator(schema=schema, leftSubQuery=leftSubQuery, rightSubQuery=self._rightSubQuery,
                                        window=window,
                                        leftCol=self._leftCol, rightCol=self._rightCol)
        else:
            joinOperator = JoinOperator(schema=schema, leftSubQuery=self._rightSubQuery, rightSubQuery=leftSubQuery,
                                        window=window,
                                        leftCol=self._rightCol, rightCol=self._leftCol)
        return [joinOperator]

    def __initializeEquivalentJoins(self, schema: Schema):

        rightSchemas = []
        if len(self._schemas) > 1:
            for i in range(len(self._schemas)):
                if self._schemas[i].get_numerical_fields() != schema.get_numerical_fields():
                    rightSchemas.append(self._schemas[i])
        else:
            rightSchemas = self._schemas

        self._rightSchema = random.sample(rightSchemas, 1)[0]
        self._rightSubQuery = Query().add_operator(SourceOperator(self._rightSchema))
        numerical_fields = deepcopy(schema).get_numerical_fields()
        self._leftCol = random_field_name(numerical_fields)
        numerical_fields.remove(self._leftCol)
        self._rightCol = random_field_name(self._rightSchema.get_numerical_fields())

        self._joinKey = ""
        # if bool(random.getrandbits(1)):
        #     self._joinKey = random_field_name(numerical_fields)

        self._intervalInMin = random_int_between(1, 4)
        self._timestampField = random_field_name(schema.get_timestamp_fields())
        self._leftSchema = schema

    def validation(self, schema: Schema) -> bool:
        if self._leftCol not in schema.get_numerical_fields():
            return False
        return True

    def update_columns(self, schema: Schema):
        for key, value in schema.get_field_name_mapping().items():
            if value == self._leftSchema.get_field_name_mapping()[self._leftCol]:
                self._leftCol = key
                break

        if self._joinKey:
            for key, value in schema.get_field_name_mapping().items():
                if value == self._leftSchema.get_field_name_mapping()[self._joinKey]:
                    self._joinKey = key
                    break

        self._leftSchema = schema
