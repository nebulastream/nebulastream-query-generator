from typing import List
import random

from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.window_operator import WindowOperator
from utils.contracts import Schema, Operator, WindowType, TimeUnit
from utils.utils import random_field_name, random_list_element, random_int_between


class DistinctWindowGeneratorStrategy(BaseGeneratorStrategy):
    def __init__(self):
        super().__init__()

    def generate(self, schema: Schema, generateKey: bool = True) -> List[Operator]:
        timestampField = random_field_name(schema.get_timestamp_fields())
        _, windowType = random_list_element([WindowType.tumbling, WindowType.sliding])
        _, timeUnit = random_list_element([TimeUnit.minutes, TimeUnit.seconds])

        windowLength = random_int_between(1, 100)
        if windowType == WindowType.sliding:
            windowSlide = random_int_between(1, 100)
            windowType = f"{windowType.value}::of(EventTime(Attribute(\"{timestampField}\")), {timeUnit.value}({windowLength}), {timeUnit.value}({windowSlide}))"
        else:
            windowType = f"{windowType.value}::of(EventTime(Attribute(\"{timestampField}\")), {timeUnit.value}({windowLength}))"

        windowKey = ""
        if generateKey and bool(random.getrandbits(1)):
            windowKey = random_field_name(schema.get_numerical_fields())

        schema = Schema(name=schema.name, int_fields=[windowKey], double_fields=[], string_fields=[],
                        timestamp_fields=["start", "end"],
                        fieldNameMapping=schema.get_field_name_mapping())
        window = WindowOperator(windowType=windowType, windowKey=windowKey, schema=schema)
        return [window]
