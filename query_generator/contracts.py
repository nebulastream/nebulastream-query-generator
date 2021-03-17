from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Schema:
    name: str
    int_fields: List[str]
    double_fields: List[str]
    string_fields: List[str]
    timestamp_fields: List[str]


class Operator(ABC):
    @abstractmethod
    def output_schema(self) -> Schema:
        """
        Get the output schema of this operator
        :return: Schema for output generated by this operator
        """
        pass

    @abstractmethod
    def generate_code(self) -> str:
        """
        Generate Code for Component
        :return: Query API representation of logic
        """
        pass


class OperatorFactory(ABC):
    @abstractmethod
    def generate(self, input_schema: Schema) -> Operator:
        """
        Generate operator that works by consuming the input schema
        :param input_schema: Fields accessible to operator
        :return:
        """
        pass