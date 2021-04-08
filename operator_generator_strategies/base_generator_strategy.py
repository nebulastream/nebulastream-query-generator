from abc import abstractmethod, ABC
from typing import List

from utils.contracts import Schema, Operator


class BaseGeneratorStrategy(ABC):

    @abstractmethod
    def generate(self, schema: Schema) -> List[Operator]:
        """
        Generate a set of connected operators that works by consuming the schema
        :return: list of connected operators
        """
        pass
