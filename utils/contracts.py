import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional


class ArithmeticOperators(enum.Enum):
    Add = "+"
    Mul = "*"
    Sub = "-"
    Div = "/"


class LogicalOperators(enum.Enum):
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    eq = "=="
    nt = "!"
    # And = "&&"
    # Or = "||"


@dataclass
class Schema:
    name: str
    int_fields: List[str]
    double_fields: List[str]
    string_fields: List[str]
    timestamp_fields: List[str]

    def get_numerical_fields(self) -> List[str]:
        return self.int_fields + self.double_fields


class Expression(ABC):
    @abstractmethod
    def generate_code(self) -> str:
        """
        Generate Code for Component
        :return: Query API representation of logic
        """
        pass


@dataclass
class ArithmeticExpression(Expression):
    left: Expression
    right: Expression
    arithOp: ArithmeticOperators

    def generate_code(self) -> str:
        return f'{self.left.generate_code()} {self.arithOp.value} {self.right.generate_code()}'


@dataclass
class LogicalExpression(Expression):
    left: Expression
    right: Expression
    logicOp: LogicalOperators

    def generate_code(self) -> str:
        return f'{self.left.generate_code()}{self.logicOp.value}{self.right.generate_code()}'


@dataclass
class ConstantExpression(Expression):
    value: str

    def generate_code(self) -> str:
        return self.value


@dataclass
class FieldAccessExpression(Expression):
    fieldName: str

    def generate_code(self) -> str:
        return f'Attribute("{self.fieldName}")'


@dataclass
class FieldAssignmentExpression(Expression):
    fieldAccessExpr: FieldAccessExpression
    expression: Expression

    def generate_code(self) -> str:
        return f'{self.fieldAccessExpr.generate_code()}={self.expression.generate_code()}'


class Operator(ABC):

    def __init__(self, schema: Schema):
        self.output_schema = schema

    def get_output_schema(self) -> Schema:
        """
        Get the output schema of this operator
        :return: Schema for output generated by this operator
        """
        return self.output_schema

    @abstractmethod
    def generate_code(self) -> str:
        """
        Generate Code for Component
        :return: Query API representation of logic
        """
        pass

