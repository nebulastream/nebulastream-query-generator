import random
from copy import deepcopy
from typing import List

from generator_config.config import GeneratorConfig
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operators.union_operator import UnionOperator
from query_generator.query import Query
from operator_generator_strategies.distinct_operator_strategies.distinct_sink_strategy import \
    DistinctSinkGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_source_strategy import SourceOperator, \
    DistinctSourceGeneratorStrategy
from utils.contracts import Schema
from utils.utils import *


class QueryGenerator:
    def __init__(self, sourceToUse: Schema, numberOfQueriesToGenerate: int,
                 equivalentOperatorGenerators: List[BaseGeneratorStrategy],
                 distinctOperatorGenerators: List[BaseGeneratorStrategy]):
        self._schema = sourceToUse
        self._numberOfQueriesToGenerate = numberOfQueriesToGenerate
        self._equivalentOperatorGenerators: List[BaseGeneratorStrategy] = equivalentOperatorGenerators
        self._distinctOperatorGenerators: List[BaseGeneratorStrategy] = distinctOperatorGenerators
        self._queries: List[Query] = []

    def generate(self) -> List[Query]:
        # self.__inject_source_operators()
        sourceGenerator = DistinctSourceGeneratorStrategy()
        sourceOperator = sourceGenerator.generate(self._schema)

        equivalentOperatorGenerators = self._equivalentOperatorGenerators
        distinctOperatorGenerators = self._distinctOperatorGenerators
        downStreamOp
        while len(self._queries) < self._numberOfQueriesToGenerate:
            new_query = Query().add_operator(sourceOperator)

            # Generate equivalent operators
            for generatorRule in equivalentOperatorGenerators:
                operators = generatorRule.generate(new_query.output_schema())
                for operator in operators:
                    new_query.add_operator(operator)

            # Generate random operators
            shuffle_list(distinctOperatorGenerators)
            for generatorRule in distinctOperatorGenerators:
                operators = generatorRule.generate(new_query.output_schema())
                for operator in operators:
                    new_query.add_operator(operator)

            # Add sink operator to the query
            outputSchema = new_query.output_schema()
            unionOperator = UnionOperator(outputSchema, new_query, new_query)
            unionedQuery = Query().add_operator(unionOperator)
            sinkOperator = DistinctSinkGeneratorStrategy().generate(outputSchema)
            unionedQuery.add_operator(sinkOperator)
            self._queries.append(unionedQuery)

        return self._queries
