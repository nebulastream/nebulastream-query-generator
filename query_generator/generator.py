import random
from copy import deepcopy
from typing import List

from generator_config.config import GeneratorConfig
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from query_generator.query import Query
from operator_generator_strategies.distinct_operator_strategies.distinct_sink_strategy import DistinctSinkGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_source_strategy import SourceOperator, DistinctSourceGeneratorStrategy
from utils.utils import *


class QueryGenerator:
    def __init__(self, config: GeneratorConfig):
        self._config = config
        self._equivalentOperatorGenerators: List[BaseGeneratorStrategy] = config.equivalentOperatorGenerators
        self._distinctOperatorGenerators: List[BaseGeneratorStrategy] = config.distinctOperatorGenerators
        self._queries: List[Query] = []

    def generate(self) -> List[Query]:
        # self.__inject_source_operators()
        sourceGenerator = DistinctSourceGeneratorStrategy()
        sourceOperator = sourceGenerator.generate(self._config.possibleSources[0])

        equivalentOperatorGenerators = self._equivalentOperatorGenerators
        distinctOperatorGenerators = self._distinctOperatorGenerators
        while len(self._queries) < self._config.numberOfQueries:
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
            sinkOperator = DistinctSinkGeneratorStrategy().generate(outputSchema)
            new_query.add_operator(sinkOperator)
            self._queries.append(new_query)

        return self._queries
