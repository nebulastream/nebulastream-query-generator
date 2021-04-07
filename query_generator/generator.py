import random
from copy import deepcopy
from typing import List

from generator_config.config import GeneratorConfig
from operator_generator.generator_rule import GeneratorRule
from query_generator.query import Query
from operator_generator.random_operators_rules.sink_generator import SinkGenerator
from operator_generator.random_operators_rules.source_generator import SourceOperator, SourceGenerator
from utils.utils import *


class QueryGenerator:
    def __init__(self, config: GeneratorConfig):
        self._config = config
        self._equivalentOperatorGenerators: List[GeneratorRule] = config.equivalentOperatorGenerators
        self._distinctOperatorGenerators: List[GeneratorRule] = config.distinctOperatorGenerators
        self._queries: List[Query] = []

    def generate(self) -> List[Query]:
        # self.__inject_source_operators()
        sourceGenerator = SourceGenerator()
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
            sinkOperator = SinkGenerator().generate(outputSchema)
            new_query.add_operator(sinkOperator)
            self._queries.append(new_query)

        return self._queries
