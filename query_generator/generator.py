from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.union_equivalent_strategy import \
    UnionEquivalentUnionGeneratorStrategy
from operators.map_operator import MapOperator
from operators.union_operator import UnionOperator
from query_generator.query import Query
from operator_generator_strategies.distinct_operator_strategies.distinct_sink_strategy import \
    DistinctSinkGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_source_strategy import \
    DistinctSourceGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy import \
    DistinctMapGeneratorStrategy
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

    def generate(self, binary: bool = True) -> List[Query]:
        # self.__inject_source_operators()
        sourceGenerator = DistinctSourceGeneratorStrategy()
        sourceOperator = sourceGenerator.generate(self._schema)

        equivalentOperatorGenerators = self._equivalentOperatorGenerators
        distinctOperatorGenerators = self._distinctOperatorGenerators

        while len(self._queries) < self._numberOfQueriesToGenerate:
            newQuery = Query().add_operator(sourceOperator)

            # Generate equivalent operators
            for generatorRule in equivalentOperatorGenerators:
                if isinstance(generatorRule, UnionEquivalentUnionGeneratorStrategy):
                    operators = generatorRule.generate(newQuery)
                    newQuery = Query().add_operator(operators[0])
                else:
                    operators = generatorRule.generate(newQuery.output_schema())
                    for operator in operators:
                        newQuery.add_operator(operator)

            # Generate random operators
            shuffle_list(distinctOperatorGenerators)
            for generatorRule in distinctOperatorGenerators:
                operators = generatorRule.generate(newQuery.output_schema())
                for operator in operators:
                    newQuery.add_operator(operator)

            # Add sink operator to the query
            outputSchema = newQuery.output_schema()
            if binary:
                unionOperator = UnionOperator(outputSchema, newQuery, newQuery)
                unionedQuery = Query().add_operator(unionOperator)
                unionedQuery.add_operator(downStreamOperator)
                sinkOperator = DistinctSinkGeneratorStrategy().generate(unionedQuery.output_schema())
                unionedQuery.add_operator(sinkOperator)
                self._queries.append(unionedQuery)
            else:
                sinkOperator = DistinctSinkGeneratorStrategy().generate(outputSchema)
                newQuery.add_operator(sinkOperator)
                self._queries.append(newQuery)

        return self._queries
