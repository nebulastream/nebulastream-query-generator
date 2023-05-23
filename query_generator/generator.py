from copy import deepcopy

from operator_generator_strategies.containment_operator_strategies.filter_containment_strategy import \
    FilterContainmentGeneratorStrategy
from operator_generator_strategies.containment_operator_strategies.window_aggregation_containment_strategy import \
    WindowAggregationContainmentGeneratorStrategy
from operator_generator_strategies.containment_operator_strategies.projection_containment_strategy import \
    ProjectionContainmentGeneratorStrategy
from operator_generator_strategies.base_generator_strategy import BaseGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_aggregation_strategy import \
    DistinctAggregationGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_join_strategy import \
    DistinctJoinGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_projection_strategy import \
    DistinctProjectionGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_union_strategy import \
    DistinctUnionGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.aggregation_equivalent_aggregation_strategy import \
    AggregationEquivalentAggregationGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.join_equivalent_strategy import \
    JoinEquivalentJoinGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.project_equivalent_strategy import \
    ProjectEquivalentProjectGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.union_equivalent_strategy import \
    UnionEquivalentUnionGeneratorStrategy
from query_generator.query import Query
from operator_generator_strategies.distinct_operator_strategies.distinct_sink_strategy import \
    DistinctSinkGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_source_strategy import \
    DistinctSourceGeneratorStrategy
from utils.contracts import Schema
from utils.utils import *


class QueryGenerator:
    def __init__(self, sourceToUse: Schema, possibleSources: List[Schema], numberOfQueriesToGenerate: int,
                 equivalentOperatorGenerators: List[BaseGeneratorStrategy],
                 distinctOperatorGenerators: List[BaseGeneratorStrategy],
                 containmentOperatorGenerators: List[BaseGeneratorStrategy]):
        self._schema = sourceToUse
        self._possibleSources = possibleSources
        self._numberOfQueriesToGenerate = numberOfQueriesToGenerate
        self._equivalentOperatorGenerators: List[BaseGeneratorStrategy] = equivalentOperatorGenerators
        self._distinctOperatorGenerators: List[BaseGeneratorStrategy] = distinctOperatorGenerators
        self._containmentOperatorGenerators: List[BaseGeneratorStrategy] = containmentOperatorGenerators

    def generate(self) -> List[Query]:
        # self.__inject_source_operators()
        sourceGenerator = DistinctSourceGeneratorStrategy()
        sourceOperator = sourceGenerator.generate(self._schema)

        # reorder operator Generators strategies to generate valid queries
        equivalentOperatorGenerators = self.reorder_generator_strategies(self._equivalentOperatorGenerators)

        queries: List[Query] = []
        while len(queries) < self._numberOfQueriesToGenerate:
            newQuery = Query().add_operator(sourceOperator)

            # Generate equivalent operators
            for generatorRule in equivalentOperatorGenerators:
                if isinstance(generatorRule, UnionEquivalentUnionGeneratorStrategy) or \
                        isinstance(generatorRule, JoinEquivalentJoinGeneratorStrategy):
                    operators = generatorRule.generate(newQuery)
                    newQuery = Query().add_operator(operators[0])
                else:
                    operators = generatorRule.generate(newQuery.output_schema())
                    for operator in operators:
                        newQuery.add_operator(operator)
            # Generate containment operators
            if len(self._containmentOperatorGenerators) == 1:
                if isinstance(self._containmentOperatorGenerators[0], WindowAggregationContainmentGeneratorStrategy) and not \
                        self._containmentOperatorGenerators[0]._base_containment:
                    self._containmentOperatorGenerators[0].generate(newQuery.output_schema())
                    newQuery.add_operator(self._containmentOperatorGenerators[0]._base_containment)
                elif isinstance(self._containmentOperatorGenerators[0], ProjectionContainmentGeneratorStrategy) and not \
                        self._containmentOperatorGenerators[0]._base_containment:
                    self._containmentOperatorGenerators[0].generate(newQuery.output_schema())
                    newQuery.add_operator(self._containmentOperatorGenerators[0]._base_containment)
                else:
                    operators = self._containmentOperatorGenerators[0].generate(newQuery.output_schema())
                    for operator in operators:
                        newQuery.add_operator(operator)

            distinctOperatorGenerators = self.shuffle_generator_strategies(deepcopy(self._distinctOperatorGenerators))
            distinctOperatorGenerators = self.reorder_generator_strategies(distinctOperatorGenerators)
            # Generate random operators
            for generatorRule in distinctOperatorGenerators:
                if isinstance(generatorRule, DistinctUnionGeneratorStrategy) or \
                        isinstance(generatorRule, DistinctJoinGeneratorStrategy):
                    operators = generatorRule.generate(newQuery)
                    newQuery = Query().add_operator(operators[0])
                else:
                    operators = generatorRule.generate(newQuery.output_schema())
                    for operator in operators:
                        newQuery.add_operator(operator)

            # Add sink operator to the query
            outputSchema = newQuery.output_schema()
            sinkOperator = DistinctSinkGeneratorStrategy().generate(outputSchema)
            newQuery.add_operator(sinkOperator)
            queries.append(newQuery)

        return queries

    def reorder_generator_strategies(self, generatorStrategies: List[BaseGeneratorStrategy]) -> \
            List[BaseGeneratorStrategy]:

        for i in range(len(generatorStrategies)):
            if isinstance(generatorStrategies[i], DistinctAggregationGeneratorStrategy) or isinstance(
                    generatorStrategies[i], AggregationEquivalentAggregationGeneratorStrategy):
                if i < len(generatorStrategies) - 1:
                    generatorStrategies[len(generatorStrategies) - 1], generatorStrategies[i] = \
                        generatorStrategies[i], generatorStrategies[len(generatorStrategies) - 1]
                break

        maxBinaryOpLoc = -1
        for i in range(len(generatorStrategies)):
            if isinstance(generatorStrategies[i], ProjectEquivalentProjectGeneratorStrategy) or \
                    isinstance(generatorStrategies[i], DistinctProjectionGeneratorStrategy):
                maxBinaryOpLoc = i
            elif isinstance(generatorStrategies[i], UnionEquivalentUnionGeneratorStrategy) or \
                    isinstance(generatorStrategies[i], DistinctUnionGeneratorStrategy):
                if i > maxBinaryOpLoc != -1:
                    generatorStrategies[maxBinaryOpLoc], generatorStrategies[i] = \
                        generatorStrategies[i], generatorStrategies[maxBinaryOpLoc]
                break

        return generatorStrategies

    def shuffle_generator_strategies(self, generatorStrategies: List[BaseGeneratorStrategy]) -> List[
        BaseGeneratorStrategy]:

        random.shuffle(generatorStrategies)

        if not generatorStrategies:
            return []

        unionStrategy = None
        joinStrategy = None
        aggregationStrategy = None

        for i in range(len(generatorStrategies)):
            if isinstance(generatorStrategies[i], DistinctUnionGeneratorStrategy):
                unionStrategy = generatorStrategies[i]
            elif isinstance(generatorStrategies[i], DistinctJoinGeneratorStrategy):
                joinStrategy = generatorStrategies[i]
            elif isinstance(generatorStrategies[i], DistinctAggregationGeneratorStrategy):
                aggregationStrategy = generatorStrategies[i]

        unionPresent = True
        joinPresent = True

        if len(self._possibleSources) == 0 and unionStrategy and joinStrategy:
            generatorStrategies.remove(unionStrategy)
            generatorStrategies.remove(joinStrategy)
            joinPresent = False
        else:
            if not random.randint(1, 10) % 10 == 0 and unionStrategy:
                generatorStrategies.remove(unionStrategy)
                unionPresent = False

            if unionPresent and joinStrategy:
                generatorStrategies.remove(joinStrategy)
                joinPresent = False

        if (not random.randint(1, 10) % 10 == 0 or joinPresent) and aggregationStrategy:
            generatorStrategies.remove(aggregationStrategy)

        return generatorStrategies
