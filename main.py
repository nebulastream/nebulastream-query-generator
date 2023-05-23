import random
from typing import List
import click
import yaml

from operator_generator_strategies.containment_operator_strategies.filter_containment_strategy import \
    FilterContainmentGeneratorStrategy
from operator_generator_strategies.containment_operator_strategies.projection_containment_strategy import \
    ProjectionContainmentGeneratorStrategy
from operator_generator_strategies.containment_operator_strategies.window_aggregation_containment_strategy import \
    WindowAggregationContainmentGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_substitute_map_expression_startegy import \
    FilterSubstituteMapExpressionGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.join_equivalent_strategy import \
    JoinEquivalentJoinGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_expression_reorder_strategy import \
    FilterExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.filter_operator_reorder_strategy import \
    FilterOperatorReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_expression_reorder_strategy import \
    MapExpressionReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.map_operator_reorder_startegy import \
    MapOperatorReorderGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.project_equivalent_strategy import \
    ProjectEquivalentProjectGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.aggregation_equivalent_aggregation_strategy import \
    AggregationEquivalentAggregationGeneratorStrategy
from operator_generator_strategies.equivalent_operator_strategies.union_equivalent_strategy import \
    UnionEquivalentUnionGeneratorStrategy
from query_generator.query import Query
from utils.contracts import Schema
from operator_generator_strategies.distinct_operator_strategies.distinct_filter_strategy import \
    DistinctFilterGeneratorStrategy
from query_generator.generator import QueryGenerator
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy1 import \
    DistinctMapGeneratorStrategy1
from operator_generator_strategies.distinct_operator_strategies.distinct_projection_strategy import \
    DistinctProjectionGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_aggregation_strategy import \
    DistinctAggregationGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_union_strategy import \
    DistinctUnionGeneratorStrategy
from operator_generator_strategies.distinct_operator_strategies.distinct_join_strategy import \
    DistinctJoinGeneratorStrategy


@click.command()
@click.option('-cf', '--config-file', help='Location of the configuration file.', type=click.STRING)
def run(config_file):
    print("Loading configurations")
    file = open(config_file, 'r')
    configuration = yaml.load(file, yaml.Loader)
    print(configuration)

    possibleSources = []
    for sourceConf in configuration['sourceList']:
        source = Schema(name=sourceConf['streamName'], int_fields=sourceConf['intFields'],
                        string_fields=sourceConf['stringFields'],
                        timestamp_fields=sourceConf['timestampFields'], double_fields=sourceConf['doubleFields'],
                        fieldNameMapping={})
        possibleSources.append(source)

    generateEquivalentQueries = configuration['generateEquivalentQueries']
    numOfDistinctSourcesToUse = configuration['sourcesToUse']
    numberOfQueries = configuration['noQueries']
    workloadType = configuration['workloadType']

    equivalentQueries: List[Query] = []
    containmentQueries: List[Query] = []
    distinctQueries: List[Query] = []
    distinctSourcesUsed: List[Query] = []

    if workloadType == "Normal":

        if generateEquivalentQueries == "equivalence":
            percentageOfRandomQueries = configuration['equivalenceConfig']['percentageOfRandomQueries']
            numberOfEquivalentQueryGroups = configuration['equivalenceConfig']['noOfEquivalentQueryGroups']
            percentageOfEquivalence = configuration['equivalenceConfig']['percentageOfEquivalence']
            numberOfQueriesPerSource = int(numberOfQueries / numOfDistinctSourcesToUse)
            numberOfQueriesPerGroup = int(numberOfQueriesPerSource / numberOfEquivalentQueryGroups)
            # Iterate over sources
            for i in range(numOfDistinctSourcesToUse):
                sourcesToUse = []
                # Loop till we find a distinct set of sources to generate queries
                while len(sourcesToUse) == 0:
                    numOfSourceToUse = random.randint(1, 2)
                    sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                    sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                    if str(sourcesToUse) not in distinctSourcesUsed:
                        distinctSourcesUsed.append(str(sourcesToUse))
                    else:
                        sourcesToUse = []

                baseSource = sourcesToUse[0]
                sourcesToUse.remove(baseSource)
                for j in range(numberOfEquivalentQueryGroups):
                    randomQueries = int((numberOfQueriesPerGroup * percentageOfRandomQueries) / 100)
                    equivalentQueries.extend(getEquivalentQueries(numberOfQueriesPerGroup - randomQueries,
                                                                  percentageOfEquivalence,
                                                                  baseSource, sourcesToUse))
                    distinctQueries.extend(getDistinctQueries(randomQueries, baseSource, sourcesToUse))

            # Populate remaining queries
            remainingQueries = numberOfQueries - (
                        numberOfQueriesPerGroup * numOfDistinctSourcesToUse * numberOfEquivalentQueryGroups)
            if remainingQueries > 0:
                sourcesToUse = []
                # Loop till we find a distinct set of sources to generate queries
                while len(sourcesToUse) == 0:
                    numOfSourceToUse = random.randint(1, 2)
                    sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                    sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                    if str(sourcesToUse) not in distinctSourcesUsed:
                        distinctSourcesUsed.append(str(sourcesToUse))
                    else:
                        sourcesToUse = []

                baseSource = sourcesToUse[0]
                sourcesToUse.remove(baseSource)
                for i in range(int(remainingQueries / numberOfQueriesPerGroup)):
                    equivalentQueries.extend(getEquivalentQueries(numberOfQueriesPerGroup, percentageOfEquivalence,
                                                                  baseSource, sourcesToUse))

        elif generateEquivalentQueries == "distinct":
            numberOfDistinctQueriesPerSource = numberOfQueries / numOfDistinctSourcesToUse
            for i in range(numOfDistinctSourcesToUse):
                numOfSourceToUse = 1
                if numOfDistinctSourcesToUse > 1:
                    numOfSourceToUse = 2

                sourcesToUse = []
                # Loop till we find a distinct set of sources to generate queries
                while len(sourcesToUse) == 0:
                    sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                    sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                    if str(sourcesToUse) not in distinctSourcesUsed:
                        distinctSourcesUsed.append(str(sourcesToUse))
                    else:
                        sourcesToUse = []

                baseSource = sourcesToUse[0]
                sourcesToUse.remove(baseSource)
                distinctQueries.extend(
                    getDistinctQueries(numberOfDistinctQueriesPerSource, baseSource, sourcesToUse))
        elif generateEquivalentQueries == "containment":
            percentageOfRandomQueries = configuration['containmentConfig']['percentageOfRandomQueries']
            percentageOfEquivalentQueries = configuration['containmentConfig']['percentageOfEquivalentQueries']
            noOfContainmentQueryGroups = configuration['containmentConfig']['noOfContainmentQueryGroups']
            percentageOfEquivalence = configuration['containmentConfig']['percentageOfEquivalence']
            allowMultiContainment = configuration['containmentConfig']['allowMultiContainment']
            numberOfQueriesPerSource = int(numberOfQueries / numOfDistinctSourcesToUse)
            numberOfQueriesPerGroup = int(numberOfQueriesPerSource / noOfContainmentQueryGroups)
            # Iterate over sources
            for i in range(numOfDistinctSourcesToUse):
                sourcesToUse = []
                # Loop till we find a distinct set of sources to generate queries
                while len(sourcesToUse) == 0:
                    numOfSourceToUse = random.randint(1, 2)
                    sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                    sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                    if str(sourcesToUse) not in distinctSourcesUsed:
                        distinctSourcesUsed.append(str(sourcesToUse))
                    else:
                        sourcesToUse = []

                baseSource = sourcesToUse[0]
                sourcesToUse.remove(baseSource)
                for j in range(noOfContainmentQueryGroups):
                    randomQueries = int((numberOfQueriesPerGroup * percentageOfRandomQueries) / 100)
                    numberOfEquivalentQueries = int((numberOfQueriesPerGroup * percentageOfEquivalentQueries) / 100)
                    containmentQueries.extend(getContainmentQueries(numberOfQueriesPerGroup - randomQueries - numberOfEquivalentQueries,
                                                                  percentageOfEquivalence, allowMultiContainment,
                                                                  baseSource, sourcesToUse))
                    distinctQueries.extend(getDistinctQueries(randomQueries, baseSource, sourcesToUse))
                    equivalentQueries.extend(getEquivalentQueries(numberOfEquivalentQueries, percentageOfEquivalence,
                                                                  baseSource, sourcesToUse))

            # Populate remaining queries
            remainingQueries = numberOfQueries - (
                    numberOfQueriesPerGroup * numOfDistinctSourcesToUse * noOfContainmentQueryGroups)
            if remainingQueries > 0:
                sourcesToUse = []
                # Loop till we find a distinct set of sources to generate queries
                while len(sourcesToUse) == 0:
                    numOfSourceToUse = random.randint(1, 2)
                    sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                    sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                    if str(sourcesToUse) not in distinctSourcesUsed:
                        distinctSourcesUsed.append(str(sourcesToUse))
                    else:
                        sourcesToUse = []

                baseSource = sourcesToUse[0]
                sourcesToUse.remove(baseSource)
                for i in range(int(remainingQueries / numberOfQueriesPerGroup)):
                    containmentQueries.extend(getContainmentQueries(numberOfQueriesPerGroup, percentageOfEquivalence, allowMultiContainment,
                                                                    baseSource, sourcesToUse))

    elif workloadType == "BiasedForHybrid":
        numberOfEquivalentQueryGroups = configuration['equivalenceConfig']['noOfEquivalentQueryGroups']
        percentageOfEquivalence = configuration['equivalenceConfig']['percentageOfEquivalence']
        numberOfGroupsPerSource = int(numberOfEquivalentQueryGroups / numOfDistinctSourcesToUse)
        numberOfQueriesPerGroup = int(numberOfQueries / numberOfEquivalentQueryGroups)
        # Iterate over sources
        for i in range(numOfDistinctSourcesToUse):
            sourcesToUse = []
            # Loop till we find a distinct set of sources to generate queries
            while len(sourcesToUse) == 0:
                numOfSourceToUse = random.randint(1, 2)
                sourcesToUse = random.sample(possibleSources, k=numOfSourceToUse)
                sourcesToUse.sort(key=lambda x: x.name, reverse=False)
                if str(sourcesToUse) not in distinctSourcesUsed:
                    distinctSourcesUsed.append(str(sourcesToUse))
                else:
                    sourcesToUse = []
            baseSource = sourcesToUse[0]
            sourcesToUse.remove(baseSource)
            equivalentQueries.extend(getEquivalentQueriesForHybrid(numberOfGroupsPerSource, numberOfQueriesPerGroup,
                                                                   percentageOfEquivalence,
                                                                   baseSource, sourcesToUse))

    queries: List[Query] = []
    queries.extend(equivalentQueries)
    queries.extend(distinctQueries)
    queries.extend(containmentQueries)
    random.shuffle(queries)
    # Write queries into file
    with open("generated_queries.txt", "w+") as f:
        for query in queries:
            f.write(query.generate_code())
            f.write("\n")


def getDistinctQueries(numberOfQueriesToGenerate: int, baseSource: Schema, possibleSources: List[Schema]) -> \
        List[Query]:
    filter_generator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    map_generator = DistinctMapGeneratorStrategy1()
    project_generator = DistinctProjectionGeneratorStrategy()
    aggregation_generator = DistinctAggregationGeneratorStrategy()
    union_generator = DistinctUnionGeneratorStrategy(possibleSources)
    join_generator = DistinctJoinGeneratorStrategy(possibleSources)
    distinctOperatorGeneratorStrategies = [filter_generator, map_generator, project_generator, union_generator,
                                           join_generator, aggregation_generator]
    return QueryGenerator(baseSource, possibleSources, numberOfQueriesToGenerate, [],
                          distinctOperatorGeneratorStrategies, []).generate()


def getEquivalentQueries(numberOfQueriesPerGroup: int, percentageOfEquivalence: int, baseSource: Schema,
                         possibleSources: List[Schema]) -> List[Query]:
    # Initialize instances of each generator strategy
    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    project_equivalent_project_strategy = ProjectEquivalentProjectGeneratorStrategy()
    aggregate_equivalent_aggregate_strategy = AggregationEquivalentAggregationGeneratorStrategy()

    # Remove the base source from possible sources for binary operator and initialize generator strategies
    union_equivalent_strategies = UnionEquivalentUnionGeneratorStrategy(possibleSources)
    join_equivalent_strategies = JoinEquivalentJoinGeneratorStrategy(possibleSources)

    equivalentOperatorGeneratorStrategies = [
        map_expression_reorder_strategy,
        map_operator_reorder_strategy,
        filter_substitute_map_expression_strategy,
        filter_expression_reorder_strategy,
        filter_operator_reorder_strategy,
        project_equivalent_project_strategy
    ]

    filterGenerator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    mapGenerator = DistinctMapGeneratorStrategy1()
    aggregateGenerator = DistinctAggregationGeneratorStrategy()
    projectGenerator = DistinctProjectionGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filterGenerator, mapGenerator, aggregateGenerator, projectGenerator,
                                           mapGenerator]

    distinctOperators = 5 - int((5 * percentageOfEquivalence) / 100)
    distinctOperatorGenerators = random.sample(distinctOperatorGeneratorStrategies, distinctOperators)
    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 5 - distinctOperators)

    if len(possibleSources) >= 1:
        unionPresent = False
        if random.randint(1, 2) % 2 == 0:
            equivalentOperatorGenerators.append(union_equivalent_strategies)
            unionPresent = True

        if not unionPresent:
            equivalentOperatorGenerators.append(join_equivalent_strategies)

    if random.randint(1, 5) % 5 == 0:
        equivalentOperatorGenerators.append(aggregate_equivalent_aggregate_strategy)

    return QueryGenerator(baseSource, possibleSources, numberOfQueriesPerGroup, equivalentOperatorGenerators,
                          distinctOperatorGenerators, []).generate()


def getContainmentQueries(numberOfQueriesPerGroup: int, percentageOfEquivalence: int, allowMultiContainment: bool, baseSource: Schema,
                          possibleSources: List[Schema]) -> List[Query]:
    # Initialize instances of each generator strategy
    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    project_equivalent_project_strategy = ProjectEquivalentProjectGeneratorStrategy()
    aggregate_equivalent_aggregate_strategy = AggregationEquivalentAggregationGeneratorStrategy()
    filter_containment_strategy = FilterContainmentGeneratorStrategy()
    window_aggregation_containment_strategy = WindowAggregationContainmentGeneratorStrategy()
    projection_containment_strategy = ProjectionContainmentGeneratorStrategy()

    # Remove the base source from possible sources for binary operator and initialize generator strategies
    union_equivalent_strategies = UnionEquivalentUnionGeneratorStrategy(possibleSources)
    join_equivalent_strategies = JoinEquivalentJoinGeneratorStrategy(possibleSources)

    equivalentOperatorGeneratorStrategies = [
        map_expression_reorder_strategy,
        map_operator_reorder_strategy,
        filter_substitute_map_expression_strategy,
        filter_expression_reorder_strategy,
        filter_operator_reorder_strategy,
        project_equivalent_project_strategy
    ]
    containmentOperatorGeneratorStrategies = [
        window_aggregation_containment_strategy,
        projection_containment_strategy,
        filter_containment_strategy
    ]

    filterGenerator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    mapGenerator = DistinctMapGeneratorStrategy1()
    aggregateGenerator = DistinctAggregationGeneratorStrategy()
    projectGenerator = DistinctProjectionGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filterGenerator, mapGenerator, aggregateGenerator, projectGenerator,
                                           mapGenerator]

    distinctOperators = 5 - int((5 * percentageOfEquivalence) / 100)
    distinctOperatorGenerators = random.sample(distinctOperatorGeneratorStrategies, distinctOperators)
    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 5 - distinctOperators)
    containmentOperatorGenerators = []
    if not allowMultiContainment:
        containmentOperatorGenerators = random.sample(containmentOperatorGeneratorStrategies, 1)

    if len(possibleSources) >= 1:
        unionPresent = False
        if random.randint(1, 2) % 2 == 0:
            equivalentOperatorGenerators.append(union_equivalent_strategies)
            unionPresent = True

        if not unionPresent:
            equivalentOperatorGenerators.append(join_equivalent_strategies)

    if random.randint(1, 5) % 5 == 0 and distinctOperators == 0:
        equivalentOperatorGenerators.append(aggregate_equivalent_aggregate_strategy)

    return QueryGenerator(baseSource, possibleSources, numberOfQueriesPerGroup, equivalentOperatorGenerators,
                          distinctOperatorGenerators, containmentOperatorGenerators).generate()


def getEquivalentQueriesForHybrid(numberOfGroupsPerSource: int, numberOfQueriesPerGroup: int,
                                  percentageOfEquivalence: int,
                                  baseSource: Schema,
                                  possibleSources: List[Schema]) -> List[Query]:
    # Initialize instances of each generator strategy
    filter_expression_reorder_strategy = FilterExpressionReorderGeneratorStrategy()
    filter_operator_reorder_strategy = FilterOperatorReorderGeneratorStrategy()
    map_expression_reorder_strategy = MapExpressionReorderGeneratorStrategy()
    map_operator_reorder_strategy = MapOperatorReorderGeneratorStrategy()
    filter_substitute_map_expression_strategy = FilterSubstituteMapExpressionGeneratorStrategy()
    project_equivalent_project_strategy = ProjectEquivalentProjectGeneratorStrategy()
    aggregate_equivalent_aggregate_strategy = AggregationEquivalentAggregationGeneratorStrategy()

    # Remove the base source from possible sources for binary operator and initialize generator strategies
    union_equivalent_strategies = UnionEquivalentUnionGeneratorStrategy(possibleSources)

    equivalentOperatorGeneratorStrategies = [
        map_expression_reorder_strategy,
        map_operator_reorder_strategy,
        filter_substitute_map_expression_strategy,
        filter_expression_reorder_strategy,
        filter_operator_reorder_strategy,
        project_equivalent_project_strategy
    ]

    filterGenerator = DistinctFilterGeneratorStrategy(max_number_of_predicates=2)
    mapGenerator = DistinctMapGeneratorStrategy1()
    aggregateGenerator = DistinctAggregationGeneratorStrategy()
    projectGenerator = DistinctProjectionGeneratorStrategy()
    distinctOperatorGeneratorStrategies = [filterGenerator, mapGenerator, aggregateGenerator, projectGenerator,
                                           mapGenerator]

    distinctOperators = 5 - int((5 * percentageOfEquivalence) / 100)
    distinctOperatorGenerators = random.sample(distinctOperatorGeneratorStrategies, distinctOperators)
    equivalentOperatorGenerators = random.sample(equivalentOperatorGeneratorStrategies, 5)

    if random.randint(1, 2) % 2 == 0:
        equivalentOperatorGenerators.append(union_equivalent_strategies)

    if random.randint(1, 5) % 5 == 0:
        equivalentOperatorGenerators.append(aggregate_equivalent_aggregate_strategy)

    queries: List[Query] = []
    generator = QueryGenerator(baseSource, possibleSources, 1, equivalentOperatorGenerators,
                               distinctOperatorGenerators)
    for i in range(numberOfGroupsPerSource):
        temp = generator.generate()
        for j in range(numberOfQueriesPerGroup):
            queries.extend(temp)

    return queries


if __name__ == '__main__':
    run()
