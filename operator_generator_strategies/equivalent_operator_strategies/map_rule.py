from copy import deepcopy

from utils.contracts import *
from operator_generator_strategies.distinct_operator_strategies.distinct_map_strategy import MapOperator
from utils.utils import *


@dataclass
class EquivalentComplexArithmeticMapExpression:
    baseMap: MapOperator
    followUpMaps: List[MapOperator]


@dataclass
class RandomlyOrderedMapExpression:
    arithmeticOp: ArithmeticOperators
    assignmentFiled: str
    expressionFields: List[str]
    schema: Schema


class MapRulesInitializer:

    def __init__(self, schema: Schema):
        mapsWithNewFieldAndSameExpression = self.initializeMapsWithNewFieldAndSameExpression(schema)
        equivalentComplexArithmeticMapExpression = self.initializeMapsWithComplexArithmeticExpressions(schema)
        randomlyOrderedMaps = self.initializeRandomlyOrderedMaps(schema)
        randomlyOrderedMapExpression = self.initializeRandomlyOrderedMapExpression(schema)
        self._mapRule = MapRules(mapsWithNewFieldAndSameExpression, equivalentComplexArithmeticMapExpression,
                                 randomlyOrderedMaps, randomlyOrderedMapExpression)

    @staticmethod
    def initializeMapsWithNewFieldAndSameExpression(schema: Schema) -> List[MapOperator]:
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, originalAssignmentFieldName = random_list_element(numFields)
        numFields.remove(originalAssignmentFieldName)
        _, assignmentFieldName1 = random_list_element(numFields)
        _, assignmentFieldName2 = random_list_element(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))

        expression = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                          FieldAccessExpression(assignmentFieldName2), arithOperation)
        mapsWithNewFieldAndSameExpression = []
        for i in range(10):
            newFiledName = f"{originalAssignmentFieldName}{random_int_between(1, 10)}"
            updateSchema = deepcopy(schema)
            updateSchema.int_fields.append(newFiledName)
            mapWithNewFieldAndSameExpression = MapOperator(
                FieldAssignmentExpression(FieldAccessExpression(newFiledName), expression),
                updateSchema)
            mapsWithNewFieldAndSameExpression.append(mapWithNewFieldAndSameExpression)

        return mapsWithNewFieldAndSameExpression

    @staticmethod
    def initializeMapsWithComplexArithmeticExpressions(schema: Schema) -> EquivalentComplexArithmeticMapExpression:
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, originalAssignmentFieldName = random_list_element(numFields)
        numFields.remove(originalAssignmentFieldName)
        contValue = random_int_between(1, 10)
        baseMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(originalAssignmentFieldName),
                                                        ConstantExpression(str(contValue))), schema)

        _, assignmentFieldName1 = random_list_element(numFields)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression1 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                           FieldAccessExpression(originalAssignmentFieldName), arithOperation)
        expression2 = ArithmeticExpression(FieldAccessExpression(assignmentFieldName1),
                                           ConstantExpression(str(contValue)), arithOperation)

        followUpMap1 = MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentFieldName1), expression1),
                                   schema)
        followUpMap2 = MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentFieldName1), expression2),
                                   schema)

        followUpMaps = [followUpMap1, followUpMap2]
        return EquivalentComplexArithmeticMapExpression(baseMap, followUpMaps)

    @staticmethod
    def initializeRandomlyOrderedMaps(schema: Schema) -> List[MapOperator]:
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, firstFieldName = random_list_element(numFields)
        numFields.remove(firstFieldName)
        contValue1 = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression1 = ArithmeticExpression(FieldAccessExpression(firstFieldName),
                                           ConstantExpression(str(contValue1)), arithOperation)
        firstMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(firstFieldName),
                                                         expression1), schema)

        _, secondFieldName = random_list_element(numFields)
        contValue2 = random_int_between(1, 10)
        _, arithOperation = random_list_element(list(ArithmeticOperators))
        expression2 = ArithmeticExpression(FieldAccessExpression(secondFieldName),
                                           ConstantExpression(str(contValue2)), arithOperation)
        secondMap = MapOperator(FieldAssignmentExpression(FieldAccessExpression(secondFieldName),
                                                          expression2), schema)
        return [firstMap, secondMap]

    @staticmethod
    def initializeRandomlyOrderedMapExpression(schema: Schema) -> RandomlyOrderedMapExpression:
        schemaCopy = deepcopy(schema)
        numFields = schemaCopy.get_numerical_fields()
        _, assignmentFieldName = random_list_element(numFields)
        _, arithOperation = random_list_element([ArithmeticOperators.Add, ArithmeticOperators.Mul])
        return RandomlyOrderedMapExpression(arithOperation, assignmentFieldName, schemaCopy.int_fields, schemaCopy)


class MapRules:
    """
    This class is responsible for applying different rules that generate equivalent queries with map operator
    """

    def __init__(self, mapsWithNewFieldAndSameExpression: List[MapOperator],
                 equivalentComplexArithmeticMapExpression: EquivalentComplexArithmeticMapExpression,
                 randomlyOrderedMaps: List[MapOperator], randomlyOrderedMapExpression: RandomlyOrderedMapExpression):
        self._mapsWithNewFieldAndSameExpression = mapsWithNewFieldAndSameExpression
        self._equivalentComplexArithmeticMapExpression = equivalentComplexArithmeticMapExpression
        self._randomlyOrderedMap = randomlyOrderedMaps
        self._randomlyOrderedMapExpression = randomlyOrderedMapExpression

    def new_field_with_same_expression(self) -> Operator:
        """
        This method is responsible for adding a map operator with a new field name with a fixed formula
        Example: map(a = b *2) vs map(x = b * 2)
        :return:
        """
        _, randomMap = random_list_element(self._mapsWithNewFieldAndSameExpression)
        return randomMap

    def complex_arithmetic_expression(self) -> List[Operator]:
        """
        This method is responsible for generating two map operators for simulating complex arithmetic expressions
        Example:  map(y = 30).map(x = y* z) vs map(y = 30).map(x= 30*z)
        :return:
        """
        baseMap = self._equivalentComplexArithmeticMapExpression.baseMap
        _, followUpMap = random_list_element(self._equivalentComplexArithmeticMapExpression.followUpMaps)
        return [baseMap, followUpMap]

    def map_operator_reordering(self) -> List[Operator]:
        """
        This method is responsible for changing the order of two map operators
        Example:  map(y = 30).map(x = 30) vs map(x = 30).map(y= 30)
        :return:
        """
        return shuffle_list(self._randomlyOrderedMap)

    def map_expression_reordering(self) -> Operator:
        """
        This method is responsible for changing the order of the map expression
        Example:  map(y = x * z) vs map(y = z * x)
        :return:
        """
        randomly_ordered_map_expression = self._randomlyOrderedMapExpression
        assignmentField = randomly_ordered_map_expression.assignmentFiled
        arithmeticOp = randomly_ordered_map_expression.arithmeticOp
        shuffledExpressionField = shuffle_list(randomly_ordered_map_expression.expressionFields);
        schema = randomly_ordered_map_expression.schema

        "Compute the arithmetic expression"
        initialExpression = ArithmeticExpression(FieldAccessExpression(shuffledExpressionField[0]),
                                                 FieldAccessExpression(shuffledExpressionField[1]), arithmeticOp)
        if len(shuffledExpressionField) > 2:
            for i in range(2, len(shuffledExpressionField)):
                initialExpression = ArithmeticExpression(initialExpression,
                                                         FieldAccessExpression(shuffledExpressionField[i]),
                                                         arithmeticOp)

        return MapOperator(FieldAssignmentExpression(FieldAccessExpression(assignmentField), initialExpression), schema)
