import random
from typing import List, Any


def random_list_element(elements: List[Any]) -> (int, Any):
    """
    Choose a random element from list and return it along with its ID
    :param elements:
    :return:
    """
    idx = random.randint(0, len(elements) - 1)
    return idx, elements[idx]
