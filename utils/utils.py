import random
from typing import List, Any

from utils.contracts import Schema


def random_list_element(elements: List[Any]) -> (int, Any):
    """
    Choose a random element from list and return it along with its ID
    :param elements:
    :return:
    """
    idx = random.randint(0, len(elements) - 1)
    return idx, elements[idx]


def random_field_name(elements: List[str], ignore_new: bool = True) -> str:
    """
    Choose a random element from list and return it along with its ID
    NOTE: only works for numerical fields
    :param elements: the list of field names
    :param ignore_new: ignore new field to be returned
    :return: field name
    """
    idx, ele = random_list_element(elements)
    if not ignore_new and "new" in ele:
        return random_field_name(elements)
    return ele


def shuffle_list(elements: List[Any]) -> List[Any]:
    """
    Shuffle the input list in random order
    :param elements:
    :return: reshuffled list
    """
    random.shuffle(elements)
    return elements


def random_int_between(start: int, end: int) -> int:
    """
    Get a random integer between start and end
    :param start: start value
    :param end: end value
    :return: random integer
    """
    return random.randint(start, end)
