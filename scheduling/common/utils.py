from typing import List


def to_csv_string(list : List):
    """Formats given list as csv string based on __str__ method of elements

    Args:
        list (List):
    """
    return "".join([str(x) + "," for x in list])[:-1]