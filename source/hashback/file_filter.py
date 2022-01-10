from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Iterable, Tuple

from . import protocol


class SkipThis(Exception):
    pass


@dataclass
class FilterPathNode:
    filter_type: protocol.FilterType = field(default=protocol.FilterType.INCLUDE)
    exceptions: Dict[str, "FilterPathNode"] = field(default_factory=dict)


def normalize_filters(filters: Iterable[protocol.Filter]) -> Tuple[List[str], FilterPathNode]:
    """
    Take a list of filters and build them into a tree of filters.
    :param filters: A list of filters
    :return: A _NormalizedFilter tree structure
    """
    result_node = FilterPathNode(filter_type=protocol.FilterType.INCLUDE)
    patterns = []
    _build_tree(result_node, patterns, filters)
    _prune_redundant_filters(result_node)
    return patterns, result_node


def _build_tree(tree_root: FilterPathNode, patterns: List[str], filters: Iterable[protocol.Filter]):
    for filter_item in filters:
        if filter_item.filter is protocol.FilterType.PATTERN_EXCLUDE:
            patterns.append(filter_item.path)
            continue
        if filter_item.path == '.':
            tree_root.filter_type = filter_item.filter
        else:
            filter_path = Path(filter_item.path)
            position = tree_root
            for directory in filter_path.parts[:-1]:
                if directory not in position.exceptions:
                    position.exceptions[directory] = FilterPathNode(filter_type=position.filter_type)
                position = position.exceptions[directory]
            directory = filter_path.name
            if directory in position.exceptions:
                position.exceptions[directory].filter_type = filter_item.filter
            else:
                position.exceptions[directory] = FilterPathNode(filter_type=filter_item.filter)


def _prune_redundant_filters(filters: FilterPathNode, parent_type: protocol.FileType = protocol.FilterType.INCLUDE):
    """
    It's perfectly legitimate for a user to have redundant filters such as excluding a directory inside another that
    is already excluded.  It's more performant to remove redundant filters before scanning
    :param filters:  The filters to prune
    :param parent_type: The effective filter type of the parent.  At the root this will be INCLUDE (default)
    """
    to_prune = []
    if filters.filter_type == parent_type:
        # If this filter is just doing the same thing as it's parent then it has no effect.  Change it's filter type
        # to propagate the parent (None).
        filters.filter_type = None
    for name, child in filters.exceptions.items():
        _prune_redundant_filters(child, filters.filter_type if filters.filter_type is not None else parent_type)
        if child.filter_type is None and not child.exceptions:
            # Here the child is propagating the parent and it has no exceptions so it has no effect... it's meaningless
            to_prune.append(name)
    for name in to_prune:
        del filters.exceptions[name]
