import logging
from collections import defaultdict
from typing import Any

from anno_sql_test.models import (
    Assertion,
    DualAggAssertion,
    DualJoinAssertion,
    FusedAssertion,
    SingleAssertAll,
)

_logger = logging.getLogger(__name__)


def group_as_fused(assertions: list[Assertion]) -> list[FusedAssertion[Assertion]]:
    single: list[SingleAssertAll] = []
    agg_by_keys: defaultdict[Any, list[DualAggAssertion]] = defaultdict(list)
    join_by_keys: defaultdict[Any, list[DualJoinAssertion]] = defaultdict(list)
    others: list[Assertion] = []

    for a in assertions:
        if isinstance(a, SingleAssertAll):
            single.append(a)
        elif isinstance(a, DualAggAssertion):
            agg_by_keys[a.grouping_key()].append(a)
        elif isinstance(a, DualJoinAssertion):
            join_by_keys[a.grouping_key()].append(a)
        else:
            others.append(a)

    result: list[FusedAssertion[Assertion]] = []
    if single:
        _logger.debug("Fused %d SingleAssertAll assertions", len(single))
        result.append(FusedAssertion(assertions=single))
    for keys, group in agg_by_keys.items():
        _logger.debug("Fused %d DualAggAssertion assertions (keys=%s)", len(group), keys)
        result.append(FusedAssertion(assertions=group))
    for keys, group in join_by_keys.items():
        _logger.debug("Fused %d DualJoinAssertion assertions (keys=%s)", len(group), keys)
        result.append(FusedAssertion(assertions=group))
    for a in others:
        result.append(FusedAssertion(assertions=[a]))

    return result
