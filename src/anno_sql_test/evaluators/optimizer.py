import logging
from collections import defaultdict
from typing import Any

from anno_sql_test.models import (
    Assertion,
    DualJoinAssertion,
    FusedAssertion,
    MultiAggAssertion,
    SingleAssertAll,
)

_logger = logging.getLogger(__name__)


def group_as_fused(assertions: list[Assertion]) -> list[FusedAssertion[Assertion]]:
    single: list[SingleAssertAll] = []
    multi: list[MultiAggAssertion] = []
    by_keys: defaultdict[Any, list[DualJoinAssertion]] = defaultdict(list)
    others: list[Assertion] = []

    for a in assertions:
        if isinstance(a, SingleAssertAll):
            single.append(a)
        elif isinstance(a, MultiAggAssertion):
            multi.append(a)
        elif isinstance(a, DualJoinAssertion):
            by_keys[a.grouping_key()].append(a)
        else:
            others.append(a)

    result: list[FusedAssertion[Assertion]] = []
    if single:
        _logger.debug("Fused %d SingleAssertAll assertions", len(single))
        result.append(FusedAssertion(assertions=single))
    if multi:
        _logger.debug("Fused %d MultiAggAssertion assertions", len(multi))
        result.append(FusedAssertion(assertions=multi))
    for keys, group in by_keys.items():
        _logger.debug("Fused %d DualJoinAssertion assertions (keys=%s)", len(group), keys)
        result.append(FusedAssertion(assertions=group))
    for a in others:
        result.append(FusedAssertion(assertions=[a]))

    return result
