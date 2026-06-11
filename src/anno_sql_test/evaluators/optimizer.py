from anno_sql_test.models import (
    Assertion,
    DualJoinAssertion,
    FusedAssertion,
    MultiAggAssertion,
    SingleAssertAll,
)


def group_as_fused(assertions: list[Assertion]) -> list[FusedAssertion[Assertion]]:
    single: list[SingleAssertAll] = []
    multi: list[MultiAggAssertion] = []
    by_keys: dict[tuple[str, ...], list[DualJoinAssertion]] = {}
    others: list[Assertion] = []

    for a in assertions:
        if isinstance(a, SingleAssertAll):
            single.append(a)
        elif isinstance(a, MultiAggAssertion):
            multi.append(a)
        elif isinstance(a, DualJoinAssertion):
            key = tuple(sorted(a.keys))
            by_keys.setdefault(key, []).append(a)
        else:
            others.append(a)

    result: list[FusedAssertion[Assertion]] = []
    if single:
        result.append(FusedAssertion(assertions=single))
    if multi:
        result.append(FusedAssertion(assertions=multi))
    for group in by_keys.values():
        result.append(FusedAssertion(assertions=group))
    for a in others:
        result.append(FusedAssertion(assertions=[a]))

    return result
