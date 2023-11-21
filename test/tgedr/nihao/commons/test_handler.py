from typing import Any, Dict, Optional

from tgedr.nihao.commons.chain import Chain


class Counter:
    score: int = 0


class AddOne(Chain):
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> Any:
        o: Counter = context["state"]
        o.score += 1


class AddTwo(Chain):
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> Any:
        o: Counter = context["state"]
        o.score += 2


def test_handling():
    chain = AddOne().next(AddTwo())
    context = {"state": Counter()}
    chain.execute(context)
    assert 3 == (context["state"]).score
