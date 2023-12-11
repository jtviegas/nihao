from typing import Any

from tgedr.nihao.commons.chainable import Chainable, ContextHandler
from tgedr.nihao.commons.processor import Processor
from tgedr.nihao.commons.sink import Sink
from tgedr.nihao.commons.source import Source


class StartCount(Source):
    def get(self, key: Any, **kwargs) -> Any:
        count = key
        return count


class AddOne(Processor):
    def process(self, obj: Any, **kwargs) -> Any:
        o: int = obj
        return o + 1


class ShowCount(Sink):
    def put(self, obj: Any, key: str, **kwargs) -> Any:
        print(f"obj: {obj}, key: {key}")


def test_handling():
    chain = (
        Chainable(
            whatever=StartCount(),
            context_handler=ContextHandler(
                read_handler=lambda x: x["state"], write_handler=lambda x, y: x.update({"state": y})
            ),
        )
        .next(
            Chainable(
                whatever=AddOne(),
                context_handler=ContextHandler(
                    read_handler=lambda x: x["state"], write_handler=lambda x, y: x.update({"state": y})
                ),
            )
        )
        .next(
            Chainable(
                whatever=ShowCount(),
                context_handler=ContextHandler(
                    read_handler=lambda x: (x["state"], "None"), write_handler=lambda x, y: None
                ),
            )
        )
    )

    context = {"state": 2}
    chain.execute(context)

    assert 3 == (context["state"])
