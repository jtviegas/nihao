from typing import Any, Dict, Optional
from tgedr.nihao.commons.chain import Chain
from tgedr.nihao.commons.chainable import Chainable, ContextHandler
from tgedr.nihao.commons.processor import Processor
from tgedr.nihao.commons.sink import Sink
from tgedr.nihao.commons.source import Source



class StartCount(Chain):
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> None:
        context["state"] = 2

class AddOne(Chain):
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> None:
        context["state"] = (context["state"] + 1)

class ShowCount(Chain):
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> None:
        print(f"count: {context['state']}")

def test_handling():
    
    chain = StartCount().next(AddOne()).next(ShowCount())
    
    context={}
    chain.execute(context)
    
    assert 3 == (context["state"])
