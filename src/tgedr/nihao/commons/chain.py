from abc import ABC, abstractmethod
from typing import Any, Dict, Optional




class Chain(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next: "Chain" = None

    def next(self, handler: "Chain") -> "Chain":
        if self._next is None:
            self._next: "Chain" = handler
        else:
            self._next.next(handler)
        return self

    @abstractmethod
    def _exec(self, context: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        raise NotImplementedError()

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        self._exec(context=context)
        if self._next is not None:
            self._next.execute(context=context)

class ProcessorChainMixin:

    def next(self, handler: "ChainMixin") -> "ChainMixin":
        if "_next" not in self.__dict__ or self._next is None:
            self._next: "ChainMixin" = handler
        else:
            self._next.next(handler)
        return self
    
    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        self.process(context=context)
        if "_next" in self.__dict__ and self._next is not None:
            self._next.execute(context=context)



