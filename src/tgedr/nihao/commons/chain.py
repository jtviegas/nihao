from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class HandlerException(Exception):
    pass


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
    def _exec(self, context: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError()

    def execute(self, context: Optional[Dict[str, Any]] = None) -> None:
        self._exec(context=context)
        if self._next is not None:
            self._next.execute(context=context)
