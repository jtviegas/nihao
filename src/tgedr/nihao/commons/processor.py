from abc import ABC, abstractmethod
import abc
import logging
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession, DataFrame

from tgedr.nihao.commons.chain import ChainMixin

logger = logging.getLogger(__name__)


class ProcessorException(Exception):
    pass


class ProcessorInterface(metaclass=abc.ABCMeta):
    """
    def process(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'process') and 
                callable(subclass.process) or 
                NotImplemented)

    
class Processor(ABC):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__()
        logger.info(f"[__init__|in] ({config})")
        self._config = config
        logger.info(f"[__init__|out]")

    @abstractmethod
    def process(self, context: Optional[Dict[str, Any]] = None) -> Any:
        raise NotImplementedError()


class SparkProcessor(Processor):
    def __init__(self, config: Optional[Dict[str, Any]] = None, spark: Optional[SparkSession] = None):
        super().__init__(config=config)
        self._spark = spark

    @abstractmethod
    def process(self, obj: DataFrame, **kwargs) -> DataFrame:
        raise NotImplementedError()
    
    
class ProcessorChain(ChainMixin, Processor):

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)
        self._next: "ChainMixin" = None

