from abc import ABC, abstractmethod
from typing import Tuple, List, AnyStr

class BaseParser(ABC):
    @abstractmethod
    def parse(self, data, *args, **kwargs) -> Tuple[AnyStr, List[AnyStr]]:
        pass