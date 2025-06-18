from abc import ABC, abstractmethod

class FaasPlatform(ABC):
    @abstractmethod
    def deploy(self, func_id: str, image: str) -> None:
        """Deploy an image under service name func_id."""
        pass

    @abstractmethod
    def invoke(self, func_id: str, payload: bytes = None) -> bytes:
        """Invoke service func_id, return raw response."""
        pass
