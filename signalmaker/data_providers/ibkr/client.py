from ib_async import IB

from .config import IBKRConfig
from .errors import IBKRDisabledError, IBKRGatewayConnectionError


class IBKRClient:
    def __init__(self, config: IBKRConfig):
        self.config = config
        self.ib = IB()

    async def connect(self) -> None:
        if not self.config.enabled:
            raise IBKRDisabledError("IBKR provider disabled. Set IBKR_ENABLED=true.")

        if self.ib.isConnected():
            return

        try:
            await self.ib.connectAsync(
                host=self.config.host,
                port=self.config.port,
                clientId=self.config.client_id,
                timeout=20,
            )
        except Exception as exc:
            raise IBKRGatewayConnectionError(
                f"Cannot connect to IB Gateway at {self.config.host}:{self.config.port}"
            ) from exc

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def is_connected(self) -> bool:
        return self.ib.isConnected()
