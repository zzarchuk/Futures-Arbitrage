import asyncio
from collections import defaultdict

# orderbook = defaultdict(
#     lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
# )

# lock = asyncio.Lock()

# subscriptions_lock = asyncio.Lock()

# subscriptions_config = {}


class AppStateForFilter:
    def __init__(self) -> None:
        self.lock_filter = asyncio.Lock()
        self.valid_tokens_filter = {}

class AppStateForArbitrgage:
    def __init__(self) -> None:
        self.lock_arbitrage = asyncio.Lock()
        self.orderbook_arbitrage = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        )
        
class WebsocketState():
    def __init__(self) -> None:
        self.lock_websocket = asyncio.Lock()
        self.websocket_clients = set()

        
state_websocket = WebsocketState()
state_filter = AppStateForFilter()
state_arbitrage = AppStateForArbitrgage()