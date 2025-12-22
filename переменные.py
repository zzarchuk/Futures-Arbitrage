import asyncio
from collections import defaultdict

orderbook = defaultdict(
    lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
)

lock = asyncio.Lock()

subscriptions_lock = asyncio.Lock()

subscriptions_config = {}