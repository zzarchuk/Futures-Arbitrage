import asyncio
from collections import defaultdict

orderbook = defaultdict(
    lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
)

lock = asyncio.Lock()