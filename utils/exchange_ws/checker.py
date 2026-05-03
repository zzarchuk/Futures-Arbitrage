import asyncio
import copy
from typing import Set
from config.config import state_filter
import logging

logger = logging.getLogger(__name__)


class ChecksumMismatch(Exception):
    pass


class DynamicSubscriptionManager:
    """Менеджер для отслеживания изменений в конфигурации подписок"""

    def __init__(self):
        self.previous_config = {}
        self.change_event = asyncio.Event()

    async def monitor_changes(self, config_dict: dict):
        """Отслеживает изменения в словаре конфигурации"""
        while True:
            await asyncio.sleep(1)  # Проверяем каждую секунду

            async with state_filter.lock_filter:
                current = set(config_dict.keys())

            if current != set(self.previous_config.keys()):
                self.previous_config = copy.deepcopy(config_dict)
                self.change_event.set()  # Сигнал всем классам обновиться

    def get_symbols_for_exchange(self, exchange: str, market: str) -> Set[str]:
        """Возвращает список символов для конкретной биржи и рынка"""
        symbols = set()

        for coin, exchanges in self.previous_config.items():
            if exchange in exchanges:
                markets = exchanges[exchange]
                if market in markets:
                    symbols.add(coin)

        return symbols
    
    def find_data(self, ex, market, symbol):

        if market == 'futures':
            try:
                funding = self.previous_config[symbol][ex][market]['funding']
                get_funding = self.previous_config[symbol][ex][market]['get_funding']
                #max_vol = self.previous_config[symbol][ex][market]['max_vol']
                return funding, get_funding
            except Exception:
                return None, None
        else:
            try:
                max_vol = self.previous_config[symbol][ex][market]['max_vol']
                return max_vol
            except Exception:
                return None


class BaseDynamicWSClient:
    """Базовый класс для динамических WebSocket подключений"""

    def __init__(self, exchange_name: str, manager: DynamicSubscriptionManager):
        self.exchange_name = exchange_name
        self.manager = manager
        self.active_connections = (
            {}
        )  # {conn_id: {'ws': ws, 'symbols': set(), 'task': task}}
        self.is_running = True

    async def _reconnect_wrapper(self, coro_func, conn_id: str):
        """Wrapper с переподключениями"""
        retry_count = 0
        while self.is_running:
            try:
                # await coro_func(conn_id)
                await coro_func()
                retry_count = 0
            except Exception as e:
                retry_count += 1
                wait_time = min(2**retry_count, 60)
                logger.warning(f"❌ {self.exchange_name} conn#{conn_id} error: {e}. Reconnect in {wait_time}s...", exc_info=True)
                
                await asyncio.sleep(wait_time)

    def stop(self):
        """Остановка всех соединений"""
        self.is_running = False
