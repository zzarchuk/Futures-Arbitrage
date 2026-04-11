from datetime import timedelta
from collections import defaultdict
import time



def apply_blacklist(data: dict, blacklist: list):
    """
    data — твой словарь монет/бирж
    blacklist — список кортежей:
        [
            ("PYTHUSDT", "mexc", "spot"),
            ("BTCUSDT", "binance", "futures"),
            ("ETHUSDT", "okx", "all"),  # удалить биржу полностью
        ]
    """

    for coin, exchange, market_type in blacklist:
        if exchange == 'all' and market_type == 'all' and coin in data:
            del data[coin]


        if coin not in data:
            continue

        if exchange not in data[coin]:
            continue

        if market_type == "all":
            del data[coin][exchange]
            continue

        if market_type in data[coin][exchange]:
            del data[coin][exchange][market_type]

        if not data[coin][exchange]:
            del data[coin][exchange]

    return data

def get_time_until_funding(funding_timestamp: int, exchange_name) -> str:
    now = time.time()
    if exchange_name in ('bingx', 'okx'):
        funding_timestamp = funding_timestamp / 1000   # type: ignore
    elif exchange_name == 'gateio':
        pass
    else:
        funding_timestamp = funding_timestamp / 1000 # type: ignore

    seconds_left = funding_timestamp - now

    if seconds_left <= 0:
        return "Фандинг уже произошёл."

    td = timedelta(seconds=seconds_left)
    hours, remainder = divmod(int(td.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)

    days = hours // 24
    hours = hours % 24

    if days > 0:
        return f"{days} дн. {hours} ч. {minutes} мин."
    return f"{hours} ч. {minutes} мин."

def filtered_dict(data, exchange, symbol, price=None, futures=None, spot=None, max_vol=None, funding=None, get_funding=None):
    if symbol not in data:
        data[symbol] = {}
    if exchange not in data[symbol]:
        data[symbol][exchange] = defaultdict(dict)

    if futures and price:
        data[symbol][exchange]["futures"]["price"] = price if price is not None else 0
        
        
    if funding is not None and get_funding is not None:
        data[symbol][exchange]['futures']["funding"] = funding * 100  
        data[symbol][exchange]['futures']["get_funding"] = get_time_until_funding(get_funding, exchange)
        
    if spot and price:
        data[symbol][exchange]["spot"]["price"] = price
    
    if futures and max_vol:
        data[symbol][exchange]["futures"]["max_vol"] = max_vol

    if spot and max_vol:
        data[symbol][exchange]["spot"]["max_vol"] = max_vol
        



