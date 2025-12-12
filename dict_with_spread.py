from filter_with_spot import apishechka
import asyncio
import aiohttp

async def арбитраж(пары):
    """
    Генерирует словарь подписок по найденным арбитражным символам.
    """
    subscriptions_config = {}
    
    for symbol, exchanges in пары.items():
        # ---- 1. Находим минимальную цену ----
        min_exchange, min_market_type, min_price = min(
            (
                (exchange, market_type, data['price'])
                for exchange, markets in exchanges.items()
                for market_type, data in markets.items()
            ),
            key=lambda x: x[2]
        )

        if min_price == 0:
            continue

        # ---- 2. Проверяем арбитраж ----
        for exchange, types in exchanges.items():
            for market_type, data in types.items():

                # Пропускаем минимальную цену и неарбитражные комбинации
                if (min_exchange == exchange or 
                    (min_market_type, market_type) in [('spot', 'spot'), ('futures', 'spot')]):
                    continue

                spread = ((data.get('price') - min_price) / min_price * 100)

                if spread >= 4:
                    # Добавляем в словарь подписок
                    if symbol not in subscriptions_config:
                        subscriptions_config[symbol] = {}
                    if exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][exchange] = []

                    if market_type not in subscriptions_config[symbol][exchange]:
                        subscriptions_config[symbol][exchange].append(market_type)

                    # Также добавляем биржу с минимальной ценой
                    if min_exchange not in subscriptions_config[symbol]:
                        subscriptions_config[symbol][min_exchange] = []
                    if min_market_type not in subscriptions_config[symbol][min_exchange]:
                        subscriptions_config[symbol][min_exchange].append(min_market_type)
    #print(subscriptions_config)
    return subscriptions_config


async def арбитраж_бот():

    data = await apishechka()
    current = await арбитраж(data, 60, 60, 25)
    return current




    
#asyncio.run(main())