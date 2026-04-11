import asyncio
import aiohttp
from config.data_blacklist import blacklist
from exchanges.binance.api_binance import binance_spot, binance_futures
from exchanges.bingx.api_bingx import bingx_spot, bingx_futures
from exchanges.bitget.api_bitget import bitget_spot, bitget_futures
from exchanges.bybit.api_bybit import bybit_spot, bybit_futures
from exchanges.gate.api_gate import gateio_spot, gateio_futures
from exchanges.htx.api_htx import htx_spot, htx_futures
from exchanges.kucoin.api_kucoin import kucoin_spot, kucoin_futures
from exchanges.lbank.api_lbank import lbank_spot, lbank_futures
from exchanges.mexc.api_mexc import mexc_spot, mexc_futures
from exchanges.okx.api_okx import okx_spot, okx_futures
from exchanges.mexc.api_mexc import get_mexc_fundings
from utils.exchange_api.filter_api import apply_blacklist
from utils.parser_func.parsers import valid_spread


async def get_tokens():
    data = {}

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            binance_spot(data, session),
            bybit_spot(data, session),
            bingx_spot(data, session),
            mexc_spot(data, session),
            bitget_spot(data, session),
            gateio_spot(data, session),
            okx_spot(data, session),
            kucoin_spot(data, session),
            htx_spot(data, session),
            lbank_spot(data, session),
            binance_futures(data, session),
            bybit_futures(data, session),
            bingx_futures(data, session),
            mexc_futures(data, session),
            bitget_futures(data, session),
            gateio_futures(data, session),
            okx_futures(data, session),
            kucoin_futures(data, session),
            htx_futures(data, session),
            lbank_futures(data, session),
        )

    with_blacklist = apply_blacklist(data, blacklist)

    data_with_2_min_exchange = {
        symbol: exchanges
        for symbol, exchanges in with_blacklist.items()
        if len(exchanges) >= 2
        and any(
            "futures" in ex_data for ex_data in exchanges.values()
        )
    }


    return data_with_2_min_exchange

async def run_filter():
    async with aiohttp.ClientSession() as session:
            tokens = await get_tokens() #get_tokens
            valid_tokens_with_spread = await valid_spread(tokens, session) #valid_spread
    return valid_tokens_with_spread