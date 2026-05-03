from config.config import state_websocket


async def message_to_site(symbol, user_id, data=None):
    data_to_ws = {
        "symbol": symbol,
        "data": data or []
    }

    async with state_websocket.lock_websocket:
        ws = state_websocket.websocket_clients.get(user_id)

        if not ws:
            return symbol

        try:
            await ws.send_json(data_to_ws)
        except Exception:
            del state_websocket.websocket_clients[user_id]

    return symbol