const protocol = location.protocol === "https:" ? "wss:" : "ws:";
const ws = new WebSocket(`${protocol}//${location.host}/ws`);
const messages = new Map();

const table = document.querySelector(".signals-table");
console.log(table)



ws.onmessage = (event) => {
    // console.log('📨 Raw message:', event);
    
    const msg = JSON.parse(event.data);
    // console.log('📦 Parsed message:', msg);
    
    // Игнорируем служебные сообщения
    if (msg.type === 'connected' || msg.type === 'ping') {
        // console.log('Ignoring service message');
        return;
    }
    if (msg.chart) {
        
    }
    
    const { symbol, data } = msg;

    const tbodyId = `tbody-${symbol}`;
    let tbody = messages.get(symbol);

    // ❌ DELETE
    if (!data || !Array.isArray(data) || data.length === 0) {
        // console.log(`🗑️ Deleting ${symbol}`);
        if (tbody) {
            tbody.remove();
            messages.delete(symbol);
        }
        return;
    }

    // ✅ CREATE
    if (!tbody) {
        // console.log(`➕ Creating tbody for ${symbol}`);
        tbody = document.createElement("tbody");
        tbody.id = tbodyId;
        table.appendChild(tbody);
        messages.set(symbol, tbody);
    }
    
    // 🔄 UPDATE
    // console.log(`🔄 Updating ${symbol} with ${data.length} rows`);
    tbody.innerHTML = "";

    // Заголовок
    tbody.insertAdjacentHTML("beforeend", `
        <tr>
            <td colspan="8"><strong>${symbol}</strong></td>
        </tr>
        <tr>
            <td>Лонг / Шорт</td>
            <td>Цены</td>
            <td>Спред</td>
            <td>Спред с выходом</td>
            <td>Фандинговый</td>
            <td>Объем</td>
            <td>Время жизни</td>
            <td>График</td>
        </tr>
    `);

    // Строки сигналов
    data.forEach(item => {
        tbody.insertAdjacentHTML("beforeend", `
            <tr>
                <td>${item.exchange_long} / ${item.long_type}<br>${item.exchange_short} / ${item.short_type}</td>
                <td>${item.long_price}<br>${item.short_price}</td>
                <td>${item.курсовой}</td>
                <td>${item.курсовой_с_тоталом}</td>
                <td>${item.funding_spread}</td>
                <td>${item.volume} TOKENS<br>${item.volume_usdt} USDT</td>
                <td>${item.lifetime ?? ""}</td>
                <td><button onclick="openChart('${symbol}', '${item.exchange_long}', '${item.long_type}', '${item.exchange_short}', '${item.short_type}', '${item.volume_usdt}')">📈</button></td>            </tr>
        `);
    });
    
    // console.log('✅ Table updated');
};


async function openChart(symbol, long, type_long, short, type_short, volume) {
    const rounded = Math.round(volume / 100) * 100;

    const url = `/api/v1/chart?symbol=${symbol}&exchange_long=${long}&type_long=${type_long}&exchange_short=${short}&type_short=${type_short}&volume=${rounded}`;

    window.open(url, '_blank');
}

// async function openPosition(symbol, long, type_long, short, type_short, quant_coins) {
//     window.open(`/position?symbol=${symbol}&long=${long}&type_long=${type_long}&short=${short}&type_short=${type_short}&quant_coins=${quant_coins}`, '_blank');
// }
