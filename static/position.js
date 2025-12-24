const symbol = new URLSearchParams(window.location.search).get("symbol");
const long = new URLSearchParams(window.location.search).get("long");
const type_long = new URLSearchParams(window.location.search).get("type_long");
const short = new URLSearchParams(window.location.search).get("short");
const type_short = new URLSearchParams(window.location.search).get("type_short");
const quant_coins = new URLSearchParams(window.location.search).get("quant_coins");

const ws = new WebSocket(`ws://${location.host}/ws_position?symbol=${symbol}&long=${long}&type_long=${type_long}&short=${short}&type_short=${type_short}&quant_coins=${quant_coins}`);
const table = document.querySelector(".signals-table");


ws.onmessage = (event) => {
    console.log('📨 Raw message:', event);
    
    const msg = JSON.parse(event.data);
    console.log('📦 Parsed message:', msg);
    
    if (msg.type === 'connected' || msg.type === 'ping') return;

    let tbody = table.querySelector("tbody");
    if (!tbody) {
        tbody = document.createElement("tbody");
        table.appendChild(tbody);
    }
    tbody.innerHTML = "";

    tbody.insertAdjacentHTML("beforeend", `
        <tr>
            <td colspan="3"><strong>${msg.symbol}</strong></td>
        </tr>
        <tr>
            <td>Лонг / Шорт</td>
            <td>Объем</td>
            <td>Выход</td>
        </tr>
    `);

    tbody.insertAdjacentHTML("beforeend", `
        <tr>
            <td>${msg.long} / ${msg.type_long}<br>${msg.short} / ${msg.type_short}</td>
            <td>${msg.quant_coins}</td>
            <td><button onclick="exitPosition('${msg.symbol}', '${msg.long}', '${msg.type_long}', '${msg.short}', '${msg.type_short}', '${msg.quant_coins}')">⏹️</button></td>
        </tr>
    `);
};


async function exitPosition(symbol, long, type_long, short, type_short, quant_coins) {
    fetch(
        `/exit_position?symbol=${symbol}&long=${long}&type_long=${type_long}&short=${short}&type_short=${type_short}&quant_coins=${quant_coins}`
    );
}