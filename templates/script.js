const ws = new WebSocket(`ws://${location.host}/ws`);
const messages = new Map();
const messagesContainer = document.getElementById("messages");

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);

    if (data.action === "create") {
        const div = document.createElement("div");
        div.className = "msg";
        div.id = data.id;

        if (data.message_type === "update_spread") {
            div.innerHTML = `
                <div class="text">${data.text}</div>
                <button class="action-btn" onclick="exitMessage('${data.id}')">
                    Выход
                </button>
            `;
        } else {
            div.innerHTML = `
                <div class="text">${data.text}</div>
                <button class="action-btn"
                    data-exchange-long='${JSON.stringify(data.exchange_long)}'
                    data-exchange-short='${JSON.stringify(data.exchange_short)}'
                    data-symbol='${data.symbol}'
                    data-volume='${data.volume}'
                    data-long-price='${data.long_price}'
                    data-short-price='${data.short_price}'
                    data-spread='${data.spread}'
                    onclick="openPosition(this)">
                    Схождения
                </button>
            `;
        }

        messagesContainer.prepend(div);
        messages.set(data.id, div);
    }

    else if (data.action === "update" && messages.has(data.id)) {
        const div = messages.get(data.id);
        div.querySelector(".text").innerHTML = data.text;
    }

    else if (data.action === "delete" && messages.has(data.id)) {
        const div = messages.get(data.id);
        div.remove();
        messages.delete(data.id);
    }
};

async function openPosition(btn) {
    btn.disabled = true;
    btn.classList.add("disabled");
    btn.innerText = "Парсинг";

    const exchangeLong = JSON.parse(btn.dataset.exchangeLong);
    const exchangeShort = JSON.parse(btn.dataset.exchangeShort);

    try {
        const res = await fetch("/open_trade", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                exchange_long: exchangeLong,
                exchange_short: exchangeShort,
                symbol: btn.dataset.symbol,
                volume: Number(btn.dataset.volume),
                long_price: Number(btn.dataset.longPrice),
                short_price: Number(btn.dataset.shortPrice),
                spread: Number(btn.dataset.spread),
            })
        });

        if (res.ok) {
            btn.innerText = "OK";
        } else {
            btn.innerText = "ERR";
            btn.disabled = false;
            btn.classList.remove("disabled");
        }
    } catch {
        btn.innerText = "ERR";
        btn.disabled = false;
        btn.classList.remove("disabled");
    }
}

async function exitMessage(messageId) {
    try {
        const res = await fetch("/exit_message", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message_id: messageId })
        });

        if (res.ok) {
            const div = messages.get(messageId);
            if (div) {
                div.remove();
                messages.delete(messageId);
            }
        }
    } catch (e) {
        console.error(e);
    }
}

ws.onerror = (e) => console.error("WebSocket error:", e);
ws.onclose = () => {
    console.log("WebSocket closed");
    document.querySelector(".status-dot").style.background = "#ef4444";
    document.querySelector(".status-indicator span").innerText = "Отключено";
};
