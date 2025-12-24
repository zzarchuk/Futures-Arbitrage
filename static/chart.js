const symbol = new URLSearchParams(window.location.search).get("symbol");

const ws = new WebSocket(`ws://${location.host}/ws_chart?symbol=${symbol}`);
const messageBox = document.getElementById('message');


ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    console.log(msg)

    // если пришёл просто текст

    messageBox.textContent = JSON.stringify(msg, null, 2);

};