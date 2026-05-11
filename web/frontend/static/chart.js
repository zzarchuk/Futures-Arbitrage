const params = new URLSearchParams(window.location.search);

const symbol = params.get("symbol") ?? null;
const volume = params.get("volume") ?? null;
const type_long = params.get("type_long") ?? null;
const type_short = params.get("type_short") ?? null;
const exchange_long = params.get("exchange_long") ?? null;
const exchange_short = params.get("exchange_short") ?? null;

const longEx = (exchange_long ?? "").toUpperCase();
const shortEx = (exchange_short ?? "").toUpperCase();
const longType = (type_long ?? "").toUpperCase();
const shortType = (type_short ?? "").toUpperCase();


const btn = document.getElementById("dropdown-btn");
const menu = document.getElementById("dropdown-menu");
const btn_next = document.getElementById("btn_next");

let loaded = false;

let currentCharts = {};
let currentKeys = [];
let currentIndex = 0;


btn_next.onclick = () => {
    if (currentKeys.length === 0) return;

    currentIndex++;

    if (currentIndex >= currentKeys.length) {
        currentIndex = 0;
    }

    const key = currentKeys[currentIndex];
    loadChart(key);
};


btn.onclick = () => {
    menu.classList.toggle("hidden");
    console.log(1232)

    if (!menu.dataset.loaded) {
        loadTokens();
        menu.dataset.loaded = "true";
    }
};

async function loadTokens() {
    const res = await fetch("/api/v1/graphs");
    const tokens = await res.json();

    console.log(tokens);

    menu.innerHTML = "";

    tokens.forEach(token => {
        const div = document.createElement("div");
        div.className = "item";
        div.innerText = token;

        div.onclick = async () => {
            btn.innerText = token + " ▼";
            menu.classList.add("hidden");

            const res = await fetch(`/api/v1/one_chart?symbol=${token}`);
            const data = await res.json();

            console.log(data);

            currentCharts = data;
            currentKeys = Object.keys(data);
            currentIndex = 0;

            const firstKey = currentKeys[0];
            loadChart(firstKey);
        };

        menu.appendChild(div);
    });
}


function loadChart(key) {
    const candles = currentCharts[key];

    const first = candles[0];

    document.getElementById("header").innerText =
        `📈 ${first.token} | ${first.exchange_long} (${first.type_long}) → ${first.exchange_short} (${first.type_short}) | volume: ${first.volume} USDT`;

    loadCandles(candles);
}

async function loadCandles(data) {
    try {
        const formatted = data.map(c => ({
            time: Number(c.time),
            open: Number(c.open_spread),
            high: Number(c.high),
            low: Number(c.low),
            close: Number(c.close),
        }));

        candleSeries.setData(formatted);

        chart.timeScale().fitContent();

    } catch (err) {
        console.error("Ошибка:", err);
    }
}


// 🧠 header
document.getElementById("header").innerText =
    `📈 ${symbol} | ${longEx} (${longType}) → ${shortEx} (${shortType}) | volume: ${volume} USDT`;


// 📊 создаём график
const chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: window.innerWidth,
    height: window.innerHeight - 50,

    layout: {
        background: { color: '#0e0e0e' },
        textColor: '#DDD',
    },

    grid: {
        vertLines: { color: '#1f1f1f' },
        horzLines: { color: '#1f1f1f' },
    },

    crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
    },

    rightPriceScale: {
        borderColor: '#333',
    },

    timeScale: {
        borderColor: '#333',
        timeVisible: true,
        secondsVisible: false,
    },
});


// 🔥 свечи
const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderUpColor: '#26a69a',
    borderDownColor: '#ef5350',
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
});


// 📡 загрузка данных
async function loadCandlesQuery() {
    try {
        const paramsFetch = new URLSearchParams({
            symbol,
            exchange_long,
            exchange_short,
            type_long,
            type_short,
            volume
        });
        const res = await fetch(`/api/v1/candles?${paramsFetch.toString()}`);
        if (!res.ok) {
            console.error("Ошибка API");
            return;
        }

        const data = await res.json();

        if (!data.length) {
            console.log("Нет данных");
            return;
        }

        const formatted = data.map(c => ({
            time: Number(c.time),
            open: Number(c.open),
            high: Number(c.high),
            low: Number(c.low),
            close: Number(c.close),
        }));

        candleSeries.setData(formatted);

        chart.timeScale().fitContent();

    } catch (err) {
        console.error("Ошибка:", err);
    }
}


// 🚀 запуск
loadCandlesQuery();


// 🎯 tooltip
const tooltip = document.getElementById("tooltip");


chart.subscribeCrosshairMove(param => {

    // ✅ ИСПРАВЛЕНО: убрана проверка !param.seriesPrices — этого свойства
    // больше не существует в lightweight-charts v4+, из-за него крашился
    // весь JS и переставали работать все кнопки на странице
    if (
        !param ||
        !param.point ||
        !param.time
    ) {
        tooltip.style.display = "none";
        return;
    }

    // ✅ ИСПРАВЛЕНО: param.seriesPrices.get() → param.seriesData.get()
    // В v4+ данные серии хранятся в seriesData, а не в seriesPrices
    const price = param.seriesData.get(candleSeries);

    if (!price) {
        tooltip.style.display = "none";
        return;
    }

    tooltip.style.display = "block";

    tooltip.style.left = param.point.x + 15 + "px";
    tooltip.style.top = param.point.y + 15 + "px";

    tooltip.innerHTML = `
        O: ${price.open}<br>
        H: ${price.high}<br>
        L: ${price.low}<br>
        C: ${price.close}
    `;
});