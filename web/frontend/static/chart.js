const params = new URLSearchParams(window.location.search);

const symbol = params.get("symbol");
const volume = params.get("volume");
const type_long = params.get("type_long");
const type_short = params.get("type_short");
const exchange_long = params.get("exchange_long");
const exchange_short = params.get("exchange_short");

const longEx = exchange_long.toUpperCase();
const shortEx = exchange_short.toUpperCase();
const longType = type_long.toUpperCase();
const shortType = type_short.toUpperCase();
console.log(symbol)
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
async function loadCandles() {
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
loadCandles();


// 🎯 tooltip
const tooltip = document.getElementById("tooltip");

chart.subscribeCrosshairMove(param => {
    if (!param.time || !param.seriesPrices.get(candleSeries)) {
        tooltip.style.display = "none";
        return;
    }

    const price = param.seriesPrices.get(candleSeries);

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


// 📐 адаптив
window.addEventListener('resize', () => {
    chart.applyOptions({
        width: window.innerWidth,
        height: window.innerHeight - 50,
    });
});