# <!DOCTYPE html>
# <html lang="ru">
# <head>
#   <meta charset="UTF-8" />
#   <meta name="viewport" content="width=device-width, initial-scale=1.0" />
#   <title>Арбитражный Бот - Сигналы</title>
#   <style>
#     * {
#       margin: 0;
#       padding: 0;
#       box-sizing: border-box;
#     }

#     body {
#       font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', sans-serif;
#       background: linear-gradient(135deg, #0a0e27 0%, #1a1f3a 100%);
#       color: #e0e6ed;
#       min-height: 100vh;
#       overflow-x: hidden;
#     }

#     body::before {
#       content: '';
#       position: fixed;
#       top: 0;
#       left: 0;
#       width: 100%;
#       height: 100%;
#       background: 
#         radial-gradient(circle at 20% 50%, rgba(102, 252, 241, 0.03) 0%, transparent 50%),
#         radial-gradient(circle at 80% 80%, rgba(69, 162, 158, 0.03) 0%, transparent 50%);
#       pointer-events: none;
#       z-index: 0;
#     }

#     header {
#       padding: 20px 32px;
#       background: rgba(15, 20, 40, 0.95);
#       backdrop-filter: blur(20px);
#       border-bottom: 1px solid rgba(102, 252, 241, 0.1);
#       box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
#       position: sticky;
#       top: 0;
#       z-index: 100;
#     }

#     .header-content {
#       max-width: 1400px;
#       margin: 0 auto;
#       display: flex;
#       align-items: center;
#       justify-content: space-between;
#       gap: 20px;
#     }

#     .header-title {
#       display: flex;
#       align-items: center;
#       gap: 12px;
#     }

#     .logo {
#       width: 36px;
#       height: 36px;
#       background: linear-gradient(135deg, #66fcf1 0%, #45a29e 100%);
#       border-radius: 10px;
#       display: flex;
#       align-items: center;
#       justify-content: center;
#       font-size: 18px;
#       box-shadow: 0 4px 16px rgba(102, 252, 241, 0.3);
#     }

#     h2 {
#       font-size: 22px;
#       font-weight: 700;
#       background: linear-gradient(135deg, #66fcf1 0%, #45a29e 100%);
#       -webkit-background-clip: text;
#       -webkit-text-fill-color: transparent;
#       background-clip: text;
#     }

#     .status-indicator {
#       display: flex;
#       align-items: center;
#       gap: 8px;
#       padding: 6px 14px;
#       background: rgba(102, 252, 241, 0.1);
#       border-radius: 20px;
#       border: 1px solid rgba(102, 252, 241, 0.2);
#       font-size: 13px;
#       font-weight: 500;
#     }

#     .status-dot {
#       width: 8px;
#       height: 8px;
#       background: #66fcf1;
#       border-radius: 50%;
#       animation: pulse 2s ease-in-out infinite;
#     }

#     @keyframes pulse {
#       0%, 100% { opacity: 1; transform: scale(1); }
#       50% { opacity: 0.5; transform: scale(0.95); }
#     }

#     #messages {
#       padding: 24px 20px;
#       display: grid;
#       grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
#       gap: 16px;
#       max-width: 1400px;
#       margin: 0 auto;
#       position: relative;
#       z-index: 1;
#     }

#     .msg {
#       padding: 18px;
#       border-radius: 14px;
#       background: rgba(31, 40, 51, 0.8);
#       backdrop-filter: blur(10px);
#       border: 1px solid rgba(102, 252, 241, 0.15);
#       box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
#       transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
#       position: relative;
#       overflow: hidden;
#       display: flex;
#       flex-direction: column;
#       gap: 12px;
#     }

#     .msg::before {
#       content: '';
#       position: absolute;
#       top: 0;
#       left: 0;
#       width: 3px;
#       height: 100%;
#       background: linear-gradient(180deg, #66fcf1 0%, #45a29e 100%);
#       transition: width 0.3s ease;
#     }

#     .msg:hover {
#       transform: translateY(-3px);
#       box-shadow: 0 12px 32px rgba(102, 252, 241, 0.25);
#       border-color: rgba(102, 252, 241, 0.3);
#     }

#     .msg:hover::before {
#       width: 4px;
#     }

#     .msg.updated {
#       animation: updatePulse 0.6s ease;
#       border-color: rgba(255, 179, 71, 0.5);
#     }

#     .msg.updated::before {
#       background: linear-gradient(180deg, #ffb347 0%, #ff8c42 100%);
#     }

#     @keyframes updatePulse {
#       0%, 100% { transform: scale(1); }
#       50% { transform: scale(1.02); }
#     }

#     .msg.deleting {
#       animation: deleteSlide 0.4s ease forwards;
#     }

#     @keyframes deleteSlide {
#       to {
#         opacity: 0;
#         transform: scale(0.9);
#       }
#     }

#     /* Компактная карточка */
#     .signal-header {
#       display: flex;
#       justify-content: space-between;
#       align-items: center;
#       margin-bottom: 8px;
#     }

#     .signal-symbol {
#       font-size: 18px;
#       font-weight: 700;
#       color: #66fcf1;
#       letter-spacing: 0.5px;
#     }

#     .signal-spread {
#       font-size: 14px;
#       font-weight: 600;
#       padding: 4px 10px;
#       background: linear-gradient(135deg, rgba(102, 252, 241, 0.2), rgba(69, 162, 158, 0.2));
#       border-radius: 8px;
#       color: #66fcf1;
#     }

#     .signal-exchanges {
#       display: grid;
#       grid-template-columns: 1fr 1fr;
#       gap: 10px;
#       margin-bottom: 10px;
#     }

#     .exchange-box {
#       padding: 10px 12px;
#       background: rgba(15, 20, 40, 0.6);
#       border-radius: 10px;
#       border: 1px solid rgba(102, 252, 241, 0.1);
#     }

#     .exchange-label {
#       font-size: 11px;
#       text-transform: uppercase;
#       letter-spacing: 0.5px;
#       color: #66fcf1;
#       margin-bottom: 4px;
#       font-weight: 600;
#     }

#     .exchange-name {
#       font-size: 14px;
#       font-weight: 600;
#       color: #e0e6ed;
#       margin-bottom: 2px;
#     }

#     .exchange-type {
#       font-size: 12px;
#       color: #8892a6;
#     }

#     .long {
#       border-left: 2px solid #4ade80;
#     }

#     .short {
#       border-left: 2px solid #ef4444;
#     }

#     .signal-volume {
#       display: flex;
#       align-items: center;
#       gap: 6px;
#       font-size: 13px;
#       color: #8892a6;
#       margin-bottom: 10px;
#     }

#     .volume-value {
#       color: #66fcf1;
#       font-weight: 600;
#     }

#     .action-btn {
#       width: 100%;
#       padding: 12px 20px;
#       background: linear-gradient(135deg, #45a29e 0%, #66fcf1 100%);
#       border: none;
#       color: #0b0c10;
#       font-weight: 700;
#       font-size: 14px;
#       border-radius: 10px;
#       cursor: pointer;
#       transition: all 0.3s ease;
#       box-shadow: 0 4px 12px rgba(102, 252, 241, 0.3);
#       position: relative;
#       overflow: hidden;
#     }

#     .action-btn::before {
#       content: '';
#       position: absolute;
#       top: 50%;
#       left: 50%;
#       width: 0;
#       height: 0;
#       border-radius: 50%;
#       background: rgba(255, 255, 255, 0.3);
#       transform: translate(-50%, -50%);
#       transition: width 0.6s, height 0.6s;
#     }

#     .action-btn:hover::before {
#       width: 300px;
#       height: 300px;
#     }

#     .action-btn:hover {
#       transform: translateY(-2px);
#       box-shadow: 0 6px 20px rgba(102, 252, 241, 0.5);
#     }

#     .action-btn:active {
#       transform: translateY(0);
#     }

#     .action-btn.disabled {
#       background: linear-gradient(135deg, #2a2e3b 0%, #3a3e4b 100%);
#       cursor: not-allowed;
#       box-shadow: none;
#     }

#     .action-btn.disabled:hover {
#       transform: none;
#     }

#     .loading {
#       display: inline-block;
#       width: 14px;
#       height: 14px;
#       border: 2px solid rgba(11, 12, 16, 0.3);
#       border-top-color: #0b0c10;
#       border-radius: 50%;
#       animation: spin 0.8s linear infinite;
#       margin-left: 6px;
#       vertical-align: middle;
#     }

#     @keyframes spin {
#       to { transform: rotate(360deg); }
#     }

#     /* Responsive */
#     @media (max-width: 1200px) {
#       #messages {
#         grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
#       }
#     }

#     @media (max-width: 768px) {
#       header {
#         padding: 16px 20px;
#       }

#       h2 {
#         font-size: 18px;
#       }

#       .status-indicator {
#         display: none;
#       }

#       #messages {
#         padding: 20px 16px;
#         grid-template-columns: 1fr;
#       }

#       .msg {
#         padding: 16px;
#       }
#     }
#   </style>
# </head>
# <body>
#   <header>
#     <div class="header-content">
#       <div class="header-title">
#         <div class="logo">🔥</div>
#         <h2>Арбитражный Бот</h2>
#       </div>
#       <div class="status-indicator">
#         <div class="status-dot"></div>
#         <span>Онлайн</span>
#       </div>
#     </div>
#   </header>

#   <div id="messages"></div>

#   <script>
#     const ws = new WebSocket(`ws://${location.host}/ws`);
#     const messages = new Map();
#     const messagesContainer = document.getElementById("messages");

#     ws.onmessage = (event) => {
#       const data = JSON.parse(event.data);

#       if (data.action === "create") {
#         // Извлекаем данные из объектов
#         const exchangeLongName = Object.keys(data.exchange_long)[0] || "N/A";
#         const exchangeLongType = Object.values(data.exchange_long)[0] || "N/A";
#         const exchangeShortName = Object.keys(data.exchange_short)[0] || "N/A";
#         const exchangeShortType = Object.values(data.exchange_short)[0] || "N/A";

#         const div = document.createElement("div");
#         div.className = "msg";
#         div.id = data.id;

#         div.innerHTML = `
#           <div class="signal-header">
#             <div class="signal-symbol">${data.symbol || 'N/A'}</div>
#             <div class="signal-spread">Спред</div>
#           </div>
          
#           <div class="signal-exchanges">
#             <div class="exchange-box long">
#               <div class="exchange-label">LONG</div>
#               <div class="exchange-name">${exchangeLongName}</div>
#               <div class="exchange-type">${exchangeLongType}</div>
#             </div>
            
#             <div class="exchange-box short">
#               <div class="exchange-label">SHORT</div>
#               <div class="exchange-name">${exchangeShortName}</div>
#               <div class="exchange-type">${exchangeShortType}</div>
#             </div>
#           </div>

#           <div class="signal-volume">
#             <span>Объем:</span>
#             <span class="volume-value">${data.volume || 0} USDT</span>
#           </div>
          
#           <button class="action-btn"
#               data-exchange-long='${JSON.stringify(data.exchange_long)}'
#               data-exchange-short='${JSON.stringify(data.exchange_short)}'
#               data-symbol='${data.symbol}'
#               data-volume='${data.volume}'
#               onclick="openPosition(this)">
#               Открыть позицию
#           </button>
#         `;
        
#         messagesContainer.prepend(div);
#         messages.set(data.id, div);

#       } else if (data.action === "update" && messages.has(data.id)) {
#         const div = messages.get(data.id);
        
#         // Обновляем только нужные данные
#         const exchangeLongName = Object.keys(data.exchange_long)[0] || "N/A";
#         const exchangeLongType = Object.values(data.exchange_long)[0] || "N/A";
#         const exchangeShortName = Object.keys(data.exchange_short)[0] || "N/A";
#         const exchangeShortType = Object.values(data.exchange_short)[0] || "N/A";

#         div.querySelector(".signal-symbol").textContent = data.symbol || 'N/A';
#         div.querySelector(".volume-value").textContent = `${data.volume || 0} USDT`;
        
#         const longBox = div.querySelector(".exchange-box.long");
#         longBox.querySelector(".exchange-name").textContent = exchangeLongName;
#         longBox.querySelector(".exchange-type").textContent = exchangeLongType;
        
#         const shortBox = div.querySelector(".exchange-box.short");
#         shortBox.querySelector(".exchange-name").textContent = exchangeShortName;
#         shortBox.querySelector(".exchange-type").textContent = exchangeShortType;

#         div.classList.add("updated");
#         setTimeout(() => div.classList.remove("updated"), 600);

#       } else if (data.action === "delete" && messages.has(data.id)) {
#         const div = messages.get(data.id);
#         div.classList.add("deleting");
#         setTimeout(() => {
#           div.remove();
#           messages.delete(data.id);
#         }, 400);
#       }
#     };

#     async function openPosition(btn) {
#       btn.disabled = true;
#       btn.classList.add("disabled");
#       const originalText = btn.innerText;
#       btn.innerHTML = 'Вход<span class="loading"></span>';

#       const exchangeLong = JSON.parse(btn.dataset.exchangeLong);
#       const exchangeShort = JSON.parse(btn.dataset.exchangeShort);
#       const symbol = btn.dataset.symbol;
#       const volume = parseFloat(btn.dataset.volume);

#       try {
#         const res = await fetch("/open_trade", {
#           method: "POST",
#           headers: { "Content-Type": "application/json" },
#           body: JSON.stringify({ 
#             exchange_long: exchangeLong, 
#             exchange_short: exchangeShort, 
#             symbol, 
#             volume 
#           })
#         });

#         if (res.ok) {
#           btn.innerText = "Сделка открыта ✅";
#           btn.style.background = "linear-gradient(135deg, #4ade80 0%, #22c55e 100%)";
#         } else {
#           btn.innerText = "Ошибка ❌";
#           btn.style.background = "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)";
#           setTimeout(() => {
#             btn.innerText = originalText;
#             btn.disabled = false;
#             btn.classList.remove("disabled");
#             btn.style.background = "";
#           }, 3000);
#         }
#       } catch (e) {
#         console.error(e);
#         btn.innerText = "Ошибка ❌";
#         btn.style.background = "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)";
#         setTimeout(() => {
#           btn.innerText = originalText;
#           btn.disabled = false;
#           btn.classList.remove("disabled");
#           btn.style.background = "";
#         }, 3000);
#       }
#     }

#     ws.onerror = (e) => console.error("WebSocket error:", e);
#     ws.onclose = () => {
#       console.log("WebSocket closed");
#       document.querySelector(".status-dot").style.background = "#ef4444";
#       document.querySelector(".status-indicator span").innerText = "Отключено";
#     };
#   </script>
# </body>
# </html>

b = {1: 1}
a = {2: 2}
zz = a | b
print(zz)