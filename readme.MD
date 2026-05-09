# Crypto Arbitrage Parser

---

## Descripción

Sistema API asíncrono para detectar oportunidades de arbitraje cubierto en 9 exchanges de criptomonedas, tanto en mercados spot como de futuros.

El sistema recopila datos de mercado en tiempo real, procesa libros de órdenes, calcula spreads y proporciona datos estructurados para análisis y visualización.

### Exchanges compatibles:
- Binance  
- Bybit  
- BingX  
- Bitget  
- OKX  
- MEXC  
- Gate  
- HTX  
- KuCoin  

---

## Arquitectura

El sistema está construido como un pipeline distribuido completamente asíncrono:

- Recolectores de datos de exchanges (REST / WebSockets)
- Streaming en tiempo real del libro de órdenes
- Procesador de datos de mercado
- Motor de arbitraje (cálculo de spread y precio ponderado por volumen)
- Sistema de filtrado (lista negra / filtros por exchange)
- Capa de base de datos (SQLite)
- API REST (FastAPI)
- Capa de streaming WebSocket
- Sistema de autenticación (JWT)

---

## Stack Tecnológico

- Python 3.12.3
- FastAPI
- SQLAlchemy (ORM asíncrono)
- SQLite
- WebSockets
- Autenticación JWT
- APIs de exchanges (REST + WS)
- Arquitectura completamente asíncrona (async/await)

---

## Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto:

```env
DATABASE_URL=sqlite+aiosqlite:///data.db
JWT_SECRET=tu_clave_secreta
```

---

## Instalación
```
git clone https://github.com/usuario/proyecto.git

cd run

python -m venv venv

Linux / Mac
source venv/bin/activate

Windows
venv\Scripts\activate

pip install -r requirements.txt
```
---

## Ejecución

`python3 run.py`

---

## Funcionalidades

* Agregación en tiempo real de datos de mercado de 9 exchanges
* Detección de oportunidades de arbitraje entre exchanges
* Análisis de spread entre spot y futuros
* Procesamiento del libro de órdenes con cálculo de precio basado en volumen
* Sistema de lista negra (monedas / exchanges)
* Filtrado de exchanges, volúmenes, spreads, etc.
* Almacenamiento histórico de datos para visualización de gráficos
* Streaming en tiempo real por WebSocket
* Sistema de autenticación basado en JWT

---

## Aviso Legal

Este proyecto es únicamente con fines educativos y de investigación.
No constituye asesoramiento financiero.

---

## Autor

Telegram: @qqqwerrrtt