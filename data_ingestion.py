import os
import json
import asyncio
import logging
import aiohttp
import aiosqlite
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

# Enforce XT.com V4 Spot API endpoint directly, stripping trailing slashes to prevent 404 pathing errors
XT_API_BASE_URL = os.getenv("XT_API_BASE_URL", "https://sapi.xt.com").rstrip('/')

async def init_db():
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                UNIQUE(symbol, interval, timestamp)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                price REAL DEFAULT 0.0,
                stop_loss REAL DEFAULT 0.0,
                take_profit_1 REAL DEFAULT 0.0,
                take_profit_2 REAL DEFAULT 0.0,
                indicators_triggered TEXT NOT NULL,
                tier TEXT DEFAULT 'FREE',
                market_regime TEXT DEFAULT 'UNKNOWN',
                trading_session TEXT DEFAULT 'UNKNOWN',
                ai_prediction REAL DEFAULT 0.0,
                status TEXT DEFAULT 'ACTIVE',
                timestamp INTEGER NOT NULL,
                is_sent INTEGER DEFAULT 0
            )
        ''')
        
        cursor = await db.execute("PRAGMA table_info(alerts)")
        columns = [row[1] for row in await cursor.fetchall()]
        
        migrations_executed = False
        if 'price' not in columns:
            await db.execute("ALTER TABLE alerts ADD COLUMN price REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN stop_loss REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN take_profit_1 REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN take_profit_2 REAL DEFAULT 0.0")
            migrations_executed = True
        if 'tier' not in columns:
            await db.execute("ALTER TABLE alerts ADD COLUMN tier TEXT DEFAULT 'FREE'")
            migrations_executed = True
        if 'market_regime' not in columns:
            await db.execute("ALTER TABLE alerts ADD COLUMN market_regime TEXT DEFAULT 'UNKNOWN'")
            await db.execute("ALTER TABLE alerts ADD COLUMN trading_session TEXT DEFAULT 'UNKNOWN'")
            migrations_executed = True
        if 'ai_prediction' not in columns:
            await db.execute("ALTER TABLE alerts ADD COLUMN ai_prediction REAL DEFAULT 0.0")
            await db.execute("ALTER TABLE alerts ADD COLUMN status TEXT DEFAULT 'ACTIVE'")
            migrations_executed = True
            
        if migrations_executed:
            logging.info("Database schema migrated: Synchronized with VANGUARD architecture.")

        await db.commit()
        logging.info("Database 'signals.db' initialized.")

async def fetch_xt_market_data(session: aiohttp.ClientSession, symbol: str, interval: str) -> list:
    endpoint = f"{XT_API_BASE_URL}/v4/public/kline"
    params = {"symbol": symbol, "interval": interval}
    
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries):
        try:
            async with session.get(endpoint, params=params) as response:
                if response.status == 200:
                    payload = await response.json()
                    records = payload.get("result", [])
                    if isinstance(records, list):
                        return records
                    else:
                        logging.error(f"Unexpected response structure for {symbol}: {payload}")
                        return []
                elif response.status == 429 or response.status >= 500:
                    delay = base_delay * (2 ** attempt)
                    logging.warning(f"HTTP {response.status} for {symbol}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logging.error(f"HTTP {response.status}: Fetch failed for {symbol} at {endpoint}.")
                    return []
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            logging.error(f"Network error during data fetch for {symbol}: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            
    logging.error(f"Failed to fetch {symbol} after {max_retries} attempts.")
    return []

async def ingest_and_sanitize(session: aiohttp.ClientSession, symbol: str, interval: str):
    # Pass the established HTTP pool session downstream instead of instantiating anew
    records = await fetch_xt_market_data(session, symbol, interval)
    
    if not records:
        return

    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        for candle in records:
            if not isinstance(candle, dict) or 't' not in candle:
                logging.warning(f"Skipping malformed candle: {candle}")
                continue
            
            try:
                timestamp = int(candle['t'])
                open_price = float(candle['o'])
                high_price = float(candle['h'])
                low_price = float(candle['l'])
                close_price = float(candle['c'])
                volume = float(candle['q']) 
                
                await db.execute('''
                    INSERT OR IGNORE INTO market_data 
                    (symbol, interval, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, interval, timestamp, open_price, high_price, low_price, close_price, volume))
            except (ValueError, TypeError, KeyError) as e:
                logging.warning(f"Sanitization error for {symbol}: {e} | Candle data: {candle}")
                continue
        
        await db.commit()
        logging.info(f"Ingested {len(records)} records for {symbol} ({interval}).")

async def main():
    await init_db()
    symbols = CONFIG['data_ingestion']['symbols']
    intervals = CONFIG['data_ingestion']['intervals']
    
    # Establish a single persistent HTTP session for connection pooling across all concurrent operations
    async with aiohttp.ClientSession() as session:
        tasks = [ingest_and_sanitize(session, sym, ivl) for sym in symbols for ivl in intervals]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
