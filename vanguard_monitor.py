import os
import json
import logging
import asyncio
import aiosqlite
import aiohttp
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

def format_price(val):
    if val >= 1:
        return f"{val:,.2f}"
    return f"{val:,.6f}"

async def monitor_active_trades():
    logging.info("Executing VANGUARD Live Trade Monitor..")
    
    env_key = CONFIG['bridge']['discord_webhook_env_key']
    webhook_url = os.getenv(env_key)
    if not webhook_url: return

    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        async with db.execute("SELECT id, symbol, direction, price, stop_loss, take_profit_1, take_profit_2, timestamp FROM alerts WHERE status = 'ACTIVE' AND is_sent = 1") as cursor:
            active_alerts = await cursor.fetchall()
            
        if not active_alerts:
            logging.info("No active trades to monitor.")
            return

        async with aiohttp.ClientSession() as session:
            for alert in active_alerts:
                alert_id, symbol, direction, entry_price, sl, tp1, tp2, alert_time = alert
                
                # Fetch all 15m candles that occurred AFTER the alert was sent
                async with db.execute("SELECT high, low FROM market_data WHERE symbol = ? AND timestamp >= ?", (symbol, alert_time)) as price_cursor:
                    candles = await price_cursor.fetchall()
                
                new_status = None
                exit_price = 0.0
                outcome_msg = ""
                embed_color = ""
                
                for high, low in candles:
                    if direction == "LONG":
                        if low <= sl:
                            new_status = "STOPPED_OUT"
                            exit_price = sl
                            outcome_msg = "🛑 STOP LOSS HIT"
                            embed_color = CONFIG['bridge']['embed_color_sl']
                            break
                        elif high >= tp2:
                            new_status = "WON_TP2"
                            exit_price = tp2
                            outcome_msg = "🎯 TAKE PROFIT 2 HIT"
                            embed_color = CONFIG['bridge']['embed_color_tp']
                            break
                        elif high >= tp1:
                            new_status = "WON_TP1"
                            exit_price = tp1
                            outcome_msg = "✅ TAKE PROFIT 1 HIT"
                            embed_color = CONFIG['bridge']['embed_color_tp']
                            break
                    elif direction == "SHORT":
                        if high >= sl:
                            new_status = "STOPPED_OUT"
                            exit_price = sl
                            outcome_msg = "🛑 STOP LOSS HIT"
                            embed_color = CONFIG['bridge']['embed_color_sl']
                            break
                        elif low <= tp2:
                            new_status = "WON_TP2"
                            exit_price = tp2
                            outcome_msg = "🎯 TAKE PROFIT 2 HIT"
                            embed_color = CONFIG['bridge']['embed_color_tp']
                            break
                        elif low <= tp1:
                            new_status = "WON_TP1"
                            exit_price = tp1
                            outcome_msg = "✅ TAKE PROFIT 1 HIT"
                            embed_color = CONFIG['bridge']['embed_color_tp']
                            break

                if new_status:
                    fmt_symbol = symbol.upper().replace('_', '/')
                    payload = {
                        "embeds": [{
                            "title": f"🔔 TRADE UPDATE: {fmt_symbol}",
                            "color": int(embed_color, 16),
                            "fields": [
                                {"name": "Result", "value": outcome_msg, "inline": False},
                                {"name": "Entry Price", "value": f"${format_price(entry_price)}", "inline": True},
                                {"name": "Exit Price", "value": f"${format_price(exit_price)}", "inline": True}
                            ],
                            "footer": {"text": "Tier: VANGUARD Trade Monitor"}
                        }]
                    }
                    
                    async with session.post(webhook_url, json=payload) as response:
                        if response.status in [200, 204]:
                            await db.execute("UPDATE alerts SET status = ? WHERE id = ?", (new_status, alert_id))
                            await db.commit()
                            logging.info(f"Trade Monitor: {symbol} resolved as {new_status}")

if __name__ == "__main__":
    asyncio.run(monitor_active_trades())
