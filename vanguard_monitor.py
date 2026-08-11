import os
import json
import logging
import asyncio
import aiosqlite
import aiohttp
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

def get_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.critical("config.json not found. Execution halted.")
        exit(1)

CONFIG = get_config()

def format_price(val: float) -> str:
    if val is None:
        return "N/A"
    try:
        val = float(val)
        if val >= 1:
            return f"{val:,.2f}"
        return f"{val:,.6f}"
    except (ValueError, TypeError):
        return "N/A"

def safe_float(val, default=None):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

async def monitor_active_trades():
    logging.info("Executing VANGUARD Live Trade Monitor..")
    
    env_key = CONFIG.get('bridge', {}).get('discord_webhook_env_key', 'DISCORD_WEBHOOK_URL')
    webhook_url = os.getenv(env_key)
    if not webhook_url:
        logging.error(f"Environment variable '{env_key}' not set. Halting.")
        return

    # CRITICAL FIX: Explicit connection variable allows strict closure in finally block
    db = None
    try:
        db = await aiosqlite.connect('signals.db', timeout=10.0)
        async with db.execute(
            "SELECT id, symbol, direction, price, stop_loss, take_profit_1, take_profit_2, timestamp "
            "FROM alerts WHERE status = 'ACTIVE' AND is_sent = 1"
        ) as cursor:
            active_alerts = await cursor.fetchall()
            
        if not active_alerts:
            logging.info("No active trades to monitor.")
            return

        async with aiohttp.ClientSession() as session:
            for alert in active_alerts:
                try:
                    alert_id, symbol, direction, entry_price, sl, tp1, tp2, alert_time = alert
                    
                    sl = safe_float(sl)
                    tp1 = safe_float(tp1)
                    tp2 = safe_float(tp2)
                    
                    query = (
                        "SELECT high, low FROM market_data "
                        "WHERE symbol = ? AND interval = '15m' AND timestamp >= ? "
                        "ORDER BY timestamp ASC"
                    )
                    async with db.execute(query, (symbol, alert_time)) as price_cursor:
                        candles = await price_cursor.fetchall()
                    
                    new_status = None
                    exit_price = 0.0
                    outcome_msg = ""
                    embed_color = ""
                    
                    for high, low in candles:
                        high_val = safe_float(high)
                        low_val = safe_float(low)
                        
                        if high_val is None or low_val is None:
                            continue

                        if direction == "LONG":
                            if sl is not None and low_val <= sl:
                                new_status = "STOPPED_OUT"
                                exit_price = sl
                                outcome_msg = "🛑 STOP LOSS HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_sl', '0xFF0000')
                                break
                            elif tp2 is not None and high_val >= tp2:
                                new_status = "WON_TP2"
                                exit_price = tp2
                                outcome_msg = "🎯 TAKE PROFIT 2 HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_tp', '0x00FF00')
                                break
                            elif tp1 is not None and high_val >= tp1:
                                new_status = "WON_TP1"
                                exit_price = tp1
                                outcome_msg = "✅ TAKE PROFIT 1 HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_tp', '0x00FF00')
                                break

                        elif direction == "SHORT":
                            if sl is not None and high_val >= sl:
                                new_status = "STOPPED_OUT"
                                exit_price = sl
                                outcome_msg = "🛑 STOP LOSS HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_sl', '0xFF0000')
                                break
                            elif tp2 is not None and low_val <= tp2:
                                new_status = "WON_TP2"
                                exit_price = tp2
                                outcome_msg = "🎯 TAKE PROFIT 2 HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_tp', '0x00FF00')
                                break
                            elif tp1 is not None and low_val <= tp1:
                                new_status = "WON_TP1"
                                exit_price = tp1
                                outcome_msg = "✅ TAKE PROFIT 1 HIT"
                                embed_color = CONFIG['bridge'].get('embed_color_tp', '0x00FF00')
                                break

                    if new_status:
                        fmt_symbol = str(symbol).upper().replace('_', '/')
                        
                        try:
                            color_int = int(str(embed_color).replace('#', ''), 16)
                        except ValueError:
                            color_int = 0
                            
                        payload = {
                            "embeds": [{
                                "title": f"🔔 TRADE UPDATE: {fmt_symbol}",
                                "color": color_int,
                                "fields": [
                                    {"name": "Result", "value": outcome_msg, "inline": False},
                                    {"name": "Entry Price", "value": f"${format_price(entry_price)}", "inline": True},
                                    {"name": "Exit Price", "value": f"${format_price(exit_price)}", "inline": True}
                                ],
                                "footer": {"text": "Tier: VANGUARD Trade Monitor"}
                            }]
                        }
                        
                        try:
                            async with session.post(webhook_url, json=payload) as response:
                                if response.status in [200, 204]:
                                    await db.execute("UPDATE alerts SET status = ? WHERE id = ?", (new_status, alert_id))
                                    await db.commit()
                                    logging.info(f"Trade Monitor: {symbol} resolved as {new_status}")
                                else:
                                    logging.error(f"Failed to push Discord update for {symbol}. HTTP {response.status}")
                        except Exception as e:
                            logging.error(f"Network error dispatching Discord notification for {symbol}: {e}")
                            
                except Exception as inner_e:
                    logging.error(f"Integrity error processing alert ID {alert[0]} ({alert[1]}): {inner_e}")
                    continue
                    
    except Exception as e:
        logging.error(f"Critical execution error: {e}")
    finally:
        # CRITICAL FIX: Guarantee connection closes and file lock on signals.db is released for Git runner
        if db:
            await db.close()

if __name__ == "__main__":
    asyncio.run(monitor_active_trades())
