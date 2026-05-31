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

async def process_queue():
    logging.info("Executing Headless Discord Signal Bridge..")
    
    env_key = CONFIG['bridge']['discord_webhook_env_key']
    webhook_url = os.getenv(env_key)
    
    if not webhook_url:
        logging.error(f"Missing environment variable: {env_key}")
        return

    client_id = CONFIG.get('client_id', 'Unknown Client')

    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        async with db.execute("SELECT id, symbol, direction, price, stop_loss, take_profit_1, take_profit_2, indicators_triggered, tier, market_regime, trading_session, ai_prediction FROM alerts WHERE is_sent = 0 AND status = 'ACTIVE'") as cursor:
            unsent_alerts = await cursor.fetchall()
            
        if not unsent_alerts:
            return

        async with aiohttp.ClientSession() as session:
            for alert in unsent_alerts:
                alert_id, symbol, direction, price, stop_loss, tp1, tp2, indicators, tier, regime, session_name, ai_prediction = alert
                
                fmt_symbol = symbol.upper().replace('_', '/')
                dir_text = "📈 LONG (Bullish)" if direction == "LONG" else "📉 SHORT (Bearish)"
                
                tier_icons = {"DIAMOND": "💎", "SILVER": "🥈", "GOLD": "🥇"}
                tier_colors = {
                    "DIAMOND": CONFIG['bridge']['embed_color_diamond'],
                    "SILVER": CONFIG['bridge']['embed_color_silver'],
                    "GOLD": CONFIG['bridge']['embed_color_gold']
                }
                
                icon = tier_icons.get(tier, "🟢")
                hex_color = tier_colors.get(tier, "0x00FF00")
                decimal_color = int(hex_color, 16)
                
                title = f"{icon} {tier} CONSENSUS: {fmt_symbol}"
                
                payload = {
                    "embeds": [
                        {
                            "title": title,
                            "color": decimal_color,
                            "fields": [
                                {
                                    "name": "Direction",
                                    "value": dir_text,
                                    "inline": True
                                },
                                {
                                    "name": "Execution Price",
                                    "value": f"${format_price(price)}",
                                    "inline": True
                                },
                                {
                                    "name": "🎯 Trade Parameters (ATR)",
                                    "value": f"**Take Profit 2:** ${format_price(tp2)}\n**Take Profit 1:** ${format_price(tp1)}\n**Stop Loss:** ${format_price(stop_loss)}"
                                },
                                {
                                    "name": "🌐 Market Context (NEXUS)",
                                    "value": f"**Regime:** {regime}\n**Active Session:** {session_name}"
                                },
                                {
                                    "name": "🧠 VANGUARD AI Projection",
                                    "value": f"**Predicted Target:** ${format_price(ai_prediction)}"
                                },
                                {
                                    "name": "📊 AEGIS Matrix Alignment",
                                    "value": indicators
                                }
                            ],
                            "footer": {
                                "text": f"Client ID: {client_id} | Tier: VANGUARD Architecture Active"
                            }
                        }
                    ]
                }
                
                async with session.post(webhook_url, json=payload) as response:
                    if response.status in [200, 204]:
                        await db.execute("UPDATE alerts SET is_sent = 1 WHERE id = ?", (alert_id,))
                        await db.commit()
                        logging.info(f"Dispatched webhook for {symbol} ({direction} - {tier})")
                    elif response.status == 429:
                        logging.warning("Discord Rate Limit Hit. Queuing remainder for next cycle.")
                        break
                    else:
                        logging.error(f"Webhook Delivery Failed HTTP {response.status}")

if __name__ == "__main__":
    asyncio.run(process_queue())
