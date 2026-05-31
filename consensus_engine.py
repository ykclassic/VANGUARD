import json
import logging
import asyncio
import aiosqlite
import pandas as pd
import pandas_ta_classic as ta
import numpy as np
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    with open('config.json', 'r') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.critical("config.json not found. Execution halted.")
    exit(1)

def get_trading_session(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    sessions = []
    
    if 23 <= hour or hour < 8:
        sessions.append("Asian")
    if 7 <= hour < 16:
        sessions.append("London")
    if 13 <= hour < 22:
        sessions.append("New York")
        
    return " / ".join(sessions) if sessions else "Off-Hours"

def generate_ai_prediction(df: pd.DataFrame) -> float:
    """Uses linear regression polynomial fitting to forecast short-term price targets."""
    lookback = CONFIG['vanguard_tier']['ai_lookback_period']
    steps = CONFIG['vanguard_tier']['ai_projection_steps']
    
    if len(df) < lookback:
        return 0.0
        
    y = df['close'].values[-lookback:]
    x = np.arange(len(y))
    
    # 1st degree polynomial fit (linear regression)
    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)
    
    future_x = len(y) + steps
    projected_price = p(future_x)
    return float(projected_price)

async def fetch_dataframe(db, symbol: str, interval: str) -> pd.DataFrame:
    query = "SELECT timestamp, open, high, low, close, volume FROM market_data WHERE symbol = ? AND interval = ? ORDER BY timestamp ASC"
    async with db.execute(query, (symbol, interval)) as cursor:
        rows = await cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.set_index(pd.to_datetime(df['timestamp'], unit='ms'), inplace=True)
        return df

def apply_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 50:
        return df
    
    df.ta.macd(append=True)
    df.ta.rsi(length=14, append=True)
    
    atr_period = CONFIG['risk_management'].get('atr_period', 14)
    df.ta.atr(length=atr_period, append=True)
    
    adx_period = CONFIG['nexus_tier'].get('adx_period', 14)
    df.ta.adx(length=adx_period, append=True)
    
    return df

def get_market_direction(df: pd.DataFrame) -> str:
    if df.empty or len(df) < 50:
        return None
        
    latest = df.iloc[-1]
    macd_val = latest.get('MACD_12_26_9', 0)
    macd_signal = latest.get('MACDs_12_26_9', 0)
    rsi = latest.get('RSI_14', 50)
    
    if macd_val > macd_signal and rsi > 50:
        return "LONG"
    elif macd_val < macd_signal and rsi < 50:
        return "SHORT"
    return None

async def evaluate_consensus():
    symbols = CONFIG['data_ingestion']['symbols']
    
    async with aiosqlite.connect('signals.db', timeout=10.0) as db:
        for symbol in symbols:
            data_frames = {}
            for tf in ['15m', '1h', '4h', '1d']:
                df = await fetch_dataframe(db, symbol, tf)
                data_frames[tf] = apply_indicators(df)
            
            dir_15m = get_market_direction(data_frames['15m'])
            dir_1h = get_market_direction(data_frames['1h'])
            dir_4h = get_market_direction(data_frames['4h'])
            dir_1d = get_market_direction(data_frames['1d'])
            
            consensus_tier = None
            direction = None
            trigger_df = None
            
            if dir_1h and (dir_1h == dir_4h == dir_1d):
                consensus_tier = "DIAMOND"
                direction = dir_1h
                trigger_df = data_frames['1h']
                reason = "1h, 4h, and 1d timeframes fully aligned."
            elif dir_1h and (dir_1h == dir_4h):
                consensus_tier = "SILVER"
                direction = dir_1h
                trigger_df = data_frames['1h']
                reason = "1h and 4h timeframes aligned."
            elif dir_1h and (dir_1h == dir_15m):
                consensus_tier = "GOLD"
                direction = dir_1h
                trigger_df = data_frames['15m']
                reason = "15m and 1h timeframes aligned."
            
            if not consensus_tier or not CONFIG['consensus_engine']['strict_mode']:
                logging.info(f"{symbol} - Holding. Consensus not met.")
                continue
                
            latest = trigger_df.iloc[-1]
            current_price = latest['close']
            atr = latest.get(f'ATRr_{CONFIG["risk_management"]["atr_period"]}', 0)
            
            adx_period = CONFIG['nexus_tier']['adx_period']
            adx_threshold = CONFIG['nexus_tier']['adx_trending_threshold']
            adx_val = latest.get(f'ADX_{adx_period}', 0)
            market_regime = "TRENDING 📈" if adx_val >= adx_threshold else "RANGING ↔️"
            
            timestamp_ms = int(latest['timestamp'])
            trading_session = get_trading_session(timestamp_ms)
            
            # VANGUARD: Generate AI Target
            ai_prediction = generate_ai_prediction(data_frames['1h'])
            
            if atr > 0:
                sl_mult = CONFIG['risk_management']['sl_multiplier']
                tp1_mult = CONFIG['risk_management']['tp1_multiplier']
                tp2_mult = CONFIG['risk_management']['tp2_multiplier']
                
                if direction == "LONG":
                    stop_loss = current_price - (atr * sl_mult)
                    take_profit_1 = current_price + (atr * tp1_mult)
                    take_profit_2 = current_price + (atr * tp2_mult)
                else:
                    stop_loss = current_price + (atr * sl_mult)
                    take_profit_1 = current_price - (atr * tp1_mult)
                    take_profit_2 = current_price - (atr * tp2_mult)
                
                await db.execute('''
                    INSERT INTO alerts (symbol, direction, price, stop_loss, take_profit_1, take_profit_2, indicators_triggered, tier, market_regime, trading_session, ai_prediction, status, timestamp, is_sent)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, direction, current_price, stop_loss, take_profit_1, take_profit_2, reason, consensus_tier, market_regime, trading_session, ai_prediction, 'ACTIVE', timestamp_ms, 0))
                
                await db.commit()
                logging.info(f"{symbol} - {consensus_tier} CONSENSUS MET. Signal registered.")

if __name__ == "__main__":
    asyncio.run(evaluate_consensus())
