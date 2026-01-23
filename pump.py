import ccxt.async_support as ccxt  # Используем асинхронную версию
import asyncio
import aiosqlite  # <--- Добавляем это
import logging
import pandas as pd
import time
import aiohttp  # Для асинхронных вебхуков
from dataclasses import dataclass

# =============================================================================
# ⚙️ НАСТРОЙКИ (Те же самые)
# =============================================================================
LEG1_THRESHOLD = 0.8        # Было 1.2. Снижаем, чтобы видеть больше движений.
LEG1_MIN_VOL_Z = 1.0        # Было 1.5. Меньше требований к аномалии объема.
MIN_VOLUME_USDT = 150000    # Было 150k. Даем шанс монетам поменьше.
ATR_MULTIPLIER = 1.5        # Импульс должен быть в 2.5 раза сильнее средней свечи

BIG_TRADE_THRESHOLD = 2000
MIN_BIG_TRADES = 1

MIN_DELTA = 0.65
TRAP_DELTA = 0.9
MIN_RATIO = 1.0

BTC_CRASH_PERCENT = 1.5
BTC_RANGE_PERCENT = 1.5
BTC_CHECK_INTERVAL = 60

WEBHOOK_LONG = "https://hook.finandy.com/OlD6seBrlUK"
SECRET_LONG = "fzq8p"
WEBHOOK_SHORT = "https://hook.finandy.com/70_ETEBrlUK"
SECRET_SHORT = "yp2ah"
DB_NAME = "pumphunter.db"

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO, datefmt='%H:%M:%S')
logger = logging.getLogger("BTC_Shield_Async")

# Инициализируем биржу внутри main, чтобы корректно работал event loop
exchange_config = {
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
}

@dataclass
class Setup:
    symbol: str
    trend_side: str
    entry_level: float
    start_time: float

active_trades = {}
blacklist_dynamic = {}  # {symbol: time_when_allowed_again}
memory_db = {}
price_history = {}
IS_TRADING_ALLOWED = True

# =============================================================================
# 🛡️ SAFETY & RISK (NEW)
# =============================================================================
STOP_LOSS_VIRTUAL = -3    # Если цена упала на 3% от входа -> БАН
BLACKLIST_DURATION = 21600  # На сколько секунд банить монету (6 часов)

# Монеты, которые мы НИКОГДА не торгуем (стейблы и мусор)
PERMANENT_BLACKLIST = [
    'USDC/USDT', 'FDUSD/USDT', 'TUSD/USDT', 'USDP/USDT', 
    'DAI/USDT', 'EURT/USDT', 'WBTC/USDT'
]

# =============================================================================
# 💾 DATABASE MANAGER (ASYNC)
# =============================================================================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                symbol TEXT PRIMARY KEY,
                entry REAL,
                side TEXT,
                start_time REAL,
                max_pnl REAL
            )
        """)
        await db.commit()
    logger.info("💾 Database initialized.")

async def save_trade_to_db(symbol, entry, side, start_time, max_pnl=0):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("""
                INSERT OR REPLACE INTO active_trades (symbol, entry, side, start_time, max_pnl)
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, entry, side, start_time, max_pnl))
            await db.commit()
    except Exception as e:
        logger.error(f"DB Save Error: {e}")

async def delete_trade_from_db(symbol):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("DELETE FROM active_trades WHERE symbol = ?", (symbol,))
            await db.commit()
    except Exception as e:
        logger.error(f"DB Delete Error: {e}")

async def load_active_trades():
    trades = {}
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT symbol, entry, side, start_time, max_pnl FROM active_trades") as cursor:
                async for row in cursor:
                    trades[row[0]] = {
                        'entry': row[1],
                        'side': row[2],
                        'time': row[3],
                        'max_pnl': row[4]
                    }
    except Exception as e:
        logger.error(f"DB Load Error: {e}")
    return trades

# =============================================================================
# 🛡️ ASYNC BTC MODULE
# =============================================================================
async def check_btc_status(exchange):
    global IS_TRADING_ALLOWED
    try:
        # await перед вызовом API
        candles = await exchange.fetch_ohlcv('BTC/USDT', timeframe='15m', limit=16)
        if not candles: return IS_TRADING_ALLOWED

        df = pd.DataFrame(candles, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        
        last_2 = df.tail(2)
        short_high = last_2['h'].max()
        short_low = last_2['l'].min()
        short_volatility = ((short_high - short_low) / short_low) * 100
        
        if short_volatility >= BTC_CRASH_PERCENT:
            if IS_TRADING_ALLOWED:
                logger.warning(f"⛔ BTC STORM DETECTED! Volatility {short_volatility:.2f}% (30min). PAUSING BOT.")
            IS_TRADING_ALLOWED = False
            return False

        long_high = df['h'].max()
        long_low = df['l'].min()
        long_volatility = ((long_high - long_low) / long_low) * 100

        if long_volatility <= BTC_RANGE_PERCENT:
            if not IS_TRADING_ALLOWED:
                logger.info(f"✅ BTC STABLE. Range {long_volatility:.2f}% (4h). RESUMING HUNT.")
            IS_TRADING_ALLOWED = True
            return True
        else:
            if not IS_TRADING_ALLOWED:
                logger.info(f"⏳ BTC cooling down... Vol {long_volatility:.2f}%. Waiting.")
            return IS_TRADING_ALLOWED

    except Exception as e:
        logger.error(f"BTC Check Fail: {e}")
        return IS_TRADING_ALLOWED

# =============================================================================
# 🚀 ASYNC UTILS
# =============================================================================

async def send_webhook(symbol, side, price, url, secret, msg):
    """Асинхронная отправка вебхука через aiohttp"""
    try:
        clean_s = symbol.split(':')[0].replace('/', '')
        payload = {
            "secret": secret, "symbol": clean_s, 
            "side": side, "signal": side,
            "price": price, "comment": msg
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=5) as resp:
                pass # Просто отправляем и забываем
        logger.info(f"🚀 SIGNAL SENT: {clean_s} {side} | {msg}")
    except Exception as e:
        logger.error(f"Webhook error: {e}")

async def filter_mature_coins(exchange, all_tickers, min_age_days=90):
    """
    Параллельная проверка возраста монет с использованием Семафора (чтобы не убить API).
    """
    logger.info(f"⏳ [FILTER] Асинхронная проверка возраста ({len(all_tickers)} монет)...")
    mature_coins = []
    cutoff_time = int((time.time() - (min_age_days * 24 * 60 * 60)) * 1000)
    
    # Ограничиваем количество одновременных запросов (например, 20)
    sem = asyncio.Semaphore(20) 

    async def check_single_coin(symbol):
        async with sem:
            try:
                # Берем самую старую свечу в пределах окна
                ohlcv = await exchange.fetch_ohlcv(symbol, '1d', since=cutoff_time, limit=1)
                if ohlcv and len(ohlcv) > 0:
                    first_candle_time = ohlcv[0][0]
                    if first_candle_time <= cutoff_time + (5 * 24 * 60 * 60 * 1000):
                        return symbol
            except:
                pass
        return None

    # Создаем задачи для всех монет
    tasks = [check_single_coin(s) for s in all_tickers]
    # Запускаем их параллельно и ждем результатов
    results = await asyncio.gather(*tasks)
    
    # Фильтруем None
    mature_coins = [r for r in results if r is not None]
    
    logger.info(f"✅ [FILTER] Готово! {len(mature_coins)} монет отобрано.")
    return mature_coins

# =============================================================================
# 🧠 CORE ANALYSIS FUNCTIONS
# =============================================================================

async def analyze_book(exchange, symbol, side):
    try:
        ob = await exchange.fetch_order_book(symbol, limit=20)
        b_vol = sum([b[1] for b in ob['bids']])
        a_vol = sum([a[1] for a in ob['asks']])
        
        if b_vol == 0 or a_vol == 0: return False, 0

        ratio = (b_vol / a_vol) if side == 'buy' else (a_vol / b_vol)
        
        if ratio >= MIN_RATIO: 
            return True, ratio
        return False, ratio
    except: return False, 0

def get_dynamic_whale_threshold(volume_24h):
    if volume_24h > 50_000_000: return 20000
    if volume_24h > 10_000_000: return 5000
    if volume_24h > 1_000_000: return 3000
    return 1500

async def verify_order_flow(exchange, symbol, side, current_price, entry_price, whale_threshold):
    try:
        trades = await exchange.fetch_trades(symbol, limit=80) # Берем последние 80 трейдов
        if not trades: return False, None, ""
        
        df = pd.DataFrame(trades, columns=['side', 'cost', 'price'])
        
        buy_vol = df[df['side'] == 'buy']['cost'].sum()
        total_vol = df['cost'].sum()
        if total_vol == 0: return False, None, ""
        
        delta_buy = buy_vol / total_vol
        my_delta = delta_buy if side == 'buy' else (1.0 - delta_buy)
        whales = len(df[df['cost'] >= whale_threshold])
        
        # Определяем, где сейчас цена относительно последних сделок (0.0 = Low, 1.0 = High)
        local_high = df['price'].max()
        local_low = df['price'].min()
        
        if local_high != local_low:
            pos = (current_price - local_low) / (local_high - local_low)
        else:
            pos = 1.0

        # --- 🛡️ STRICT LOGIC (ЖЕСТКИЕ ФИЛЬТРЫ) ---
        
        # 1. ОТСЕЧЕНИЕ ХАЕВ/ЛОЕВ (Самое важное!)
        # Если мы хотим купить, но цена уже на самом пике (выше 0.85) -> ОТМЕНА. Это фитиль.
        if side == 'buy' and pos > 0.85: return False, None, ""
        # Если мы хотим шортить, но цена на самом дне (ниже 0.15) -> ОТМЕНА.
        if side == 'sell' and pos < 0.15: return False, None, ""

        # 2. Ловля разворотов (TRAP)
        trap_delta_val = (1.0 - my_delta)
        is_trap = False
        # Ловушка для шортистов (цена растет, хотя дельта красная)
        if side == 'buy' and trap_delta_val > TRAP_DELTA and pos > 0.6: is_trap = True
        # Ловушка для лонгистов (цена падает, хотя дельта зеленая)
        if side == 'sell' and trap_delta_val > TRAP_DELTA and pos < 0.4: is_trap = True
        
        if is_trap and whales >= 1:
             return True, 'reversal', f"TRAP D:{trap_delta_val:.2f} Pos:{pos:.2f}"

        # 3. Вход по тренду (Только обоснованный!)
        # Требуем: Дельта > 0.60 (агрессия) И наличие Китов И Цена не на хаях
        if my_delta >= 0.60 and whales >= 1:
            
            # LONG: Цена должна быть в верхней половине (сила), но не на пике
            if side == 'buy' and 0.40 <= pos <= 0.85:
                 return True, 'trend', f"VALID_ENTRY D:{my_delta:.2f} Pos:{pos:.2f}"
            
            # SHORT: Цена в нижней половине (слабость), но не на дне
            if side == 'sell' and 0.15 <= pos <= 0.60:
                 return True, 'trend', f"VALID_ENTRY D:{my_delta:.2f} Pos:{pos:.2f}"

        return False, None, ""

    except Exception as e:
        return False, None, ""

# =============================================================================
# 🔄 MAIN LOOP LOGIC
# =============================================================================

async def check_market_state(exchange, symbol, price, volume_usd):
    current_time = time.time()
    
    # --- 0. SAFETY CHECKS (NEW) ---
    if symbol in PERMANENT_BLACKLIST: return 0
    
    if symbol in blacklist_dynamic:
        if current_time < blacklist_dynamic[symbol]:
            return 0 # Монета в бане, пропускаем
        else:
            del blacklist_dynamic[symbol] # Бан истек
    
    # Clean old trades
    if symbol in active_trades:
        trade = active_trades[symbol]
        # Если прошло 30 минут (1800 сек)
        if current_time - trade['time'] > 1800: 
            del active_trades[symbol]
            await delete_trade_from_db(symbol)  # <--- УДАЛЯЕМ ИЗ БД

    # --- 2. SETUP VALIDATION ---
    if symbol in memory_db:
        if not IS_TRADING_ALLOWED: return 0

        setup = memory_db[symbol]
        if current_time - setup.start_time > 1200: del memory_db[symbol]; return 0
        
        # Parallel fetch is possible here if needed, but sequential is fine for logical check
        is_ob, ratio = await analyze_book(exchange, symbol, setup.trend_side)
        if is_ob:
            dynamic_threshold = get_dynamic_whale_threshold(volume_usd)
            is_of, kind, msg = await verify_order_flow(exchange, symbol, setup.trend_side, price, setup.entry_level, dynamic_threshold)
            
            if is_of:
                final_side = setup.trend_side if kind == 'trend' else ('sell' if setup.trend_side == 'buy' else 'buy')
                final_msg = f"TREND_{msg}" if kind == 'trend' else f"REVERSE_{msg}"
                
                target_url = WEBHOOK_LONG if final_side == 'buy' else WEBHOOK_SHORT
                target_secret = SECRET_LONG if final_side == 'buy' else SECRET_SHORT
                
                # Async Webhook
                asyncio.create_task(send_webhook(symbol, final_side, price, target_url, target_secret, final_msg))
                
                # 1. Записываем в память
                active_trades[symbol] = {'entry': price, 'side': final_side, 'max_pnl': 0, 'time': current_time}
                
                # 2. 🔥 ВСТАВЛЯЕМ СОХРАНЕНИЕ В БД СЮДА 🔥
                await save_trade_to_db(symbol, price, final_side, current_time, 0)
                
                # 3. Удаляем из списка ожидания
                del memory_db[symbol]
        return 0

    # --- 3. IMPULSE SEARCH (Fast Logic) ---
    if symbol not in price_history: price_history[symbol] = [{'p': price, 't': current_time}]; return 0
    price_history[symbol].append({'p': price, 't': current_time})
    price_history[symbol] = [x for x in price_history[symbol] if x['t'] > current_time - 120]
    
    if len(price_history[symbol]) < 5: return 0
    
    start_p = price_history[symbol][0]['p']
    max_p = max([x['p'] for x in price_history[symbol]])
    min_p = min([x['p'] for x in price_history[symbol]])
    change = ((price - start_p) / start_p) * 100
    
    # Anti-FOMO
    if change > 0 and (max_p - price) / max_p * 100 > 0.3: return 0 
    if change < 0 and (price - min_p) / min_p * 100 > 0.3: return 0

    if abs(change) >= LEG1_THRESHOLD:
        try:
            # Запрашиваем свечи для анализа объема И волатильности
            bars = await exchange.fetch_ohlcv(symbol, timeframe='1m', limit=20)
            if not bars or len(bars) < 15: return 0

            df = pd.DataFrame(bars, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            # 1. Z-Score Объема (как и было)
            z = (df['v'].iloc[-1] - df['v'].mean()) / df['v'].std() if df['v'].std() > 0 else 0
            
            # 2. НОВОЕ: Расчет ATR (Среднего размера свечи за 15 мин)
            # Считаем размер тела свечи в % для последних 15 свечей (кроме текущей памповой)
            df['candle_size_pct'] = (df['h'] - df['l']) / df['o'] * 100
            # Берем среднее за предыдущие 15 минут
            avg_volatility = df['candle_size_pct'].iloc[-16:-1].mean()
            
            # Защита от деления на ноль (если монета спала)
            if avg_volatility < 0.1: avg_volatility = 0.1

            # --- ГЛАВНАЯ ПРОВЕРКА ---
            # Импульс должен быть больше порога (1.2%) И выше средней волатильности * множитель
            is_anomaly = abs(change) >= (avg_volatility * ATR_MULTIPLIER)

            if z >= LEG1_MIN_VOL_Z and is_anomaly:
                status_icon = "✅" if IS_TRADING_ALLOWED else "⛔"
                
                # Добавил вывод ATR в лог, чтобы ты видел, почему бот взял монету
                logger.info(f"{status_icon} [SETUP] {symbol} {change:+.2f}% | Z:{z:.1f} | ATR_x:{abs(change)/avg_volatility:.1f}")
                
                memory_db[symbol] = Setup(symbol, 'buy' if change > 0 else 'sell', price, current_time)
                price_history[symbol] = []
                
        except Exception as e:
            logger.error(f"Error checking {symbol}: {e}")
            pass
            
    return change

async def main():
    logger.info("============== 🦅 ASYNC HUNTER V2.5 (PRO) ЗАПУЩЕН ==============")
    
    # Инициализация биржи
    exchange = ccxt.okx(exchange_config)
    
    # 1. ИНИЦИАЛИЗАЦИЯ БД
    await init_db()
    
    # Восстанавливаем активные сделки после перезагрузки
    global active_trades
    loaded_trades = await load_active_trades()
    active_trades.update(loaded_trades)
    
    if len(active_trades) > 0:
        logger.info(f"♻️ Восстановлено {len(active_trades)} сделок из памяти.")

    try:
        # Проверка BTC при старте
        await check_btc_status(exchange)
        
        # 2. ПОЛУЧЕНИЕ И ФИЛЬТРАЦИЯ МОНЕТ
        markets = await exchange.load_markets()
        all_usdt = [symbol for symbol in markets if symbol.endswith('/USDT')]
        
        logger.info(f"🔎 Фильтр возраста для {len(all_usdt)} монет...")
        try:
            target_coins_list = await filter_mature_coins(exchange, all_usdt, min_age_days=90)
        except Exception as e:
            logger.error(f"⚠️ Ошибка фильтра: {e}")
            target_coins_list = []

        # Защита: если фильтр сломался, берем все
        if len(target_coins_list) == 0:
            logger.warning("⚠️ Фильтр вернул 0. Работаем со всем рынком.")
            target_coins_set = set(all_usdt)
        else:
            logger.info(f"✅ Фильтр пройден: {len(target_coins_list)} монет в работе.")
            target_coins_set = set(target_coins_list)

        last_h = 0
        last_btc_check = 0

        # --- ГЛАВНЫЙ ЦИКЛ ОХОТЫ ---
        while True:
            current_time = time.time()
            
            # Чек битка
            if current_time - last_btc_check > BTC_CHECK_INTERVAL:
                await check_btc_status(exchange)
                last_btc_check = current_time

            try:
                tickers_data = await exchange.fetch_tickers()
            except Exception as e:
                logger.error(f"API Error: {e}")
                await asyncio.sleep(5)
                continue
            
            scanned = 0
            
            for s, d in tickers_data.items():
                # 1. ЧИСТИМ ТИКЕР (убираем :USDT хвост для сверки со списком)
                clean_name = s.split(':')[0]
                
                if clean_name not in target_coins_set: continue
                
                p = d.get('last')
                # Считаем объем
                v = d.get('quoteVolume') 
                if v is None or v == 0:
                    base_vol = d.get('baseVolume')
                    if base_vol and p: v = base_vol * p
                    else: v = 0
                
                # 2. ПРОВЕРКА ОБЪЕМА И ЗАПУСК АНАЛИЗА
                if p and v and v > MIN_VOLUME_USDT: 
                    # Используем полный тикер 's' для торговли
                    await check_market_state(exchange, s, p, v)
                    scanned += 1
            
            # ОТЧЕТ (PULSE)
            if current_time - last_h > 10:
                btc_text = "🟢" if IS_TRADING_ALLOWED else "🔴"
                logger.info(f"💓 {btc_text} | Scan: {scanned} coins | Setups: {len(memory_db)}")
                last_h = current_time

                # МОНИТОРИНГ СДЕЛОК
                if active_trades:
                    logger.info("📈 ACTIVE TRADES MONITOR:")
                    for sym in list(active_trades.keys()):
                        trade_data = active_trades[sym]
                        ticker = tickers_data.get(sym)
                        
                        if ticker:
                            curr_price = ticker['last']
                            entry_price = trade_data['entry']
                            side = trade_data['side']
                            
                            if side == 'buy': pnl = ((curr_price - entry_price) / entry_price) * 100
                            else: pnl = ((entry_price - curr_price) / entry_price) * 100
                            
                            if pnl > trade_data['max_pnl']: 
                                active_trades[sym]['max_pnl'] = pnl
                            
                            # STOP LOSS CHECK
                            if pnl <= STOP_LOSS_VIRTUAL:
                                logger.warning(f"💀 STOP LOSS: {sym} ({pnl:.2f}%). BANNED 6H.")
                                blacklist_dynamic[sym] = current_time + BLACKLIST_DURATION
                                del active_trades[sym]
                                await delete_trade_from_db(sym)
                                continue

                            icon = "🟢" if pnl > 0 else "🔻"
                            logger.info(f"   {icon} {sym} ({side}) | PNL: {pnl:+.2f}% | Max: {trade_data['max_pnl']:+.2f}%")
            
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        await exchange.close()
        logger.info("Exchange closed.")

if __name__ == "__main__":
    asyncio.run(main())
