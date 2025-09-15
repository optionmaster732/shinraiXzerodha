import os
import pandas as pd
import numpy as np
import logging
import time
from kiteconnect import KiteConnect
import datetime
from datetime import datetime as dt
from pytz import timezone
from collections import deque

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('algo_trading.log'),
        logging.StreamHandler()
    ]
)

# Constants
API_KEY = "8oi1wfon3rk6cy3z"
API_SECRET = "5c59406amyvts8uklc9ucef37zvfewsb"
INDIA_TZ = timezone('Asia/Kolkata')
EXCEL_PATH = r"C:\Users\LG\Desktop\shinraiXzerodha\excel file\stockmanagerV3byGPT.xlsx"
BANKNIFTY_TOKEN = 260105

# Initialize KiteConnect
kite = KiteConnect(api_key=API_KEY)

# Track last 10 trade signals
trade_signals = deque(maxlen=10)

# Desired symbol order
SYMBOL_ORDER = [
    "RELIANCE", "NIFTY 50", "NIFTY BANK", "TATASTEEL", "SBIN",
    "DLF", "MARUTI", "JINDALSTEL", "TATAMOTORS"
]

# Cache for option instruments
option_instruments = {}

def authenticate():
    """Authenticate with Zerodha API"""
    try:
        if os.path.exists("access_token.txt"):
            with open("access_token.txt", "r") as f:
                access_token = f.read().strip()
                kite.set_access_token(access_token)
                kite.profile()
                print("API: Access token loaded successfully")
                logging.info("Access token loaded successfully")
                return True
        else:
            print("No access token found. Generating new one.")
            logging.info("No access token found. Generating new one.")
            print("Please visit this URL to login:", kite.login_url())
            request_token = input("Enter request token from URL after login: ").strip()
            data = kite.generate_session(request_token, api_secret=API_SECRET)
            access_token = data["access_token"]
            with open("access_token.txt", "w") as f:
                f.write(access_token)
            kite.set_access_token(access_token)
            print("API: Access token generated and saved")
            logging.info("Access token generated and saved")
            return True
    except Exception as e:
        print(f"API: Authentication failed - {e}")
        logging.error(f"Authentication failed: {e}")
        return False

def fetch_option_instruments():
    """Fetch and cache option instruments from Zerodha"""
    global option_instruments
    try:
        if not option_instruments:
            instruments = kite.instruments(exchange="NFO")
            today = dt.now(INDIA_TZ).date()
            for inst in instruments:
                if inst['instrument_type'] in ['CE', 'PE']:
                    symbol = inst['name'].upper()
                    if symbol == "NIFTY":
                        symbol = "NIFTY 50"
                    elif symbol == "BANKNIFTY":
                        symbol = "NIFTY BANK"
                    if symbol in SYMBOL_ORDER:
                        expiry_date = inst['expiry']
                        expiry_str = expiry_date.strftime('%y%b').upper()
                        if symbol not in option_instruments:
                            option_instruments[symbol] = {}
                        if expiry_str not in option_instruments[symbol]:
                            option_instruments[symbol][expiry_str] = []
                        option_instruments[symbol][expiry_str].append({
                            'tradingsymbol': inst['tradingsymbol'],
                            'strike': inst['strike'],
                            'option_type': inst['instrument_type'],
                            'expiry_date': expiry_date
                        })
            logging.info("Option instruments cached successfully")
        return option_instruments
    except Exception as e:
        logging.error(f"Error fetching option instruments: {e}")
        return {}

def get_instrument_token(symbol):
    """Get instrument token for a symbol"""
    try:
        if symbol == "NIFTY 50":
            return 256265
        elif symbol == "NIFTY BANK":
            return 260105
        else:
            ltp_data = kite.ltp([f"NSE:{symbol}"])
            return ltp_data[f"NSE:{symbol}"]["instrument_token"]
    except Exception as e:
        logging.error(f"Error getting instrument token for {symbol}: {e}")
        return None

def read_excel(file_path):
    """Read and validate Excel input"""
    try:
        if not os.path.exists(file_path):
            print("Error: Excel file not found")
            logging.error("Excel file not found")
            return None
        
        df = pd.read_excel(file_path)
        
        if df.empty:
            print("Error: Excel file is empty")
            logging.error("Excel file is empty")
            return None
            
        df.columns = df.columns.str.strip()
        
        required_cols = ['Stock', 'Expiry', 'EMA Short', 'EMA Long', 'Time Frame',
                        'Do you have positions?', 'Order Type', 'Strike Price Gap', 
                        'ATM Offset', 'Lot Size', 'Algo Status']
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Error: Missing required columns - {missing_cols}")
            logging.error(f"Missing columns: {missing_cols}")
            return None
            
        df['Algo Status'] = df['Algo Status'].astype(str)
        
        def format_expiry(row):
            expiry_date = pd.to_datetime(row['Expiry'])
            return expiry_date.strftime('%y%b').upper()
        
        df['Expiry'] = df.apply(format_expiry, axis=1)
        df['Stock'] = df['Stock'].replace({"NIFTY 50": "NIFTY50", "NIFTY BANK": "NIFTYBANK"})
        
        print("Excel: File loaded successfully")
        logging.info("Excel file loaded successfully")
        return df
        
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        logging.error(f"Error reading Excel file: {e}")
        return None

def get_historical_data(symbol, interval, max_ema_period, current_price=None):
    """Get clean historical data"""
    try:
        instrument_token = get_instrument_token(symbol)
        if not instrument_token:
            return pd.DataFrame()

        candles_needed = max_ema_period * 4
        market_hours_per_day = 6.5
        candles_per_day = (60 / 15) * market_hours_per_day
        days_needed = max(60, int(np.ceil(candles_needed / candles_per_day)))
        
        to_date = dt.now(INDIA_TZ)
        from_date = to_date - datetime.timedelta(days=days_needed)
        
        raw_data = kite.historical_data(
            instrument_token,
            from_date - datetime.timedelta(minutes=30),
            to_date,
            '15minute'
        )
        
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        df['date'] = pd.to_datetime(df['date']).dt.tz_convert(INDIA_TZ)
        df.set_index('date', inplace=True)
        
        is_index = symbol in ["NIFTY 50", "NIFTY BANK"]
        if not is_index:
            df = df[df['volume'] > 0]
        
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum' if not is_index else 'last'
        }
        
        df = df.resample('15min').apply(ohlc_dict).dropna()
        
        if current_price is not None:
            current_time = dt.now(INDIA_TZ)
            last_candle_time = df.index[-1]
            next_candle_start = last_candle_time + datetime.timedelta(minutes=15)
            if current_time >= next_candle_start:
                new_row = pd.DataFrame({
                    'open': [df['close'].iloc[-1]],
                    'high': [max(df['close'].iloc[-1], current_price)],
                    'low': [min(df['close'].iloc[-1], current_price)],
                    'close': [current_price],
                    'volume': [0]
                }, index=[next_candle_start])
                df = pd.concat([df, new_row])
            else:
                df.loc[last_candle_time, 'close'] = current_price
                df.loc[last_candle_time, 'high'] = max(df.loc[last_candle_time, 'high'], current_price)
                df.loc[last_candle_time, 'low'] = min(df.loc[last_candle_time, 'low'], current_price)
        
        return df[['open', 'high', 'low', 'close', 'volume']]
        
    except Exception as e:
        logging.error(f"Historical data error for {symbol}: {e}")
        return pd.DataFrame()

def calculate_ema(series, period):
    """Calculate EMA with validation"""
    if len(series) < period:
        return np.nan
    return series.ewm(span=period, adjust=False, min_periods=period).mean().iloc[-1]

def get_ltp(symbol):
    """Get last traded price"""
    try:
        if symbol == "NIFTY50":
            return kite.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"]
        elif symbol == "NIFTYBANK":
            return kite.ltp("NSE:NIFTY BANK")["NSE:NIFTY BANK"]["last_price"]
        else:
            return kite.ltp(f"NSE:{symbol}")[f"NSE:{symbol}"]["last_price"]
    except Exception as e:
        logging.error(f"Error getting LTP for {symbol}: {e}")
        return None

def get_closest_expiry(symbol):
    """Fallback to find the closest future expiry"""
    try:
        today = dt.now(INDIA_TZ).date()
        actual_symbol = "NIFTY 50" if symbol == "NIFTY50" else "NIFTY BANK" if symbol == "NIFTYBANK" else symbol
        expiries = option_instruments.get(actual_symbol, {})
        
        if not expiries:
            return None
        
        expiry_dates = [(exp, dt.strptime(exp, '%y%b').date()) 
                        for exp in expiries.keys()]
        future_expiries = [(exp_str, exp_date) for exp_str, exp_date in expiry_dates if exp_date >= today]
        
        if not future_expiries:
            return None
        
        return min(future_expiries, key=lambda x: x[1])[0]
    except Exception as e:
        logging.error(f"Error finding expiry for {symbol}: {e}")
        return None

def get_option_ltp(symbol, strike, option_type, expiry_from_excel):
    """Get LTP of an option"""
    try:
        actual_symbol = "NIFTY 50" if symbol == "NIFTY50" else "NIFTY BANK" if symbol == "NIFTYBANK" else symbol
        expiry = expiry_from_excel
        
        instruments = option_instruments.get(actual_symbol, {}).get(expiry, [])
        if not instruments:
            expiry = get_closest_expiry(symbol)
            if not expiry:
                return None
            instruments = option_instruments.get(actual_symbol, {}).get(expiry, [])
            if not instruments:
                return None
        
        valid_strikes = [inst['strike'] for inst in instruments if inst['option_type'] == option_type]
        if not valid_strikes:
            return None
        
        closest_strike = min(valid_strikes, key=lambda x: abs(x - strike))
        
        if symbol == "NIFTY50":
            option_symbol = f"NFO:NIFTY{expiry}{int(closest_strike)}{option_type}"
        elif symbol == "NIFTYBANK":
            option_symbol = f"NFO:BANKNIFTY{expiry}{int(closest_strike)}{option_type}"
        else:
            option_symbol = f"NFO:{symbol}{expiry}{int(closest_strike)}{option_type}"
        
        quote_data = kite.quote(option_symbol)
        if not quote_data or option_symbol not in quote_data:
            return None
        return quote_data[option_symbol]["last_price"]
    except Exception as e:
        logging.error(f"Error getting option LTP for {symbol} {strike}{option_type}: {e}")
        return None

def get_current_positions():
    """Check existing positions from Kite"""
    try:
        positions = kite.positions()
        return {pos['tradingsymbol']: pos for pos in positions['net']}
    except Exception as e:
        logging.error(f"Position check failed: {e}")
        return {}

def calculate_strike_price(spot, strike_gap, offset, option_type):
    """Calculate option strike price based on ATM offset"""
    atm_strike = round(spot / strike_gap) * strike_gap
    if option_type == "CE":
        return int(atm_strike - offset * strike_gap)  # Negative offset = OTM, Positive = ITM
    else:  # PE
        return int(atm_strike + offset * strike_gap)  # Negative offset = OTM, Positive = ITM

def get_last_order(symbol, expiry, strike_gap, atm_offset, ema_short_period, ema_long_period):
    """Backtest to find the last order based on EMA crossover"""
    try:
        hist_data = get_historical_data(
            "NIFTY 50" if symbol == "NIFTY50" else "NIFTY BANK" if symbol == "NIFTYBANK" else symbol,
            "15minute",
            max(ema_short_period, ema_long_period)
        )
        if hist_data.empty:
            logging.warning(f"No historical data for {symbol}")
            return None, None, None
        
        hist_data['EMA_Short'] = hist_data['close'].ewm(span=ema_short_period, adjust=False, min_periods=ema_short_period).mean()
        hist_data['EMA_Long'] = hist_data['close'].ewm(span=ema_long_period, adjust=False, min_periods=ema_long_period).mean()
        hist_data['State'] = hist_data['EMA_Short'] > hist_data['EMA_Long']
        hist_data['Prev_State'] = hist_data['State'].shift(1)
        hist_data['Crossover'] = (hist_data['State'] != hist_data['Prev_State']) & hist_data['Prev_State'].notna()
        
        crossover_events = hist_data[hist_data['Crossover']].tail(1)
        if crossover_events.empty:
            logging.warning(f"No crossover events for {symbol}")
            return None, None, None
        
        last_event = crossover_events.iloc[-1]
        spot = last_event['close']
        option_type = "CE" if last_event['State'] else "PE"
        strike = calculate_strike_price(spot, strike_gap, atm_offset, option_type)
        
        if not isinstance(strike, (int, float)) or strike <= 0:
            logging.error(f"Invalid strike calculated for {symbol}: {strike}")
            return None, None, None
        
        option_symbol = (f"NIFTY{expiry}{int(strike)}{option_type}" if symbol == "NIFTY50" else
                         f"BANKNIFTY{expiry}{int(strike)}{option_type}" if symbol == "NIFTYBANK" else
                         f"{symbol}{expiry}{int(strike)}{option_type}")
        
        logging.info(f"Last order for {symbol}: {option_symbol} at {last_event.name}")
        return option_symbol, last_event.name, option_type
    except Exception as e:
        logging.error(f"Error backtesting last order for {symbol}: {e}")
        return None, None, None

def place_option_order(symbol, expiry, strike, option_type, lot_size, order_type, transaction_type="BUY", option_ltp=None):
    """Execute option order"""
    try:
        if symbol == "NIFTY50":
            option_symbol = f"NIFTY{expiry}{strike}{option_type}"
        elif symbol == "NIFTYBANK":
            option_symbol = f"BANKNIFTY{expiry}{strike}{option_type}"
        else:
            option_symbol = f"{symbol}{expiry}{strike}{option_type}"
        
        if option_ltp is None:
            option_ltp = get_option_ltp(symbol, strike, option_type, expiry)
        
        if option_ltp is None:
            return False, "Failed to get option LTP"
        
        if lot_size <= 0:
            return False, f"Invalid lot size: {lot_size}"
        
        order_params = {
            "variety": kite.VARIETY_REGULAR,
            "tradingsymbol": option_symbol,
            "exchange": "NFO",
            "transaction_type": transaction_type,
            "quantity": int(lot_size),
            "order_type": order_type.upper(),
            "price": round(option_ltp, 1) if order_type.upper() == "LIMIT" else None,
            "product": "NRML",
            "validity": "DAY"
        }
        
        order_id = kite.place_order(**order_params)
        
        time.sleep(1)
        order_history = kite.order_history(order_id)
        latest_status = order_history[-1]['status'].upper()
        executed = latest_status == "COMPLETE"
        
        if executed:
            logging.info(f"Order placed: {option_symbol}, LTP={option_ltp}, Qty={int(lot_size)}, Type={transaction_type}")
            return True, "Order executed successfully"
        else:
            reason = order_history[-1].get('status_message', 'Unknown failure')
            return False, f"Order status: {latest_status} - {reason}"
    except Exception as e:
        logging.error(f"Order failed for {option_symbol}: {e}")
        return False, f"Exception: {str(e)}"

def is_candle_closing_time():
    """Check if current time is near 15-minute candle close"""
    now = dt.now(INDIA_TZ)
    return now.minute % 15 == 14 and now.second >= 30

def is_next_candle_opening():
    """Check if current time is the opening of the next 15-minute candle"""
    now = dt.now(INDIA_TZ)
    return now.minute % 15 == 0 and now.second < 5

def display_market_data(symbols, df, positions, pending_trades):
    """Display market data with offset-adjusted strikes and system-relevant positions"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    current_time = dt.now(INDIA_TZ).strftime("%H:%M:%S")
    print(f"Last Refresh: {current_time}\n")
    
    for symbol in SYMBOL_ORDER:
        display_symbol = symbol.replace("NIFTY 50", "NIFTY50").replace("NIFTY BANK", "NIFTYBANK")
        if display_symbol not in symbols:
            continue
            
        try:
            ltp = get_ltp(display_symbol)
            if ltp is None:
                print(f"{display_symbol.ljust(10)} - Error getting data")
                continue
            
            row = df[df['Stock'] == display_symbol].iloc[0]
            hist_data = get_historical_data(symbol, row['Time Frame'], max(row['EMA Short'], row['EMA Long']), current_price=ltp)
            
            if not hist_data.empty:
                short_ema = calculate_ema(hist_data['close'], row['EMA Short'])
                long_ema = calculate_ema(hist_data['close'], row['EMA Long'])
                
                strike_gap = row['Strike Price Gap']
                atm_offset = row['ATM Offset']
                ce_strike = calculate_strike_price(ltp, strike_gap, atm_offset, "CE")
                pe_strike = calculate_strike_price(ltp, strike_gap, atm_offset, "PE")
                
                ce_ltp = get_option_ltp(display_symbol, ce_strike, "CE", row['Expiry'])
                pe_ltp = get_option_ltp(display_symbol, pe_strike, "PE", row['Expiry'])
                
                price_fmt = f"{ltp:>8.2f}" if ltp > 1000 else f"{ltp:>7.2f}"
                ema_fmt = "{:>8.2f}" if ltp > 1000 else "{:>7.2f}"
                opt_fmt = "{:>5.2f}" if ce_ltp is not None and pe_ltp is not None else "N/A"
                
                last_order_info = ""
                if row['Do you have positions?'].upper() == "YES":
                    last_symbol, last_time, last_type = get_last_order(
                        display_symbol, row['Expiry'], strike_gap, atm_offset,
                        row['EMA Short'], row['EMA Long']
                    )
                    if last_symbol:
                        try:
                            strike_from_symbol = int(last_symbol[-6:-2])
                            last_ltp = get_option_ltp(display_symbol, strike_from_symbol, last_type, row['Expiry'])
                            in_portfolio = last_symbol in positions and positions[last_symbol]['quantity'] != 0
                            last_order_info = f" | Last Order: {last_symbol}, LTP: {last_ltp:.2f if last_ltp is not None else 'N/A'}"
                            if in_portfolio:
                                last_order_info += f" (Entered: {last_time.strftime('%Y-%m-%d %H:%M')})"
                        except ValueError as e:
                            logging.error(f"Error parsing strike from {last_symbol}: {e}")
                            last_order_info = f" | Last Order: Invalid symbol {last_symbol}"
                
                print(f"{display_symbol.ljust(10)} - CMP: {price_fmt} | EMA_Short({row['EMA Short']}): {ema_fmt.format(short_ema)} | EMA_Long({row['EMA Long']}): {ema_fmt.format(long_ema)}{last_order_info}")
                print(f"{' '*11} {int(ce_strike)}CE: {opt_fmt.format(ce_ltp) if ce_ltp else 'N/A'} | {int(pe_strike)}PE: {opt_fmt.format(pe_ltp) if pe_ltp else 'N/A'}")
            
        except Exception as e:
            print(f"{display_symbol.ljust(10)} - Error processing data: {e}")
            logging.error(f"Error in display for {display_symbol}: {e}")
    
    has_positions = any(row['Do you have positions?'].upper() == "YES" for _, row in df.iterrows())
    if has_positions and positions:
        print("\nSystem Positions:")
        for symbol in SYMBOL_ORDER:
            display_symbol = symbol.replace("NIFTY 50", "NIFTY50").replace("NIFTY BANK", "NIFTYBANK")
            if display_symbol not in symbols:
                continue
            row = df[df['Stock'] == display_symbol].iloc[0]
            if row['Do you have positions?'].upper() == "YES":
                last_symbol, _, _ = get_last_order(
                    display_symbol, row['Expiry'], row['Strike Price Gap'], row['ATM Offset'],
                    row['EMA Short'], row['EMA Long']
                )
                if last_symbol and last_symbol in positions and positions[last_symbol]['quantity'] != 0:
                    pos = positions[last_symbol]
                    print(f"{last_symbol.ljust(20)} - Qty: {pos['quantity']}, Avg Price: {pos['average_price']:.2f}, LTP: {pos['last_price']:.2f}")
    
    if pending_trades:
        print("\nPending Trades:")
        for trade in pending_trades:
            print(f"{trade['symbol']} - {trade['option_symbol']} at {trade['trigger_time']} {'(Square-off)' if trade.get('is_square_off') else ''}")
    
    if trade_signals:
        print("\nLast 10 Orders:")
        for signal in reversed(trade_signals):
            status = "EXECUTED" if signal['executed'] else f"FAILED - {signal['failure_reason']}"
            print(f"{signal['symbol']} - {signal['option_symbol']} at {signal['spot_price']:.2f} (LTP: {signal['option_ltp']:.2f}) - {signal['transaction_type']} - {status}")

# Global state variables
prev_ema_states = {}
pending_trades = []

def initialize_ema_states(df, active_symbols):
    """Initialize prev_ema_states"""
    global prev_ema_states
    for symbol in active_symbols:
        row = df[df['Stock'] == symbol].iloc[0]
        hist_data = get_historical_data(
            "NIFTY 50" if symbol == "NIFTY50" else "NIFTY BANK" if symbol == "NIFTYBANK" else symbol,
            row['Time Frame'],
            max(row['EMA Short'], row['EMA Long'])
        )
        if not hist_data.empty:
            short_ema = calculate_ema(hist_data['close'], row['EMA Short'])
            long_ema = calculate_ema(hist_data['close'], row['EMA Long'])
            if not np.isnan(short_ema) and not np.isnan(long_ema):
                prev_ema_states[symbol] = short_ema > long_ema
                logging.info(f"Initialized {symbol}: EMA_Short={short_ema:.2f}, EMA_Long={long_ema:.2f}, Prev_State={prev_ema_states[symbol]}")
            else:
                prev_ema_states[symbol] = None
        else:
            prev_ema_states[symbol] = None

def monitor_and_trade(file_path):
    """Main trading loop"""
    global prev_ema_states, pending_trades
    try:
        if not authenticate():
            print("Authentication failed. Exiting.")
            return
        
        fetch_option_instruments()
        
        df = read_excel(file_path)
        if df is None:
            return
            
        active_symbols = df[df['Algo Status'].str.upper() == "ON"]['Stock'].unique()
        if not active_symbols.size:
            print("No active symbols found")
            return
            
        print(f"Monitoring {len(active_symbols)} active symbols\n")
        logging.info(f"Monitoring {len(active_symbols)} active symbols")
        
        initialize_ema_states(df, active_symbols)
        pending_trades = []
        
        while True:
            try:
                current_time = dt.now(INDIA_TZ)
                
                if current_time.second == 0 or is_candle_closing_time() or is_next_candle_opening():
                    positions = get_current_positions()
                    display_market_data(active_symbols, df, positions, pending_trades)
                
                if is_candle_closing_time():
                    live_prices = {symbol: get_ltp(symbol) for symbol in active_symbols if get_ltp(symbol) is not None}
                    
                    for _, row in df[df['Algo Status'].str.upper() == "ON"].iterrows():
                        symbol = row['Stock']
                        if symbol not in live_prices:
                            continue
                            
                        cmp = live_prices[symbol]
                        expiry = row['Expiry']
                        strike_gap = row['Strike Price Gap']
                        atm_offset = row['ATM Offset']
                        lot_size = row['Lot Size']
                        order_type = row['Order Type']
                        has_positions = row['Do you have positions?'].upper() == "YES"
                        
                        hist_data = get_historical_data(
                            "NIFTY 50" if symbol == "NIFTY50" else "NIFTY BANK" if symbol == "NIFTYBANK" else symbol,
                            row['Time Frame'],
                            max(row['EMA Short'], row['EMA Long']),
                            current_price=cmp
                        )
                        
                        if hist_data.empty:
                            continue
                            
                        short_ema = calculate_ema(hist_data['close'], row['EMA Short'])
                        long_ema = calculate_ema(hist_data['close'], row['EMA Long'])
                        
                        if np.isnan(short_ema) or np.isnan(long_ema):
                            continue
                            
                        current_state = short_ema > long_ema
                        prev_state = prev_ema_states.get(symbol)
                        
                        logging.info(f"{symbol}: EMA_Short={short_ema:.2f}, EMA_Long={long_ema:.2f}, Prev_State={prev_state}, Current_State={current_state}, CMP={cmp:.2f}")
                        
                        if prev_state is not None and current_state != prev_state:
                            option_type = "CE" if current_state else "PE"
                            strike = calculate_strike_price(cmp, strike_gap, atm_offset, option_type)
                            option_symbol = (f"NIFTY{expiry}{strike}{option_type}" if symbol == "NIFTY50" else
                                            f"BANKNIFTY{expiry}{strike}{option_type}" if symbol == "NIFTYBANK" else
                                            f"{symbol}{expiry}{strike}{option_type}")
                            confirmation = (current_state and cmp > long_ema) or (not current_state and cmp < long_ema)
                            
                            if confirmation:
                                option_ltp = get_option_ltp(symbol, strike, option_type, expiry)
                                
                                if has_positions:
                                    last_symbol, _, last_type = get_last_order(
                                        symbol, expiry, strike_gap, atm_offset,
                                        row['EMA Short'], row['EMA Long']
                                    )
                                    if last_symbol and last_symbol in positions and positions[last_symbol]['quantity'] > 0:
                                        square_off_ltp = get_option_ltp(symbol, int(last_symbol[-6:-2]), last_type, expiry)
                                        pending_trades.append({
                                            'symbol': symbol,
                                            'expiry': expiry,
                                            'strike': int(last_symbol[-6:-2]),
                                            'option_type': last_type,
                                            'option_symbol': last_symbol,
                                            'spot_price': cmp,
                                            'option_ltp': square_off_ltp,
                                            'lot_size': lot_size,
                                            'order_type': order_type,
                                            'transaction_type': "SELL",
                                            'trigger_time': current_time.strftime("%H:%M:%S"),
                                            'is_square_off': True
                                        })
                                        logging.info(f"Pending square-off: {last_symbol}")
                                
                                pending_trades.append({
                                    'symbol': symbol,
                                    'expiry': expiry,
                                    'strike': strike,
                                    'option_type': option_type,
                                    'option_symbol': option_symbol,
                                    'spot_price': cmp,
                                    'option_ltp': option_ltp,
                                    'lot_size': lot_size,
                                    'order_type': order_type,
                                    'transaction_type': "BUY",
                                    'trigger_time': current_time.strftime("%H:%M:%S"),
                                    'is_square_off': False
                                })
                                logging.info(f"Pending trade: {option_symbol}")
                            else:
                                logging.info(f"No trade for {symbol}: Confirmation failed")
                        else:
                            logging.info(f"No trade for {symbol}: No crossover")
                        
                        prev_ema_states[symbol] = current_state
                        
                if is_next_candle_opening() and pending_trades:
                    for trade in pending_trades[:]:
                        order_success, reason = place_option_order(
                            trade['symbol'], trade['expiry'], trade['strike'], trade['option_type'],
                            trade['lot_size'], trade['order_type'], trade['transaction_type'], trade['option_ltp']
                        )
                        
                        signal = {
                            'symbol': trade['symbol'],
                            'expiry': trade['expiry'],
                            'time': current_time.strftime("%H:%M:%S"),
                            'spot_price': trade['spot_price'],
                            'strike': trade['strike'],
                            'option_type': trade['option_type'],
                            'option_symbol': trade['option_symbol'],
                            'option_ltp': trade['option_ltp'] if trade['option_ltp'] is not None else 0.0,
                            'transaction_type': trade['transaction_type'],
                            'executed': order_success,
                            'failure_reason': reason if not order_success else "N/A"
                        }
                        trade_signals.append(signal)
                        pending_trades.remove(trade)
                    
                time.sleep(1)
                
            except KeyboardInterrupt:
                print("\nStopped by user")
                break
            except Exception as e:
                logging.error(f"Main loop error: {e}")
                time.sleep(30)
                
    except Exception as e:
        print(f"Error in monitor_and_trade: {e}")
        logging.error(f"Error in monitor_and_trade: {e}")

if __name__ == "__main__":
    monitor_and_trade(EXCEL_PATH)