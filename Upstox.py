import sys
import time
import gevent
from collections import deque
import requests
import pandas as pd
import datetime
import pytz
import Next_Now_intervals
from logger_module import logger

instruments = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")

def upstox_profile(access_token):
    url = 'https://api.upstox.com/v2/user/profile'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    try:
        response = requests.get(url, headers=headers)
        #print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            response_data = response.json()
            # Extract available_margin from equity section
            if response_data.get('status') == 'success' and 'data' in response_data:
                #profile = response_data['data']['equity']['available_margin']
                profile = {'User ID': response_data.get('data')['user_id'],
                           'User Name': response_data.get('data')['user_name'],
                           'Email':response_data.get('data')['email']}
                return profile
            else:
                logger.write("⚠️ Failed to retrieve balance: Invalid response structure")
                return None
        else:
            logger.write(f"🚨 API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logger.write(f"🚨 Exception in balance function: {e}")
    return None

def upstox_balance(access_token):
     print("DEBUG: entered upstox_balance, caller:", inspect.stack()[1].function)
    url = 'https://api.upstox.com/v2/user/get-funds-and-margin'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(url, headers=headers)
        #print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            response_data = response.json()
            # Extract available_margin from equity section
            if response_data.get('status') == 'success' and 'data' in response_data:
                #print(response_data['data']['equity'])
                total_balance = response_data['data']['equity']['available_margin'] + response_data['data']['equity']['used_margin']
                balance = {"Total Balance":total_balance, "Available Margin":response_data['data']['equity']['available_margin'],"Used Margin":response_data['data']['equity']['used_margin']}
                return balance
            else:
                logger.write("⚠️ Failed to retrieve balance: Invalid response structure")
                return None
        else:
            logger.write(f"🚨 API Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        logger.write(f"🚨 Exception in balance function: {e}")
        return None

def upstox_instrument_key(name):

    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce').dt.date
    indices = ['Nifty 50', 'Nifty Bank', 'Nifty Fin Service','NIFTY MID SELECT']

    if name in indices:
        instrument_type = "INDEX"
        exchange = "NSE_INDEX"
    else:
        instrument_type = "EQUITY"
        exchange = "NSE_EQ"
    filtered = instruments[
        (instruments['instrument_type'] == instrument_type) &
        (instruments['name'] == name) &
        (instruments['exchange'] == exchange)
        ]

    if filtered.empty:
        logger.write("❌ No matching option instrument found")
        return

    if not filtered.empty:
        instrument_key = filtered.iloc[0]['instrument_key']
        return instrument_key
    else:
        logger.write("❌ No matching option instrument found")
        return

def upstox_fetch_historical_data_with_retry(access_token, instrument_key, interval):
    """Fetches historical 30-minute OHLC data, retrying for previous days."""
    today = datetime.date.today()
    end_date = (today - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start = today - datetime.timedelta(days=25)
    start_date = start.strftime('%Y-%m-%d')

    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}/{end_date}/{start_date}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get('data', {})
        candles = data.get('candles')

        if candles:
            df = pd.DataFrame(candles, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi'])
            df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)
            df.sort_values('datetime', inplace=True)
            df.set_index('datetime', inplace=True)

            df.drop(['oi'], axis=1, inplace=True)

            df['5ema'] = df['close'].ewm(span=5, adjust=False).mean()
            logger.write(f"✅ Fetched historical data form: {start_date}")
            return df

        else:
            logger.write(f"⚠️ No data on {start_date} (market holiday or no trades). Trying earlier day...")
    else:
        logger.write(f"❌ Failed to fetch data for {start_date}. HTTP {response.status_code} and {response.json()}. Retrying...")

    logger.write(f"❗Could not fetch historical data for {instrument_key} from 25 days.")
    return pd.DataFrame()

def upstox_fetch_intraday_data(access_token, instrument_key, interval):
    now_interval, next_interval = Next_Now_intervals.round_to_next_interval(interval)
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/minutes/{interval}"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    max_wait_seconds = 30
    sleep_interval = 5
    waited = 0

    while waited <= max_wait_seconds:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                candles = response.json().get('data', {}).get('candles', [])
                if candles:
                    df = pd.DataFrame(candles, columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi'])
                    df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)
                    df.sort_values('datetime', inplace=True)
                    df.set_index('datetime', inplace=True)
                    df.drop(['volume', 'oi'], axis=1, inplace=True)

                    if df.index[-1] == now_interval:
                        completed_df = df[:-1]
                    # Filter to return only fully completed candles
                    completed_df = df[df.index.map(lambda x: x.second == 0 and x.microsecond == 0)]

                    if not completed_df.empty:
                        return completed_df
                    else:
                        logger.write(f"⏳ Waiting for complete candle data... Retry in {sleep_interval}s")
                else:
                    logger.write("⚠️ No candle data found in response.")
            else:
                logger.write(f"🚨 API Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.write(f"🚨 Exception in fetch_intraday_data: {e}")

        gevent.sleep(sleep_interval)
        waited += sleep_interval

    print("❌ Failed to fetch complete candle data within 30 seconds.")
    return None

def upstox_fetch_positions(access_token):
    """Fetch current open positions from Upstox API."""
    url = 'https://api.upstox.com/v2/portfolio/short-term-positions'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        positions = response.json().get('data', [])
        return positions
    logger.write(f"Failed to fetch positions: {response.text}")
    return []


def upstox_ohlc_data_fetch(access_token, instrument_key):
    retries = 3
    url = 'https://api.upstox.com/v3/market-quote/ohlc'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }

    params = {
        "instrument_key": instrument_key,
        "interval": "I1"
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                try:
                    json_key = instrument_key.replace('|', ':')
                    data = response.json()['data'][json_key]
                    prev = data['prev_ohlc']

                    ist = pytz.timezone("Asia/Kolkata")
                    prev_ts = datetime.datetime.fromtimestamp(prev['ts'] / 1000, tz=ist)

                    # Extract close price for EMA calculation
                    close_price = prev['close']

                    return {
                        "datetime": prev_ts,
                        "open": prev['open'],
                        "high": prev['high'],
                        "low": prev['low'],
                        "close": close_price,
                    }

                except KeyError as e:
                    logger.write(f"OHLC KeyError in response: {e}")
                    return None
            else:
                logger.write("OHLC Error:", response.status_code, response.text)
                gevent.sleep(2)
                return None
        except requests.exceptions.RequestException as e:
            logger.write(f"🔌 OHLC Network error (attempt {attempt}/{retries}): {e}")

        gevent.sleep(1)

def upstox_live_option_Value(access_token, instrument_key):
    url = 'https://api.upstox.com/v3/market-quote/ohlc'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    data = {
        "instrument_key": instrument_key,
        "interval": "1d"
    }

    response = requests.get(url, headers=headers, params=data)

    if response.status_code == 200:
        json_data = response.json()
        if 'data' in json_data:
            # Dynamically get the token key from the dict
            token = list(json_data['data'].keys())[0]
            instrument_data = json_data['data'].get(token, {})
            close_price = instrument_data.get('live_ohlc', {}).get('close', None)
            if close_price is not None:
                return close_price
            else:
                logger.write(f"Close price not available for {token}.")
        else:
            logger.write("No data field in response.")
    else:
        logger.write(f"Request failed with status code: {response.status_code}")

def upstox_close_position(credentials, pos):
    access_token = credentials['access_token']
    quantity = pos['quantity']
    instrument_token = pos['instrument_token']

    url = 'https://api-hft.upstox.com/v3/order/place'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {access_token}",
    }

    data = {
        'quantity': quantity,
        'product': 'D',
        'validity': 'DAY',
        'price': 0,
        'tag': 'string',
        'instrument_token': instrument_token,
        'order_type': "MARKET",
        'transaction_type': "SELL",
        'disclosed_quantity': 0,
        'trigger_price': 0,
        'is_amo': False,
        'slice': False
    }

    try:
        # Send the POST request
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 200:
            logger.write("Position closed successfully")
        else:
            logger.write(f"Order placed not successful. The response code is : {response.status_code}")
    except Exception as e:
        # Handle exceptions
        logger.write('Error:', str(e))

def upstox_place_order_single(access_token, instrument_token, quantity, transaction_type,price):

    quantity = abs(quantity)
    if price == 0:
        order_type = "MARKET"
    else:
        order_type = "LIMIT"


    url = 'https://api-hft.upstox.com/v3/order/place'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f"Bearer {access_token}",
    }

    data = {
        'quantity': quantity,
        'product': 'D',
        'validity': 'DAY',
        'price': price,
        'tag': 'string',
        'instrument_token': instrument_token,
        'order_type': order_type,
        'transaction_type': transaction_type,
        'disclosed_quantity': 0,
        'trigger_price': price,
        'is_amo': False,
        'slice': False
    }

    try:
        # Send the POST request
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 200:
            if transaction_type == "BUY":
                logger.write("order placed successfully")
            elif transaction_type == "SELL":
                logger.write("Old option position closed successfully")
        else:
            logger.write(f"Order placed not successful. The response code is : {response.status_code}")


    except Exception as e:
        # Handle exceptions
        logger.write('Error:', str(e))

def upstox_gtt_place_order(access_token, instrument_key, quantity, transaction_type, entry,tgt):
    try:
        url = "https://api.upstox.com/v3/order/gtt/place"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "MULTIPLE",
            "quantity": quantity,
            "product": "D",
            "instrument_token": instrument_key,
            "transaction_type": transaction_type,
            "rules": [
                {
                    "strategy": "ENTRY",
                    "trigger_type": "BELOW",
                    "trigger_price": entry
                },
                {
                    "strategy": "STOPLOSS",
                    "trigger_type": "IMMEDIATE",
                    "trigger_price": 0.5
                }
            ]
        }
        if tgt > 0:
            payload["rules"].append({
                "strategy": "TARGET",
                "trigger_type": "IMMEDIATE",
                "trigger_price": tgt
            })
        res = requests.post(url, headers=headers, json=payload)
        if res.status_code == 200:
            logger.write("✅ GTT order placed successfully.")
            return res.status_code
        else:
            logger.writelogger.write(f"❌ GTT order placement failed: {res.text}")
    except Exception as e:
        logger.write(f"❌ Error placing GTT order: {e}")

def upstox_fetch_option_data(upstox_access_token,stock, spot_value, tgt,lots, option_type):
    # Fetch instruments
    logger.write(f"{stock}--{spot_value}--{tgt}--{lots}--{option_type}")
    indices = {"NIFTY": "Nifty 50", "BANKNIFTY": "Nifty Bank", "FINNIFTY": "Nifty Fin Service","MIDCPNIFTY": "NIFTY MID SELECT"}

    if stock in indices:
        stock = indices[stock]
        instrument_type = "OPTIDX"
    else:
        instrument_type = "OPTSTK"

    today = datetime.datetime.now().date()
    now_time = datetime.datetime.now().time()

    # Convert expiry column
    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce').dt.date

    # Filter relevant instruments
    filtered = instruments[
        (instruments['instrument_type'] == instrument_type) &
        (instruments['name'] == stock) &
        (instruments['expiry'] >= today) &
        (instruments['option_type'] == option_type)
        ]

    if filtered.empty:
        logger.write("❌ No matching option instrument found")
    else:
        filtered = filtered.copy()  # ✅ prevents slice warning
        filtered['strike'] = pd.to_numeric(filtered['strike'], errors='coerce')

        # All available expiries
        sorted_expiries = sorted(filtered['expiry'].unique())
        if not sorted_expiries:
            logger.write("❌ No expiry available")
        else:
            nearest_expiry = sorted_expiries[0]

            # 🚨 Expiry skip rule:
            if nearest_expiry == today:
                # If today is expiry day → always skip
                if len(sorted_expiries) > 1:
                    nearest_expiry = sorted_expiries[1]

            elif nearest_expiry == today + datetime.timedelta(days=1) and now_time >= datetime.time(15, 0):
                # If tomorrow is expiry → skip only after 15:00
                if len(sorted_expiries) > 1:
                    nearest_expiry = sorted_expiries[1]

            # Filter by selected expiry
            filtered = filtered[filtered['expiry'] == nearest_expiry].copy()

            # Find nearest strike
            filtered['strike_diff'] = abs(filtered['strike'] - spot_value)
            nearest_option = filtered.loc[filtered['strike_diff'].idxmin()]

            # Calculate total quantity (lots × lot size)
            lot_size = nearest_option['lot_size']
            instrument_key = nearest_option['instrument_key']
            strike = nearest_option['strike']
            option_tick_size = nearest_option['tick_size']

            option_buffer = deque(maxlen=500)
            ist = pytz.timezone('Asia/Kolkata')

            # Fetch latest intraday data
            option_intraday_data = upstox_fetch_intraday_data(upstox_access_token, instrument_key, 1)

            if option_intraday_data is None or option_intraday_data.empty or len(option_intraday_data) < 1:
                logger.write("⚠️ Insufficient intraday data for option (need at least 1 candles).")
                return

            # Process only the last two candles
            latest_candle = option_intraday_data.iloc[-1]

            # Add latest candle to option_buffer
            dt_aware = latest_candle.name if latest_candle.name.tzinfo else ist.localize(latest_candle.name)
            candle = {
                'datetime': dt_aware,
                'open': latest_candle['open'],
                'high': latest_candle['high'],
                'low': latest_candle['low'],
                'close': latest_candle['close'],
            }
            option_buffer.append(candle)

            logger.write("+---------------------+----------+----------+----------+----------+")
            logger.write("| Time                | Open     | High     | Low      | Close    |")
            logger.write("+---------------------+----------+----------+----------+----------+")

            for candle in [latest_candle]:
                dt_aware = candle.name if candle.name.tzinfo else ist.localize(candle.name)
                logger.write("| {:<19} | {:>8.2f} | {:>8.2f} | {:>8.2f} | {:>8.2f} |".format(
                    dt_aware.strftime('%Y-%m-%d %H:%M'),
                    candle['open'],
                    candle['high'],
                    candle['low'],
                    candle['close']
                ))

            logger.write("+---------------------+----------+----------+----------+----------+")

            close_price = float(latest_candle["close"])
            target = (close_price * (100+int(tgt)))/100
            target_price = round(round(target / option_tick_size) * option_tick_size, 2)
            buy_price = close_price
            logger.write(f"Strike Price is: {strike}  {option_type}  Entry: {buy_price},  Target : {target_price}")
            lots = int(lots)
            lot_size = int(lot_size)
            quantity = lots * lot_size

            positions = upstox_fetch_positions(upstox_access_token)
            if positions:
                count = 0
                for pos in positions:
                    quantity_old = pos['quantity']
                    symbol = pos['tradingsymbol']
                    option_type = symbol[-2:]

                    if quantity_old > 0 and (option_type == "PE" or option_type == "CE"):
                        logger.write(f"You have live position for the Trading symbol  {symbol}, Skipping the {option_type}Order placing")
                    else:
                        count += 1
                        if count == 1:
                            upstox_gtt_place_order(upstox_access_token, instrument_key, quantity, "BUY", buy_price,target_price)
            else:
                upstox_gtt_place_order(upstox_access_token, instrument_key, quantity, "BUY", buy_price,target_price)

def upstox_trade_conditions_check(lots, tgt, indicators_df, credentials, stock,strategy):
    upstox_access_token = credentials['access_token']
    if strategy == "ADX_MACD_WillR_Supertrend":
        # ✅ Check for signal
        latest_adx = indicators_df["ADX"].iloc[-1]
        latest_adxema = indicators_df['ADX_EMA21'].iloc[-1]
        latest_willr = indicators_df['WillR_14'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        latest_macd = indicators_df['MACD'].iloc[-1]
        latest_macd_signal = indicators_df['MACD_signal'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])
        tgt = float(tgt)

        positions1 = upstox_fetch_positions(upstox_access_token)
        if positions1:
            for pos in positions1:
                quantity = pos['quantity']
                if quantity > 0:
                    instrument_token = pos['instrument_token']
                    tradingsymbol = pos['tradingsymbol']
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and ((latest_willr < -70 and latest_supertrend > close_price) or (
                            latest_willr < -70 and latest_macd < latest_macd_signal) or (
                                                        latest_supertrend > close_price and latest_macd < latest_macd_signal)):
                        upstox_place_order_single(upstox_access_token, instrument_token, quantity, "SELL",close_price)
                    elif option_type == "PE" and ((latest_willr > -30 and latest_supertrend < close_price) or (
                            latest_willr > -30 and latest_macd > latest_macd_signal) or (
                                                          latest_supertrend < close_price and latest_macd < latest_macd_signal)):
                        upstox_place_order_single(upstox_access_token, instrument_token, quantity, "SELL",close_price)

        positions = upstox_fetch_positions(upstox_access_token)
        if latest_adx > latest_adxema and latest_willr > -30 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
            logger.write("🔼 BUY SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            logger.write(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots,"CE")
            else:
                upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "CE")

        elif latest_adx > latest_adxema and latest_willr < -70 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
            logger.write("🔽 SELL SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            logger.write(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                        upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots,"PE")
            else:
                upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "PE")
        else:
            logger.write("⏸️ NO TRADE SIGNAL GENERATED")
            sys.stdout.flush()

    elif strategy == "Ema10_Ema20_Supertrend":
        # ✅ Check for signal
        latest_Ema10 = indicators_df["ema10"].iloc[-1]
        latest_Ema20 = indicators_df['ema20'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])
        tgt = float(tgt)

        positions1 = upstox_fetch_positions(upstox_access_token)
        if positions1:
            for pos in positions1:
                quantity = pos['quantity']
                if quantity > 0:
                    instrument_token = pos['instrument_token']
                    tradingsymbol = pos['tradingsymbol']
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and (latest_Ema10 < latest_Ema20 or latest_supertrend > close_price):
                        upstox_place_order_single(upstox_access_token, instrument_token, quantity, "SELL", close_price)
                    elif option_type == "PE" and (latest_Ema10 > latest_Ema20 or latest_supertrend < close_price):
                        upstox_place_order_single(upstox_access_token, instrument_token, quantity, "SELL", close_price)

        positions = upstox_fetch_positions(upstox_access_token)
        if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price:
            logger.write("🔼BUY SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            logger.write(f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "CE")
            else:
                upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "CE")

        elif latest_Ema10 < latest_Ema20 or latest_supertrend > close_price:
            logger.write("🔽 SELL SIGNAL GENERATED")
            sys.stdout.flush()
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            logger.write(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "PE")
            else:
                upstox_fetch_option_data(upstox_access_token, stock, close_price, tgt, lots, "PE")
        else:
            logger.write("⏸️NO TRADE SIGNAL GENERATED")
            sys.stdout.flush()
