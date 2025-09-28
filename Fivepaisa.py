import requests
from tabulate import tabulate
import datetime
import pandas as pd
#from logger_module import logger
import logging
from io import StringIO

LOG = logging.getLogger(__name__)

def fivepaisa_get_balance(app_key, access_token, client_code):

    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V4/Margin"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"bearer {access_token}"
    }

    payload = {
        "head": {"key": app_key},
        "body": {"ClientCode": client_code}
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        balance_data = response.json()
        balance = {"Available Margin": balance_data['body']['EquityMargin'][0]['NetAvailableMargin']}
        return balance
    else:
        LOG.error(f"Error: {response.status_code} - {response.text}")
        return None

def fivepaisa_scripcode_fetch(name):

    # Load the instrument master from 5paisa
    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/nse_eq"
    instruments = pd.read_csv(url)

    result = instruments[
        (instruments["Exch"] == "N") &
        (instruments["Name"] == name)
    ]

    if not result.empty:
        scrip_code = result.iloc[0]["ScripCode"]
        LOG.info(f"ScripCode for {name} (NSE) is: {scrip_code}")
        return scrip_code
    else:
        print(f"No match found for {name} in NSE")

def fivepaisa_get_nearest_option(symbol_root, spot_value, option_type):
    # Load 5paisa instruments master
    # Filter base instruments
    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/nse_fo"
    instruments = pd.read_csv(url)

    df = instruments[
        (instruments["Exch"] == "N") &
        (instruments["SymbolRoot"] == symbol_root) &
        (instruments["ScripType"] == option_type)
    ].copy()

    if df.empty:
        return None

    # Parse expiry dates
    df["Expiry"] = pd.to_datetime(df["Expiry"], errors="coerce")

    # Sort by expiry
    df = df.sort_values("Expiry")

    today = datetime.datetime.now().date()
    tomorrow = today + datetime.timedelta(days=1)

    # Exclude today and tomorrow expiry
    df = df[~df["Expiry"].isin([pd.Timestamp(today), pd.Timestamp(tomorrow)])]

    if df.empty:
        return None

    # Choose nearest expiry (smallest future expiry)
    nearest_expiry = df["Expiry"].min()
    df = df[df["Expiry"] == nearest_expiry]

    # Find nearest strike to spot value
    df["StrikeDiff"] = abs(df["StrikeRate"] - spot_value)
    df = df.sort_values("StrikeDiff")

    # Pick the top row
    return df.iloc[[0]]

def fivepaisa_fetch_positions(app_key, access_token, client_code):

    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V2/NetPositionNetWise"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"bearer {access_token}"
    }

    payload = {
        "head": {"key": app_key},
        "body": {"ClientCode": client_code}
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        positions_data = response.json()
        net_positions = positions_data['body']['NetPositionDetail']
        return net_positions
    else:
        LOG.error(f"Error: {response.status_code} - {response.text}")
        return None

def fivepaisa_historical_data_fetch(access_token, scripCode, interval,days):
  end_date = datetime.datetime.today().strftime("%Y-%m-%d")
  from_date = (datetime.datetime.today()-datetime.timedelta(days = days)).strftime("%Y-%m-%d")
  if days == 25:
      exchange_type = "C"
  elif days == 1:
      exchange_type = "D"
  url = f"https://openapi.5paisa.com/V2/historical/N/{exchange_type}/{scripCode}/{interval}?from={from_date}&end={end_date}"

  headers = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token}"}
  resp = requests.get(url, headers=headers)

  if resp.status_code == 200:
    data = resp.json()
    if "data" in data and "candles" in data["data"]:
      candles = data["data"]["candles"]

      # Convert into DataFrame
      df = pd.DataFrame(candles, columns=["dateTime", "open", "high", "low", "close", "volume"])

      # Clean up timestamp & convert to datetime
      df["dateTime"] = pd.to_datetime(df["dateTime"].str.replace("T", " "))

      # Set DateTime as index
      df.set_index("dateTime", inplace=True)
      return df
    else:
      LOG.warning("❌ No candle data found in response.")
      return pd.DataFrame()
  else:
    LOG.error(f"❌ Error: {resp.status_code} - {resp.text}")
    return pd.DataFrame()

def fivepaisa_close_position(credentials, pos):
    access_token = credentials['access_token']
    user_key = credentials['app_key']
    quantity = pos['NetQty']
    scrip_data = pos['ScripName']
    scrip_code = pos['ScripCode']
    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V1/PlaceOrderRequest"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    payload = {
        "head": {"key": user_key},
        "body": {
            "OrderType": "S",              # "B" = Buy, "S" = Sell
            "Exchange": "N",               # N = NSE, B = BSE, M = MCX
            "ExchangeType": "D",           # C = Cash, D = Derivatives, U = Currency
            "ScripCode": scrip_code,        # Numeric ScripCode
            "ScripData": scrip_data,       # Symbol format
            "Price": 0,                # Limit Price
            "PriceType": "MKT",              # L = Limit, MKT = Market
            "StopLossPrice": 0,
            "Qty": quantity,
            "DisQty": 0,
            "IsIntraday": False,
            "IsStopLossOrder": False,
            "iOrderValidity": 0,           # 0 = Day, 1 = IOC
            "AHPlaced": "N"
        }
    }
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 200:
        LOG.info(resp.json())
    else:
        LOG.error("Error:", resp.status_code, resp.text)

def fivepaisa_place_single_order(access_token, scripCode, user_key, scrip_data, price, quantity, order_type):
    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V1/PlaceOrderRequest"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    payload = {
        "head": {"key": user_key},
        "body": {
            "OrderType": order_type,              # "B" = Buy, "S" = Sell
            "Exchange": "N",               # N = NSE, B = BSE, M = MCX
            "ExchangeType": "D",           # C = Cash, D = Derivatives, U = Currency
            "ScripCode": scripCode,        # Numeric ScripCode
            "ScripData": scrip_data,       # Symbol format
            "Price": price,                # Limit Price
            "PriceType": "L",              # L = Limit, MKT = Market
            "StopLossPrice": 0,
            "Qty": quantity,
            "DisQty": 0,
            "IsIntraday": False,
            "IsStopLossOrder": False,
            "iOrderValidity": 0,           # 0 = Day, 1 = IOC
            "AHPlaced": "N"
        }
    }

    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 200:
        LOG.info(resp.json())
    else:
        LOG.error("Error:", resp.status_code, resp.text)

def fivepaisa_place_bracket_order(access_token, scripCode, user_key, scrip_data, price, quantity, order_type, target):
    url = "https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V1/PlaceOrderRequest"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    payload = {
        "head": {"key": user_key},
        "body": {
            "OrderType": order_type,              # "B" = Buy, "S" = Sell
            "Exchange": "N",               # N = NSE, B = BSE, M = MCX
            "ExchangeType": "D",           # C = Cash, D = Derivatives, U = Currency
            "ScripCode": str(scripCode),        # Numeric ScripCode
            "ScripData": str(scrip_data),       # Symbol format
            "Price": float(price),                # Limit Price
            "PriceType": "L",              # L = Limit, MKT = Market
            "StopLossPrice": 0,
            "TargetPrice": float(target),  # ✅ Target
            #"TrailingSL": 5,  # Optional
            "Qty": int(quantity),
            "DisQty": 0,
            "IsIntraday": True,
            "IsStopLossOrder": False,
            "iOrderValidity": 0,           # 0 = Day, 1 = IOC
            "AHPlaced": "N"
        }
    }

    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 200:
        LOG.info(resp.json())
    else:
        LOG.error("Error:", resp.status_code, resp.text)

def format_option_name(option_str: str) -> str:
    parts = option_str.split()
    # Example: ['HDFCBANK', '30', 'SEP', '2025', 'PE', '970.00']
    symbol = parts[0]
    day = parts[1]
    month = parts[2].upper()
    year = parts[3]
    option_type = parts[4]
    strike = parts[5].split('.')[0]  # remove decimals

    # Convert "30 SEP 2025" → "20250930"
    expiry_date = datetime.datetime.strptime(f"{day} {month} {year}", "%d %b %Y")
    expiry_str = expiry_date.strftime("%Y%m%d")

    return f"{symbol}_{expiry_str}_{option_type}_{strike}"

def fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots,option_type):

    nearest_option = fivepaisa_get_nearest_option(stock, close_price, option_type)
    scrip_code = nearest_option['ScripCode'].values[0]
    scripdata = nearest_option['Name'].values[0]
    scrip_data = format_option_name(scripdata)
    lot_size = int(nearest_option['LotSize'].values[0])
    tick_size = float(nearest_option['TickSize'].values[0])
    print(f"{scrip_code}--{scrip_data}--{lot_size}--{tick_size}")
    option_candle_data = fivepaisa_historical_data_fetch(access_token, scrip_code, 1,1)
    close_price = option_candle_data['close'].iat[-1]
    target = round(((int(close_price) * (100+int(tgt)))/100)/tick_size)*tick_size
    quantity = int(lots)*lot_size
    fivepaisa_place_bracket_order(access_token, scrip_code, user_key, scrip_data, close_price, quantity, "B",target)


def fivepaisa_trade_conditions_check(lots, tgt, indicators_df, credentials, stock_details,strategy):

    stock= stock_details['symbol']
    access_token = credentials['access_token']
    user_key = credentials['app_key']
    client_code = credentials['client_id']
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

        positions1 = fivepaisa_fetch_positions(user_key, access_token, client_code)
        if positions1:
            for pos in positions1:
                quantity = pos['NetQty']
                if quantity > 0:
                    scrip_data = pos['ScripName']
                    scrip_code = pos['ScripCode']
                    parts = scrip_code.split("_")
                    option_type = parts[2]
                    price = pos['LTP']
                    if option_type == "CE" and ((latest_willr < -70 and latest_supertrend > close_price) or (
                            latest_willr < -70 and latest_macd < latest_macd_signal) or (
                                                        latest_supertrend > close_price and latest_macd < latest_macd_signal)):
                        fivepaisa_place_single_order(access_token, scrip_code, user_key, scrip_data, price, quantity, "S")
                    elif option_type == "PE" and ((latest_willr > -30 and latest_supertrend < close_price) or (
                            latest_willr > -30 and latest_macd > latest_macd_signal) or (
                                                          latest_supertrend < close_price and latest_macd < latest_macd_signal)):
                        fivepaisa_place_single_order(access_token, scrip_code, user_key, scrip_data, price, quantity, "S")

        positions = fivepaisa_fetch_positions(user_key, access_token, client_code)
        if latest_adx > latest_adxema and latest_willr > -30 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
            LOG.info("🔼 BUY SIGNAL GENERATED")
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            LOG.info(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots,"CE")
            else:
                fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "CE")

        elif latest_adx > latest_adxema and latest_willr < -70 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
            LOG.info("🔽 SELL SIGNAL GENERATED")
            if positions:
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            LOG.info(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                        fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots,"PE")
            else:
                fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "PE")
        else:
            LOG.info("⏸️ NO TRADE SIGNAL GENERATED")

    elif strategy == "Ema10_Ema20_Supertrend":
        # ✅ Check for signal
        latest_Ema10 = indicators_df["ema10"].iloc[-1]
        latest_Ema20 = indicators_df['ema20'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])
        tgt = float(tgt)

        positions1 = fivepaisa_fetch_positions(user_key, access_token, client_code)
        if positions1:
            for pos in positions1:
                quantity = pos['NetQty']
                if quantity > 0:
                    scrip_data = pos['ScripName']
                    scrip_code = pos['ScripCode']
                    parts = scrip_code.split("_")
                    option_type = parts[2]
                    price = pos['LTP']
                    if option_type == "CE" and (latest_Ema10 < latest_Ema20 or latest_supertrend > close_price):
                        fivepaisa_place_single_order(access_token, scrip_code, user_key, scrip_data, price, quantity, "S")
                    elif option_type == "PE" and (latest_Ema10 > latest_Ema20 or latest_supertrend < close_price):
                        fivepaisa_place_single_order(access_token, scrip_code, user_key, scrip_data, price, quantity, "S")

        positions = fivepaisa_fetch_positions(user_key, access_token, client_code)
        if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price:
            LOG.info("🔼 BUY SIGNAL GENERATED")
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            LOG.info(f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "CE")
            else:
                fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "CE")

        elif latest_Ema10 < latest_Ema20 or latest_supertrend > close_price:
            LOG.info("🔽 SELL SIGNAL GENERATED")
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            LOG.info(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "PE")
            else:
                fivepaisa_fetch_option_data(access_token, user_key, stock, close_price, tgt, lots, "PE")
        else:
            LOG.info("⏸️ NO TRADE SIGNAL GENERATED")
