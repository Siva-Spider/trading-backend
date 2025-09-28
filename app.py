import gevent
from gevent import monkey
monkey.patch_all()

from flask import Flask, request, jsonify, Response
import threading
from flask_cors import CORS
#from logger_module import logger
import logging
from logging.handlers import RotatingFileHandler
import os
import datetime
import time
import pytz
from tabulate import tabulate
from kiteconnect import KiteConnect
import traceback

# --- 1. Custom Log Storage and Handler (REPLACEMENT for logger_module) ---
class WebLogHandler(logging.Handler):
    """A custom logging handler that stores log records in memory for web streaming."""
    def __init__(self, log_list):
        super().__init__()
        self.log_list = log_list
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    def emit(self, record):
        """Formats and appends the log message to the shared list."""
        self.log_list.append(self.format(record))

# --- Central Log List and Setup ---
# This list holds all logs to be streamed to the frontend
SHARED_LOGS = []

# Configure the root logger
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# Add the custom handler to the root logger
web_handler = WebLogHandler(SHARED_LOGS)
LOG.addHandler(web_handler)

# Optional: Add a file handler for persistence
file_handler = RotatingFileHandler('app.log', maxBytes=1024*1024*10, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
LOG.addHandler(file_handler)

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return {"status": "Backend is running 🚀"}

broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

# --- Stock Map ---
stock_map = {
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "HDFC BANK LTD": "HDFCBANK",
    "ICICI BANK LTD.": "ICICIBANK",
    "INFOSYS LIMITED": "INFY",
    "TATA CONSULTANCY SERV LT": "TCS",
    "STATE BANK OF INDIA": "SBIN",
    "AXIS BANK LTD": "AXISBANK",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "ITC LTD": "ITC",
    "LARSEN & TOUBRO LTD.": "LT",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "HINDUSTAN UNILEVER LTD": "HINDUNILVR",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA",
    "MARUTI SUZUKI INDIA LTD": "MARUTI",
    "NTPC LTD": "NTPC",
    "HCL TECHNOLOGIES LTD": "HCLTECH",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "TITAN COMPANY LIMITED": "TITAN",
    "BHARAT ELECTRONICS LTD": "BEL",
    "POWER GRID CORP. LTD": "POWERGRID",
    "TATA STEEL LIMITED": "TATASTEEL",
    "TRENT LTD": "TRENT",
    "ASIAN PAINTS LIMITED": "ASIANPAINT",
    "JIO FIN SERVICES LTD": "JIOFIN",
    "BAJAJ FINSERV LTD": "BAJAJFINSV",
    "GRASIM INDUSTRIES LTD": "GRASIM",
    "ADANI PORT & SEZ LTD": "ADANIPORTS",
    "JSW STEEL LIMITED": "JSWSTEEL",
    "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "OIL AND NATURAL GAS CORP": "ONGC",
    "TECH MAHINDRA LIMITED": "TECHM",
    "BAJAJ AUTO LIMITED": "BAJAJ-AUTO",
    "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN",
    "CIPLA LTD": "CIPLA",
    "COAL INDIA LTD": "COALINDIA",
    "SBI LIFE INSURANCE CO LTD": "SBILIFE",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "NESTLE INDIA LIMITED": "NESTLEIND",
    "DR. REDDY S LABORATORIES": "DRREDDY",
    "APOLLO HOSPITALS ENTER. L": "APOLLOHOSP",
    "EICHER MOTORS LTD": "EICHERMOT",
    "WIPRO LTD": "WIPRO",
    "TATA CONSUMER PRODUCT LTD": "TATACONSUM",
    "ADANI ENTERPRISES LIMITED": "ADANIENT",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO",
    "INDUSIND BANK LIMITED": "INDUSINDBK",
    "Nifty 50": "NIFTY",
    "Nifty Bank": "BANKNIFTY",
    "Nifty Fin Service": "FINNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
}

# Reverse map to get full company name from symbol
reverse_stock_map = {v: k for k, v in stock_map.items()}
# In-memory storage for logs + active trade status
trade_logs = []
active_trades = {}   # { "NIFTY": True, "RELIANCE": False }
broker_sessions = {}

@app.route("/api/stream-logs")
def stream_logs():
    def event_stream():
        last_index = 0
        while True:
            # Reference the new SHARED_LOGS list
            if len(SHARED_LOGS) > last_index:
                new_logs = SHARED_LOGS[last_index:]
                for log in new_logs:
                    # SSE format: data: [message]\n\n
                    yield f"data: {log}\n\n"
                last_index += len(new_logs)
            
            gevent.sleep(1) # Use gevent.sleep to yield control

    return Response(event_stream(), mimetype="text/event-stream")

# === CONNECT BROKER ===
@app.route('/api/connect-broker', methods=['POST'])
def connect_broker():
    data = request.get_json()
    #brokers_data = data.get('brokers', [])
    selected_brokers = data.get("brokers", [])
    responses = []
    for broker_item in selected_brokers:
        broker_key = broker_item.get('name')
        creds = broker_item.get('credentials')
        broker_name = broker_map.get(broker_key)

        profile = None
        balance = None
        message = "Broker not supported or credentials missing."
        status = "failed"

        try:
            if broker_name == "Upstox":
                import Upstox as us
                print(broker_name)
                access_token = creds.get('access_token')
                profile = us.upstox_profile(access_token)
                balance = us.upstox_balance(access_token)
                print(profile, balance)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your access token."

            elif broker_name == "Zerodha":
                import Zerodha as zr
                api_key = creds.get('api_key')
                access_token = creds.get('access_token')
                profile = zr.zerodha_get_profile(api_key, access_token)
                balance = zr.zerodha_get_equity_balance(api_key, access_token)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your API key and access token."

            elif broker_name == "AngelOne":
                import AngelOne as ar
                print(broker_name)
                api_key = creds.get('api_key')
                user_id = creds.get('user_id')
                pin = creds.get('pin')
                totp_secret = creds.get('totp_secret')
                obj, refresh_token, auth_token,feed_token = ar.angelone_connect(api_key, user_id, pin, totp_secret)
                profile, balance = ar.angelone_fetch_profile_and_balance(obj, refresh_token)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                    broker_sessions[broker_name] = {
                        "obj": obj,
                        "refresh_token": refresh_token,
                        "auth_token": auth_token,
                        "feed_token": feed_token
                    }
                else:
                    message = "Connection failed. Check your credentials."
            elif broker_name == "5paisa":
                import Fivepaisa as fp
                app_key = creds.get('app_key')
                access_token = creds.get('access_token')
                client_code = creds.get("client_id")
                #profile = "5Paisa don't have the Profile Fetch fecility"
                profile = {'User Name':client_code,}
                balance = fp.fivepaisa_get_balance(app_key, access_token, client_code)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your API key and access token."
            elif broker_name == "Groww":
                import Groww as gr
                api_key = creds.get('api_key')
                access_token = creds.get('access_token')
                if api_key and access_token:
                    profile = {"User Name": f"Dummy {broker_name} User"}
                    balance = {"Available Margin": "10000.00"}
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Missing API key or access token."
        except Exception as e:
            status = "failed"
            message = f"An error occurred: {str(e)}"

        responses.append({
            "broker": broker_name,
            "broker_key": broker_key,
            "status": status,
            "message": message,
            "profileData": {
                "profile": profile,
                "balance": balance,
                "status": status,
                "message": message
            }
        })
    return jsonify(responses)

# === LOT SIZE ===
@app.route('/api/get-lot-size', methods=['GET'])
def get_lot_size():
    import get_lot_size as ls
    symbol = request.args.get('symbol')
    print(symbol)
    if not symbol:
        return jsonify({"error": "Stock symbol is required."}), 400

    lot_size = ls.lot_size(symbol)

    if lot_size:
        return jsonify({"lot_size": lot_size, "symbol": symbol})
    else:
        return jsonify({"message": "Lot size not found for the given symbol."}), 404

def find_positions_for_symbol(broker, symbol, credentials):
    """
    Fetch positions for the given broker and return only those matching the symbol.
    """
    positions = []

    try:
        # --- Upstox ---
        if broker.lower() == "upstox":
            access_token = credentials.get("access_token")
            positions = us.upstox_fetch_positions(access_token)


        # --- Zerodha ---
        elif broker.lower() == "zerodha":
            api_key = credentials.get("api_key")
            access_token = credentials.get("access_token")
            kite = KiteConnect(api_key)
            kite.set_access_token(access_token)
            positions_data = kite.positions()
            positions = positions_data.get("net", [])

        # --- AngelOne ---
        elif broker.lower() == "angelone":
            api_key = credentials.get("api_key")
            user_id = credentials.get("user_id")
            pin = credentials.get("pin")
            totp_secret = credentials.get("totp_secret")
            session = broker_sessions.get(broker)
            if not session:
                return jsonify({"status": "failed", "message": "Broker not connected."})
            auth_token = session["auth_token"]
            positions = ar.angeeone_fetch_positions(api_key, auth_token)
        # --- 5 Paisa ---
        elif broker.lower() == "5paisa":
            app_key = credentials.get("app_key")
            access_token = credentials.get('access_token')
            client_code = credentials.get("client_id")
            positions = fp.fivepaisa_fetch_positions(app_key, access_token, client_code)

        # --- Groww (dummy example here) ---
        elif broker.lower() == "groww":
            # Assume you have functions like gr.groww_positions(), fp.fivepaisa_positions()
            positions = []  # placeholder

        # --- Filter matching positions ---
        matching = []
        for pos in positions:
            trading_symbol = pos.get("tradingsymbol", "")
            if trading_symbol.startswith(symbol):  # match beginning
                matching.append(pos)

        return matching

    except Exception as e:
        print(f"❌ Error fetching positions for {broker}, {symbol}: {e}")
        return []

# === TRADING LOOP FOR ALL STOCKS ===
def run_trading_logic_for_all(trading_parameters, selected_brokers):
    IST_TIMEZONE = pytz.timezone('Asia/Kolkata')
    try:
        import Upstox as us
        import Zerodha as zr
        import AngelOne as ar
        import Groww as gr
        import Fivepaisa as fp
        import Next_Now_intervals as nni
        import combinding_dataframes as cdf
        import indicators as ind
        if not trading_parameters:
            LOG.warning("❌ Trading loop aborted: Received empty trading parameters list.")
            return 
            
        # mark all as active initially
        for stock in trading_parameters:
            active_trades[stock['symbol']] = True
        LOG.info("✅ Trading loop started for all selected stocks")
        LOG.info("\n⏳ Starting new trading cycle setup...")
        LOG.info(active_trades)
        # STEP 1: Fetch instrument keys once at the beginning
        for stock in trading_parameters:
            if not active_trades.get(stock['symbol']):
                continue
    
            broker_key = stock.get('broker')
            broker_name = broker_map.get(broker_key)
            symbol = stock.get('symbol')
            strategy = stock.get('strategy')
            company = reverse_stock_map.get(symbol, " ")
            interval = stock.get('interval')
    
            LOG.info(f"🔑 Fetching instrument key for {company} ({symbol}) via {broker_name}...")
            instrument_key = None
    
            if broker_name.lower() == "upstox":
                instrument_key = us.upstox_instrument_key(company)

            elif broker_name.lower() == "zerodha":
                broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                if broker_info:
                    api_key = broker_info['credentials'].get("api_key")
                    access_token = broker_info['credentials'].get("access_token")
                    instrument_key = zr.zerodha_instruments_token(api_key, access_token, symbol)
            elif broker_name.lower() == "angelone":
               LOG.info(company)
               instrument_key = ar.angelone_get_token_by_name(symbol)
            elif broker_name.lower() == "5paisa":
               instrument_key = fp.fivepaisa_scripcode_fetch(symbol)

            if instrument_key:
                stock['instrument_key'] = instrument_key
                LOG.info(f"✅ Found instrument key {instrument_key} for {symbol}")
                gevent.sleep(1)
            else:
                LOG.warning(f"⚠️ No instrument key found for {symbol}, skipping this stock.")
                active_trades[stock['symbol']] = False
                
        # setup time intervals
        interval = trading_parameters[0].get("interval", "1minute")
        now_interval, next_interval = nni.round_to_next_interval(interval)
        LOG.info(f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}")
        # loop until all stocks disconnected
        while any(active_trades.values()):
            gevent.sleep(2)
            now_ist = datetime.datetime.now(IST_TIMEZONE)
            now = now_ist.strftime("%Y-%m-%d %H:%M:%S")
            if now >= next_interval:
                interval = trading_parameters[0].get("interval", "1minute")
                now_interval, next_interval = nni.round_to_next_interval(interval)
                
                # STEP 2: Fetch data + indicators
                for stock in trading_parameters:
                    symbol = stock.get('symbol')
                    if symbol not in active_trades:  # <-- skip if removed
                        continue
    
                    broker_key = stock.get('broker')
                    broker_name = broker_map.get(broker_key)
                    company = reverse_stock_map.get(symbol, " ")
                    interval = stock.get('interval')
                    instrument_key = stock.get('instrument_key')
    
                    LOG.info(f"🕯 Fetching candles for {symbol}-{company} from {broker_name}")
    
                    combined_df = None
                    try:
                        if broker_name.lower() == "upstox":
                            access_token = next((b['credentials']['access_token'] for b in selected_brokers if b['name'] == broker_key),
                                None
                            )
                            if access_token:
                                hdf = us.upstox_fetch_historical_data_with_retry(access_token, instrument_key, interval)
                                idf = us.upstox_fetch_intraday_data(access_token, instrument_key, interval)
                                if hdf is not None and idf is not None:
                                    combined_df = cdf.combinding_dataframes(hdf, idf)
    
                        elif broker_name.lower() == "zerodha":
                            broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                            if broker_info:
                                kite = KiteConnect(broker_info['credentials'].get("api_key"))
                                kite.set_access_token(broker_info['credentials'].get("access_token"))
                                if interval == "1":
                                    interval = ""
                                hdf = zr.zerodha_historical_data(kite, instrument_key, interval)
                                idf = zr.zerodha_intraday_data(kite, instrument_key, interval)
                                if hdf is not None and idf is not None:
                                    combined_df = cdf.combinding_dataframes(hdf, idf)
                        elif broker_name.lower() == "angelone":
                            broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                            if broker_info:
                                api_key = broker_info['credentials'].get("api_key")
                                user_id = broker_info['credentials'].get("user_id")
                                pin = broker_info['credentials'].get("pin")
                                totp_secret = broker_info['credentials'].get("totp_secret")
                                session = broker_sessions.get(broker_name)
                                if not session:
                                    return jsonify({"status": "failed", "message": "Broker not connected."})
                                obj = session["obj"]
                                auth_token = session["auth_token"]
                                interval = ar.number_to_interval(interval)
                                combined_df = ar.angelone_get_historical_data(api_key,auth_token, obj,"NSE", instrument_key, interval)
                        elif broker_name.lower() == "5paisa":
                            broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                            if broker_info:
                                app_key = broker_info['credentials'].get("app_key")
                                access_token = broker_info['credentials'].get("access_token")
                                combined_df = fp.fivepaisa_historical_data_fetch(access_token, instrument_key, interval,25)
    
                    except Exception as e:
                        LOG.error(f"❌ Error fetching data for {symbol}: {e}")
    
                    if combined_df is None or combined_df.empty:
                        LOG.error(f"❌ No data for {symbol}, skipping.")
                        continue
    
                    LOG.info(f"✅ Data ready for {symbol}")
                    gevent.sleep(0.5)
                    indicators_df = ind.all_indicators(combined_df)
                    row = indicators_df.tail(1).iloc[0]
                    cols = indicators_df.columns.tolist()
                    col_widths = [max(len(str(c)), len(str(row[c]))) + 2 for c in cols]
                    def line():
                        return "+" + "+".join(["-" * w for w in col_widths]) + "+"
    
                    header = "|" + "|".join([f"{c:^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                    values = "|" + "|".join([f"{str(row[c]):^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                    # --- log it line by line ---
                    LOG.info(line())
                    LOG.info(header)
                    LOG.info(line())
                    LOG.info(values)
                    LOG.info(line())
                    #LOG.info(tabulate(indicators_df.tail(1), headers="keys", tablefmt="pretty", showindex=False))
    
                    # STEP 3: Check trade conditions
                    LOG.info(f"📊 Checking trade conditions for {symbol}")
                    lots = stock.get("lots")
                    target_pct = stock.get("target_percentage")
                    name = stock.get("symbol")
    
                    try:
                        creds = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)
                        if broker_name.lower() == "upstox":
                            us.upstox_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, company,strategy)
                        elif broker_name.lower() == "zerodha":
                            zr.zerodha_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, symbol,strategy)
                        elif broker_name.lower() == "angelone":
                            session = broker_sessions.get(broker_name)
                            if not session:
                                return jsonify({"status": "failed", "message": "Broker not connected."})
                            obj = session["obj"]
                            auth_token = session["auth_token"]
                            interval = ar.number_to_interval(interval)
                            ar.angelone_trade_conditions_check(obj, auth_token, lots, target_pct, indicators_df, creds, name,strategy)
                        elif broker_name.lower() == "5paisa":
                            fp.fivepaisa_trade_conditions_check(lots, target_pct, indicators_df, creds, stock,strategy)
    
                    except Exception as e:
                        LOG.error(f"❌ Error running strategy for {symbol}: {e}")
    
                LOG.info("✅ Trading cycle complete")
                gevent.sleep(1)  # wait before next cycle
    except Exception as e:
        # 🚨 CRITICAL: Log the full traceback if a crash occurs
        error_msg = f"🚨 FATAL CRASH in background trading thread: {e}\n"
        full_trace = traceback.format_exc()
        
        # Use the LOG (or logger) to write the full trace
        LOG.error(error_msg + full_trace) # Assuming you are using the logging setup from snippet 2

# === START ALL TRADING ===
@app.route('/api/start-all-trading', methods=['POST'])
def start_all_trading():
    data = request.get_json()
    trading_parameters = data.get("tradingParameters", [])
    selected_brokers = data.get("selectedBrokers", [])

    # Run in background thread
    thread = threading.Thread(
        target=run_trading_logic_for_all,
        args=(trading_parameters, selected_brokers),
        daemon=True
    )
    thread.start()

    return jsonify({"logs": ["🟢 Started trading for all stocks together"]})

@app.route("/api/close-position", methods=["POST"])
def close_position():
    data = request.json
    symbol = data.get("symbol")
    broker = data.get("broker")
    credentials = data.get("credentials")
    session = broker_sessions.get("AngelOne")
    if not session:
        return jsonify({"status": "failed", "message": "Broker not connected."})
    obj = session["obj"]

    closed = []
    matches = find_positions_for_symbol(broker, symbol, credentials)

    for pos in matches:
        order_id = None

        if broker.lower() == "upstox":
            order_id = us.upstox_close_position(credentials, pos)

        elif broker.lower() == "zerodha":
            order_id = zr.zerodha_close_position(credentials, pos)

        elif broker.lower() == "angelone":
            order_id = ar.angelone_close_position(obj, pos)

        elif broker.lower() == "groww":
            order_id = gr.groww_close_position(credentials, pos)

        elif broker.lower() == "5paisa":
            order_id = fp.fivepaisa_close_position(credentials, pos)

        if order_id:
            closed.append({"symbol": symbol, "broker": broker, "order_id": order_id})

    if closed:
        return jsonify({"message": f"✅ Closed position for {symbol}", "closed": closed})
    else:
        return jsonify({"message": f"⚠️ No open position found for {symbol}"}), 404

@app.route("/api/close-all-positions", methods=["POST"])
def close_all_positions():
    data = request.json
    trading_parameters = data.get("tradingParameters", [])  # list of active stocks
    selected_brokers = data.get("selectedBrokers", [])      # broker credentials
    session = broker_sessions.get("AngelOne")
    if not session:
        return jsonify({"status": "failed", "message": "Broker not connected."})
    obj = session["obj"]

    closed = []

    for stock in trading_parameters:
        symbol = stock.get("symbol")
        broker_key = stock.get("broker")
        broker_name = broker_map.get(broker_key)
        credentials = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)

        if not broker_name or not credentials:
            continue

        matches = find_positions_for_symbol(broker_name, symbol, credentials)

        for pos in matches:
            order_id = None

            if broker_name.lower() == "upstox":
                order_id = us.upstox_close_position(credentials, pos)

            elif broker_name.lower() == "zerodha":
                order_id = zr.zerodha_close_position(credentials, pos)

            elif broker_name.lower() == "angelone":
                order_id = ar.angelone_close_position(obj, pos)

            elif broker_name.lower() == "groww":
                order_id = gr.groww_close_position(credentials, pos)

            elif broker_name.lower() == "5paisa":
                order_id = fp.fivepaisa_close_position(credentials, pos)

            if order_id:
                closed.append({"symbol": symbol, "broker": broker_name, "order_id": order_id})

    return jsonify({
        "message": "✅ Closed all positions successfully",
        "closed": closed
    })


# === DISCONNECT STOCK ===
@app.route("/api/disconnect-stock", methods=["POST"])
def disconnect_stock():
    data = request.json
    symbol = data.get("symbol")

    if symbol in active_trades:
        # ❌ Remove the symbol from active_trades completely
        active_trades.pop(symbol, None)
        return jsonify({"message": f"❌ {symbol} Disconnected"})

    return jsonify({"message": "⚠️ Stock not active"})


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    debug_flag = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_flag, use_reloader=False)
