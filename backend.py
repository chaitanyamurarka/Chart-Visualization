# backend.py
import configparser
import json
import logging
import os
import threading
from datetime import datetime, timedelta
import math
import requests
import socketio # python-socketio
from flask import Flask, jsonify, request, session, redirect, url_for, render_template, current_app
from flask_cors import CORS
from six.moves.urllib.parse import urljoin
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.serving import run_simple
from collections import OrderedDict
import pandas as pd # Still needed for Supertrend
from supertrend_strategy import calculate_supertrend

# --- Configuration & Globals ---
CONFIG_FILE_NAME = 'config.ini'
MASTER_CONTRACT_DIR = "csv"
users_db = {}
user_broker_credentials_db = {}
user_xts_sessions = {} # Stores {'token': ..., 'userID': ...} keyed by platform_user_id

XTS_SEGMENT_CODE_MAP = {
    "NSECM": 1, "NSEFO": 2, "NSECUR": 3, "MCXFO": 4, "NCDEXFO": 5,
    "BSECM": 13, "BSEFO": 14, "BSECDS": 15, "NSEIDX": 20
}

flask_app = Flask(__name__)
flask_app.secret_key = os.urandom(32)
CORS(flask_app)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)
log = logging.getLogger(__name__)

# --- XTS API Interaction Classes (XTSCommon, XTSConnect) ---
class XTSCommon:
    def __init__(self, token=None, userID=None, isInvestorClient=None):
        self.token = token
        self.userID = userID
        self.isInvestorClient = isInvestorClient if isInvestorClient is not None else False

class XTSConnect(XTSCommon):
    cfg = configparser.ConfigParser()
    _default_root_uri = 'https://moxtsapi.motilaloswal.com:3000'
    _ssl_flag = True # This corresponds to 'disable_ssl' = False in config by default, so SSL is enabled.
    _default_timeout = 7

    try:
        if not cfg.read(CONFIG_FILE_NAME): log.error(f"CRITICAL: {CONFIG_FILE_NAME} not found. Using defaults.")
        else:
            _default_root_uri = cfg.get('root_url', 'root', fallback=_default_root_uri)
            # cfg.getboolean returns a boolean. If 'disable_ssl' is True in config, _ssl_flag becomes True.
            _ssl_flag = cfg.getboolean('SSL', 'disable_ssl', fallback=not _ssl_flag) # if disable_ssl=true, _ssl_flag becomes true
    except Exception as e: log.error(f"Error reading {CONFIG_FILE_NAME}: {e}. Using defaults.")

    _routes = {
        "market.login": "/apimarketdata/auth/login", "market.logout": "/apimarketdata/auth/logout",
        "market.instruments.master": "/apimarketdata/instruments/master",
        "market.instruments.subscription": "/apimarketdata/instruments/subscription",
        "market.instruments.unsubscription": "/apimarketdata/instruments/subscription",
        "market.instruments.ohlc": "/apimarketdata/instruments/ohlc",
        "market.search.instrumentsbystring": '/apimarketdata/search/instruments',
    }

    def __init__(self, api_key=None, secret_key=None, source="WEBAPI", root=None, timeout=None, disable_ssl_param=None, token=None, userID=None): # Renamed disable_ssl to disable_ssl_param
        self.apiKey = api_key; self.secretKey = secret_key; self.source = source
        self.root = root or self._default_root_uri; self.timeout = timeout or self._default_timeout
        
        # verify_ssl is True if SSL is enabled, False if disabled.
        # _ssl_flag is True if config says 'disable_ssl = True'.
        # So, verify_ssl should be 'not _ssl_flag'.
        if disable_ssl_param is not None: # Parameter override
             self.verify_ssl = not disable_ssl_param
        else: # Use config
             self.verify_ssl = not self._ssl_flag


        super().__init__(token, userID)
        self.reqsession = requests.Session()
        if not self.verify_ssl:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except (ImportError, AttributeError) : log.warning("Cannot disable urllib3 warnings.")


    def _set_common_variables(self, token, userID, isInvestorClient=False):
        super().__init__(token, userID, isInvestorClient); self.token = token; self.userID = userID

    def _request(self, route, method, params_in=None):
        params = params_in.copy() if params_in else {}; uri = self._routes.get(route)
        if not uri: return {"type":"error", "description":f"Internal: Route '{route}' missing."}
        url = urljoin(self.root, uri); headers = {'Content-Type':'application/json'}
        if self.token: headers['Authorization'] = self.token
        data = json.dumps(params) if method in ["POST", "PUT"] and params else None
        qparams = params if method == "GET" and params else None
        try:
            r = self.reqsession.request(method,url,data=data,params=qparams,headers=headers,timeout=self.timeout,verify=self.verify_ssl)
            if route=="market.instruments.master" and r.status_code==200 and "text/plain" in r.headers.get("content-type","").lower(): return {"type":"success", "result":r.text, "contentType":"text/plain"}
            try: resp_json = r.json()
            except json.JSONDecodeError:
                if r.status_code==200: return {"type":"success_non_json", "content":r.text, "status_code":r.status_code}
                return {"type":"error", "description":"Invalid JSON from server", "details":r.text[:200], "code":r.status_code}
            
            if r.status_code >= 400 or resp_json.get("type","").lower() == "error" or ("description" in resp_json and "error" in str(resp_json.get("description","")).lower()) or ("message" in resp_json and "error" in str(resp_json.get("message","")).lower()):
                err_desc = resp_json.get("description", resp_json.get("message", f"HTTP {r.status_code} Error"))
                err_code = resp_json.get("code", r.status_code)
                err_details = resp_json.get("result", resp_json) # 'result' might contain more details on error
                return {"type": "error", "description": err_desc, "code": err_code, "details": err_details}
            return resp_json
        except requests.exceptions.RequestException as e: return {"type":"error","description":str(e)}

    def _get(self,r,p=None): return self._request(r,"GET",p)
    def _post(self,r,p=None): return self._request(r,"POST",p)
    def _put(self,r,p=None): return self._request(r,"PUT",p)

    def marketdata_login(self):
        if not self.apiKey or not self.secretKey: return {"type": "error", "description": "API Key/Secret missing for XTS login."}
        resp = self._post("market.login", {"appKey": self.apiKey, "secretKey": self.secretKey, "source": self.source})
        if resp and resp.get('type') == 'success' and isinstance(resp.get('result'), dict) and resp['result'].get('token'):
            self._set_common_variables(resp['result']['token'], resp['result']['userID'])
        return resp
    def get_master(self, seg_list):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (get_master)"}
        return self._post("market.instruments.master", {"exchangeSegmentList":seg_list if isinstance(seg_list,list) else [seg_list]})
    def get_ohlc(self, seg, iid, start, end, comp_val):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (get_ohlc)"}
        try: seg_int = int(seg)
        except ValueError:
            seg_mapped = XTS_SEGMENT_CODE_MAP.get(str(seg).upper())
            if seg_mapped is None: return {"type":"error", "description":f"Invalid segment for OHLC: {seg}"}
            seg_int = seg_mapped
        return self._get('market.instruments.ohlc', {'exchangeSegment':seg_int, 'exchangeInstrumentID':iid, 'startTime':start, 'endTime':end, 'compressionValue':comp_val})

    def send_subscription(self, insts, code=1501):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (send_subscription)"}
        return self._post('market.instruments.subscription', {'instruments':insts, 'xtsMessageCode':code})

    def send_unsubscription(self, insts, code=1501):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (send_unsubscription)"}
        return self._put('market.instruments.unsubscription', {'instruments':insts, 'xtsMessageCode':code})

# --- Market Data Socket Adapter ---
md_socket_clients_per_user = {}

class MDSocket_io_ClientAdapter:
    def __init__(self, token, user_id, flask_sio_server_instance, platform_user_id_ref):
        self.token, self.user_id, self.flask_sio = token, user_id, flask_sio_server_instance
        self.platform_user_id_ref = platform_user_id_ref
        cfg = configparser.ConfigParser(); cfg.read(CONFIG_FILE_NAME)
        root_uri = cfg.get('root_url', 'root', fallback=XTSConnect._default_root_uri)
        pub_fmt = cfg.get('root_url', 'publishformat', fallback='JSON')
        bc_mode = cfg.get('root_url', 'broadcastmode', fallback='Full')
        
        if root_uri.endswith('/'):
            root_uri = root_uri[:-1]
        
        self.conn_url = f"{root_uri}/?token={token}&userID={user_id}&publishFormat={pub_fmt}&broadcastMode={bc_mode}"
        self.xts_client = socketio.Client(logger=False, engineio_logger=False, reconnection_attempts=3, reconnection_delay=5)
        self._reg_handlers()

    def _reg_handlers(self):
        client = self.xts_client
        client.on('connect', self._on_xts_connect)
        client.on('disconnect', self._on_xts_disconnect)
        client.on('error', self._on_xts_error)
        client.on('connect_error', self._on_xts_conn_err)
        ev_map = {
            '1501-json-full':'md_1501_full', '1502-json-full':'md_1502_full',
            '1504-json-full':'md_1504_full', '1505-json-full':'md_1505_full',
            '1510-json-full':'md_1510_full', '1512-json-full':'md_1512_full',
            '1507-json-full': 'md_1507_full', '1101-json-full': 'md_1101_full',
            '1105-json-full': 'md_1105_full',
        }
        for xts_ev, flask_ev_suffix in ev_map.items():
            client.on(xts_ev, lambda d, fe_sfx=flask_ev_suffix, xe=xts_ev: self._fwd(f"market_data_{fe_sfx}", d, xe))

    def connect_xts(self, path='/apimarketdata/socket.io'):
        if not self.token:
            log.error(f"MDS_Adapter ({self.platform_user_id_ref}): Cannot connect XTS WS, token missing.")
            return False
        try:
            log.info(f"MDS_Adapter ({self.platform_user_id_ref}): Attempting to connect to XTS WS: {self.conn_url} with path {path}")
            self.xts_client.connect(self.conn_url, transports=['websocket'], socketio_path=path)
            return True
        except socketio.exceptions.ConnectionError as e:
            log.error(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS connection failed (socketio.ConnectionError): {e}", exc_info=True)
            return False
        except Exception as e:
            log.error(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS connection failed (General Exception): {e}", exc_info=True)
            return False

    def disconnect_xts(self):
        if self.xts_client and self.xts_client.connected:
            log.info(f"MDS_Adapter ({self.platform_user_id_ref}): Disconnecting from XTS WS.")
            self.xts_client.disconnect()
            # self.xts_client.wait() # Not always necessary, can cause blocking if not handled well
        else:
            log.info(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS already disconnected or not initialized.")

    def _fwd(self, fe, d, xe):
        data_payload_orig = d
        try:
            data_payload = d
            if isinstance(d, str):
                try: data_payload = json.loads(d)
                except json.JSONDecodeError:
                    log.error(f"MDS_Adapter ({self.platform_user_id_ref}): JSONDecodeError for event {xe}. Raw data: {d[:200]}")
                    self.flask_sio.emit(fe, d) 
                    return

            if not isinstance(data_payload, dict):
                log.error(f"MDS_Adapter ({self.platform_user_id_ref}): Parsed/received data is not a dict for event {xe}. Type: {type(data_payload)}, Data: {str(data_payload)[:200]}")
                self.flask_sio.emit(fe, data_payload)
                return

            instrument_id = data_payload.get('ExchangeInstrumentID')
            segment_code = data_payload.get('ExchangeSegment')

            if instrument_id is not None and segment_code is not None:
                room_name = f"instrument_{segment_code}_{instrument_id}"
                self.flask_sio.emit(fe, data_payload, room=room_name)
            else:
                self.flask_sio.emit(fe, data_payload)
        except Exception as e:
            log.error(f"MDS_Adapter ({self.platform_user_id_ref}): Error in _fwd processing event {xe}: {e}. Original Data: {str(data_payload_orig)[:200]}", exc_info=True)
            self.flask_sio.emit(fe, data_payload_orig)

    def _on_xts_connect(self): log.info(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS Connected! SID:{self.xts_client.sid}")
    def _on_xts_disconnect(self): log.info(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS Disconnected!")
    def _on_xts_error(self,d): log.error(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS Error: {d}")
    def _on_xts_conn_err(self,d): log.error(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS ConnectError: {d}")

# --- Socket.IO Server Instance ---
sio_server = socketio.Server(async_mode='threading', cors_allowed_origins="*")
application = socketio.WSGIApp(sio_server, flask_app)

# --- Utility Functions ---
def create_dir_if_not_exists(d):
    if not os.path.exists(d): os.makedirs(d); log.info(f"Created dir: {d}")

def get_xts_client_by_platform_id(platform_user_id):
    if not platform_user_id:
        log.warning("get_xts_client_by_platform_id: platform_user_id is missing.")
        return None
    creds = user_broker_credentials_db.get(platform_user_id)
    xts_session_data = user_xts_sessions.get(platform_user_id)
    if creds and xts_session_data and xts_session_data.get('token'):
        return XTSConnect(api_key=creds['api_key'], secret_key=creds['api_secret'],
                          token=xts_session_data['token'], userID=xts_session_data['userID'])
    else:
        log.warning(f"Could not get XTS client for platform_user_id '{platform_user_id}'. Creds or XTS session data/token missing.")
        return None

def get_current_user_xts_client_instance():
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id or not session.get('xts_logged_in'): return None
    return get_xts_client_by_platform_id(platform_user_id)

def format_ohlc_data_for_frontend(ohlc_text, interval_minutes_for_aggregation):
    raw_parsed_points = []
    if not ohlc_text: return []
    data_point_strings = ohlc_text.split(',')
    if not data_point_strings or (len(data_point_strings) == 1 and not data_point_strings[0].strip()): return []

    for point_str in data_point_strings:
        if not point_str.strip(): continue
        parts = point_str.split('|')
        if len(parts) >= 6:
            try:
                ts = int(parts[0]);
                o, h, l, c = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]);
                v = int(parts[5])
                if not all(math.isfinite(val) for val in [ts, o, h, l, c, v]): log.debug(f"Skipping point due to non-finite value: {point_str}"); continue
                if v < 0: v = 0
                if h < l: log.debug(f"Skipping point due to high < low: {point_str}"); continue
                raw_parsed_points.append({"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
            except ValueError as e: log.debug(f"Skipping point due to ValueError {e}: {point_str}"); pass
        else: log.debug(f"Skipping point due to insufficient parts: {point_str}"); pass

    if not raw_parsed_points: return []
    raw_parsed_points.sort(key=lambda x: x["time"])

    aggregated_candles = OrderedDict()
    interval_seconds = interval_minutes_for_aggregation * 60

    for point in raw_parsed_points:
        interval_start_time = (point["time"] // interval_seconds) * interval_seconds
        if interval_start_time not in aggregated_candles:
            aggregated_candles[interval_start_time] = {
                "time": interval_start_time, "open": point["open"], "high": point["high"],
                "low": point["low"], "close": point["close"], "volume": point["volume"],
                "_latest_timestamp_in_interval": point["time"]
            }
        else:
            candle = aggregated_candles[interval_start_time]
            candle["high"] = max(candle["high"], point["high"])
            candle["low"] = min(candle["low"], point["low"])
            candle["volume"] += point["volume"]
            if point["time"] >= candle["_latest_timestamp_in_interval"]:
                candle["close"] = point["close"]
                candle["_latest_timestamp_in_interval"] = point["time"]
    final_candles = []
    for candle_data in aggregated_candles.values():
        del candle_data["_latest_timestamp_in_interval"]
        if candle_data["high"] < candle_data["low"]: log.warning(f"Aggregated candle invalid (H<L): {candle_data}"); continue
        final_candles.append(candle_data)
    return final_candles

def convert_ohlc_text_to_df(ohlc_text):
    if not ohlc_text:
        return pd.DataFrame()
    data_point_strings = ohlc_text.split(',')
    records = []
    for point_str in data_point_strings:
        if not point_str.strip(): continue
        parts = point_str.split('|')
        if len(parts) >= 6:
            try:
                record = {
                    "time": int(parts[0]),
                    "open": float(parts[1]), "high": float(parts[2]),
                    "low": float(parts[3]), "close": float(parts[4]),
                    "volume": int(parts[5])
                }
                records.append(record)
            except ValueError as e:
                log.debug(f"Skipping point in convert_ohlc_text_to_df due to ValueError {e}: {point_str}")
                pass
        else:
            log.debug(f"Skipping point due to insufficient parts in convert_ohlc_text_to_df: {point_str}")
            pass
    df = pd.DataFrame(records)
    if not df.empty:
        df.sort_values(by='time', inplace=True)
    return df

# --- Flask Routes ---
@flask_app.route('/')
def landing_page_route():
    if session.get('platform_user_id'):
        if user_broker_credentials_db.get(session['platform_user_id']) and \
           session.get('xts_logged_in') and \
           session.get('platform_user_id') in user_xts_sessions:
            return redirect(url_for('chart_page_route'))
        return redirect(url_for('broker_setup_page_route'))
    return render_template('landing.html')

@flask_app.route('/signup', methods=['GET', 'POST'])
def platform_signup_route():
    if request.method == 'POST':
        username = request.form.get('username'); password = request.form.get('password')
        if not username or not password: return render_template('platform_signup.html', error="Username and password are required.")
        if username in users_db: return render_template('platform_signup.html', error="Username already exists.")
        users_db[username] = generate_password_hash(password)
        session['platform_user_id'] = username; session['logged_in'] = True
        session['xts_logged_in'] = False
        return redirect(url_for('broker_setup_page_route'))
    return render_template('platform_signup.html')

@flask_app.route('/login', methods=['GET', 'POST'])
def platform_login_route():
    if request.method == 'POST':
        username = request.form.get('username'); password = request.form.get('password')
        user_hash = users_db.get(username)
        if user_hash and check_password_hash(user_hash, password):
            session['platform_user_id'] = username; session['logged_in'] = True
            if username in user_xts_sessions and user_xts_sessions[username].get('token'):
                session['xts_logged_in'] = True
                session['xts_token'] = user_xts_sessions[username]['token']
                session['xts_userID'] = user_xts_sessions[username]['userID']
                return redirect(url_for('chart_page_route'))
            else:
                session['xts_logged_in'] = False
                return redirect(url_for('broker_setup_page_route'))
        else: return render_template('platform_login.html', error="Invalid username or password.")
    return render_template('platform_login.html')

@flask_app.route('/logout', methods=['POST'])
def platform_logout_route():
    platform_user_id = session.get('platform_user_id')
    if platform_user_id:
        if platform_user_id in md_socket_clients_per_user:
            md_client = md_socket_clients_per_user.pop(platform_user_id)
            md_client.disconnect_xts()
            log.info(f"Disconnected and removed MDSocket client for user {platform_user_id} on logout.")
        if platform_user_id in user_xts_sessions:
            del user_xts_sessions[platform_user_id]
            log.info(f"Cleared XTS session data for user {platform_user_id}.")
    session.clear()
    return redirect(url_for('landing_page_route'))

@flask_app.route('/broker_setup', methods=['GET', 'POST'])
def broker_setup_page_route():
    if not session.get('platform_user_id'): return redirect(url_for('platform_login_route'))
    platform_user_id = session['platform_user_id']

    if request.method == 'POST':
        api_key = request.form.get('api_key'); api_secret = request.form.get('api_secret')
        if not api_key or not api_secret: return render_template('broker_setup.html', error="API Key and Secret are required.", user=platform_user_id, creds_exist=bool(user_broker_credentials_db.get(platform_user_id)))

        user_broker_credentials_db[platform_user_id] = {"api_key": api_key, "api_secret": api_secret}
        temp_xts_client = XTSConnect(api_key, api_secret)
        xts_resp = temp_xts_client.marketdata_login()

        if xts_resp and xts_resp.get('type') == 'success' and temp_xts_client.token:
            user_xts_sessions[platform_user_id] = {"token": temp_xts_client.token, "userID": temp_xts_client.userID}
            session['xts_token'] = temp_xts_client.token
            session['xts_userID'] = temp_xts_client.userID
            session['xts_logged_in'] = True

            if platform_user_id in md_socket_clients_per_user:
                old_md_client = md_socket_clients_per_user.pop(platform_user_id)
                old_md_client.disconnect_xts()
                log.info(f"Disconnected old MDSocket client for user {platform_user_id} before new login.")

            md_user_client = MDSocket_io_ClientAdapter(
                temp_xts_client.token, temp_xts_client.userID,
                sio_server, platform_user_id
            )
            if md_user_client.connect_xts():
                md_socket_clients_per_user[platform_user_id] = md_user_client
                log.info(f"Successfully initiated and connected XTS market data socket for user {platform_user_id}.")
            else:
                log.error(f"Failed to connect user {platform_user_id}'s XTS market data socket after broker login.")
            return redirect(url_for('chart_page_route'))
        else:
            desc = xts_resp.get('description', "XTS Login Failed") if isinstance(xts_resp, dict) else "XTS Login Failed (Unknown reason)"
            session['xts_logged_in'] = False
            if platform_user_id in user_xts_sessions: del user_xts_sessions[platform_user_id]
            current_creds = user_broker_credentials_db.get(platform_user_id)
            return render_template('broker_setup.html', error=f"XTS Login Failed: {desc}", user=platform_user_id, creds_exist=bool(current_creds))

    existing_creds = user_broker_credentials_db.get(platform_user_id)
    xts_http_session_active = session.get('xts_logged_in', False) and \
                              (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token'))
    xts_md_socket_active = platform_user_id in md_socket_clients_per_user and \
                           md_socket_clients_per_user[platform_user_id].xts_client.connected

    return render_template('broker_setup.html',
                           user=platform_user_id,
                           creds_exist=bool(existing_creds),
                           xts_session_active=xts_http_session_active,
                           xts_md_socket_active=xts_md_socket_active)

@flask_app.route('/chart')
def chart_page_route():
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id or not session.get('logged_in'):
        return redirect(url_for('platform_login_route'))

    is_xts_http_setup_valid = user_broker_credentials_db.get(platform_user_id) and \
                              session.get('xts_logged_in', False) and \
                              (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token'))

    if not is_xts_http_setup_valid:
        log.warning(f"User {platform_user_id} attempting to access chart page without valid XTS HTTP session/setup. Redirecting to broker setup.")
        if not (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token')):
            session['xts_logged_in'] = False
        return redirect(url_for('broker_setup_page_route'))

    return render_template('chart_page.html', username=platform_user_id)

@flask_app.route('/api/download-master-contracts', methods=['POST'])
def download_master_contracts_api():
    if not session.get('platform_user_id') or not session.get('xts_logged_in'):
        return jsonify({"error":"Authentication required for XTS operations"}), 401

    client = get_current_user_xts_client_instance()
    if not client:
        return jsonify({"error":"Broker client unavailable or XTS session invalid"}), 500

    segs_req = request.json.get('segments',["NSECM","NSEFO","MCXFO", "NSECUR", "NSEIDX"])
    results={}; create_dir_if_not_exists(MASTER_CONTRACT_DIR)

    for s_name in segs_req:
        r = client.get_master([s_name])
        if r and r.get('type')=='success' and r.get("contentType")=="text/plain":
            try:
                fpath=os.path.join(MASTER_CONTRACT_DIR,f"{s_name.upper()}.csv")
                dat=r['result']
                hdr_cm = "ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,UnderlyingInstrumentId,UnderlyingIndexName,ContractExpiration,StrikePrice,OptionType,displayName,ISIN,PriceNumerator,PriceDenominator\n"
                hdr_fo = "ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,UnderlyingInstrumentId,UnderlyingIndexName,ContractExpiration,StrikePrice,OptionType,displayName,PriceNumerator,PriceDenominator\n"
                hdr_idx = "ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,displayName\n"

                if "CM" in s_name.upper(): current_hdr = hdr_cm
                elif "FO" in s_name.upper() or "CUR" in s_name.upper(): current_hdr = hdr_fo
                elif "IDX" in s_name.upper(): current_hdr = hdr_idx
                else: current_hdr = hdr_cm

                with open(fpath,"w",encoding='utf-8') as f:
                    f.write(current_hdr + dat.replace("|",","))
                results[s_name]={"type":"success","description":f"Saved {s_name} to {fpath}."}
            except Exception as e:
                results[s_name]={"type":"error","description":str(e)}
                log.error(f"Error saving master contract for {s_name}: {e}", exc_info=True)
        else:
            desc = r.get('description','Download failed') if isinstance(r,dict) else 'Download failed (unknown)'
            code = r.get('code', 'N/A') if isinstance(r,dict) else 'N/A'
            results[s_name]={"type":"error","description":desc, "code": code, "details":r if isinstance(r,dict) else str(r)}
            log.error(f"Failed to download master contract for {s_name}: {desc} (Code: {code})")

    return jsonify(results)


# --- REVERTED /api/scrips to original logic ---
@flask_app.route('/api/scrips', methods=['GET'])
def get_scrips_api():
    if not session.get('platform_user_id'): return jsonify({"error":"Not authenticated"}), 401
    all_scrips = []
    files_map = {"NSECM.CSV":"NSECM", "NSEFO.CSV":"NSEFO", "MCXFO.CSV":"MCXFO", "NSECUR.CSV":"NSECUR", "NSEIDX.CSV":"NSEIDX"}
    for fname_upper, def_seg_key in files_map.items():
        fpath = os.path.join(MASTER_CONTRACT_DIR, fname_upper)
        if os.path.exists(fpath):
            try:
                with open(fpath, 'r', encoding='utf-8-sig') as f: # utf-8-sig handles BOM
                    lines = f.readlines()
                    if len(lines) < 2: continue # Need header and at least one data line

                    header_line = lines[0].strip()
                    header = [h.strip().lower() for h in header_line.split(',')] # Normalize header

                    # Define aliases for critical columns, original approach
                    col_map = {'displayName': ['displayname', 'symbol', 'name', 'description', 'namewithseries'],
                               'exchangeInstrumentID': ['exchangeinstrumentid', 'instrumentid', 'token'],
                               'exchangeSegment': ['exchangesegment']}
                    indices = {}
                    missing_critical_col = False
                    for key, aliases in col_map.items():
                        found_idx = -1
                        for alias in aliases:
                            try:
                                found_idx = header.index(alias)
                                break
                            except ValueError:
                                continue # Alias not in header
                        if found_idx != -1:
                            indices[key] = found_idx
                        elif key in ['displayName', 'exchangeInstrumentID']: # If critical alias group not found
                            log.warning(f"Critical column group '{key}' not found in header of {fname_upper} using aliases: {aliases}")
                            missing_critical_col = True
                            break
                    
                    if missing_critical_col:
                        log.warning(f"Skipping file {fname_upper} due to missing critical columns.")
                        continue

                    for l in lines[1:]:
                        p = [c.strip() for c in l.split(',')]
                        try:
                            # Ensure row has enough columns for max index required
                            max_req_idx = 0
                            if 'displayName' in indices: max_req_idx = max(max_req_idx, indices['displayName'])
                            if 'exchangeInstrumentID' in indices: max_req_idx = max(max_req_idx, indices['exchangeInstrumentID'])
                            if 'exchangeSegment' in indices: max_req_idx = max(max_req_idx, indices['exchangeSegment'])

                            if len(p) > max_req_idx:
                                dispN = p[indices['displayName']] if 'displayName' in indices else None
                                idS = p[indices['exchangeInstrumentID']] if 'exchangeInstrumentID' in indices else None
                                
                                segV_csv = def_seg_key # Default segment
                                if 'exchangeSegment' in indices and indices['exchangeSegment'] < len(p) and p[indices['exchangeSegment']]:
                                    segV_csv = p[indices['exchangeSegment']].upper()
                                
                                if dispN and idS: # Only add if essential parts are present
                                    all_scrips.append({"displayName":dispN, "exchangeInstrumentID":idS, "exchangeSegment": segV_csv})
                        except IndexError:
                            log.debug(f"IndexError processing line in {fname_upper}. Line: '{l[:100]}...', Indices: {indices}")
                            pass # Skip malformed lines
                        except KeyError as e:
                             log.debug(f"KeyError processing line in {fname_upper}: {e}. Line: '{l[:100]}...'. This might indicate a missing critical column not caught by initial check.")
                             pass


            except Exception as e: log.error(f"Error processing scrip file {fpath}: {e}", exc_info=True)

    if not all_scrips:
        all_scrips = [{"displayName":"NIFTY 50","exchangeInstrumentID":"26000","exchangeSegment":"NSEIDX"}]
    else:
        seen_keys = set(); unique_scrips_list = []
        for s_obj in all_scrips:
            item_key = f"{s_obj['exchangeSegment']}_{s_obj['exchangeInstrumentID']}"
            if item_key not in seen_keys:
                unique_scrips_list.append(s_obj)
                seen_keys.add(item_key)
        all_scrips = sorted(unique_scrips_list, key=lambda x: (x["displayName"], x["exchangeSegment"]))
    return jsonify(all_scrips)
# --- END OF REVERTED /api/scrips ---


@flask_app.route('/api/historical-data', methods=['GET'])
def get_historical_data_api():
    if not session.get('platform_user_id') or not session.get('xts_logged_in'):
        return jsonify({"type":"error","description":"Authentication required."}), 401
    client = get_current_user_xts_client_instance()
    if not client:
        return jsonify({"type":"error","description":"Broker client not available or XTS session invalid."}), 500

    sName = request.args.get('scrip')
    tf = request.args.get('timeframe')
    seg_name = request.args.get('exchangeSegment')
    iid = request.args.get('exchangeInstrumentID')

    if not (sName and tf and seg_name and iid):
        return jsonify({"type":"error", "description":"All parameters (scrip, timeframe, exchangeSegment, exchangeInstrumentID) required."}), 400

    timeframe_map = {"1D":(1440,730), "1H":(60,90), "15min":(15,30), "5min":(5,15), "1min":(1,7)}
    config = timeframe_map.get(tf)
    if not config: return jsonify({"type":"error", "description":f"Unsupported timeframe: {tf}"}), 400

    compV_minutes, daysLB = int(config[0]), int(config[1])
    seg_code = XTS_SEGMENT_CODE_MAP.get(seg_name.upper())
    if seg_code is None:
        return jsonify({"type": "error", "description": f"Invalid exchange segment name: {seg_name}"}), 400

    endT = datetime.now(); startT_dt = endT - timedelta(days=daysLB)
    startT_str = startT_dt.strftime("%b %d %Y %H%M"); endT_str = endT.strftime("%b %d %Y %H%M")

    log.info(f"Fetching OHLC for {sName} (SegCode:{seg_code}, ID:{iid}) TF:{tf} ({compV_minutes}m) from {startT_str} to {endT_str}")
    ohlcResp = client.get_ohlc(seg_code, iid, startT_str, endT_str, compV_minutes)

    if ohlcResp and ohlcResp.get("type") == "success" and "result" in ohlcResp:
        ohlcTxt = ohlcResp["result"].get("dataReponse", "") # Assuming 'dataReponse' is the actual key
        if not ohlcTxt:
            log.info(f"Empty dataReponse from API for {sName} ({seg_name}:{iid}). OHLC Response: {ohlcResp}")
            return jsonify([])
        formatted_data = format_ohlc_data_for_frontend(ohlcTxt, compV_minutes)
        return jsonify(formatted_data)
    else:
        desc = ohlcResp.get('description', 'OHLC fetch failed') if isinstance(ohlcResp, dict) else 'OHLC API error'
        status_code_val = ohlcResp.get("code", 500) if isinstance(ohlcResp, dict) else 500
        try: status_code_val = int(status_code_val)
        except ValueError: status_code_val = 500
        log.error(f"OHLC API error for {sName} ({seg_name}:{iid}): {desc}. Details: {ohlcResp}")
        return jsonify({"type":"error", "description":desc, "details":ohlcResp}), status_code_val


@flask_app.route('/api/strategy-data', methods=['GET'])
def get_strategy_data_api():
    if not session.get('platform_user_id') or not session.get('xts_logged_in'):
        return jsonify({"type":"error","description":"Authentication required."}), 401

    client = get_current_user_xts_client_instance()
    if not client:
        return jsonify({"type":"error","description":"Broker client not available or XTS session invalid."}), 500

    sName = request.args.get('scrip')
    tf = request.args.get('timeframe')
    seg_name = request.args.get('exchangeSegment')
    iid = request.args.get('exchangeInstrumentID')
    strategy_name = request.args.get('strategy', 'supertrend')
    st_period = request.args.get('st_period', default=10, type=int)
    st_multiplier = request.args.get('st_multiplier', default=3, type=float)

    if not (sName and tf and seg_name and iid):
        return jsonify({"type":"error", "description":"Scrip, timeframe, segment, and instrumentID are required."}), 400

    timeframe_map = {"1D":(1440,730), "1H":(60,90), "15min":(15,30), "5min":(5,15), "1min":(1,7)}
    config = timeframe_map.get(tf)
    if not config: return jsonify({"type":"error", "description":f"Unsupported timeframe: {tf}"}), 400

    compV_minutes, daysLB = int(config[0]), int(config[1])
    seg_code = XTS_SEGMENT_CODE_MAP.get(seg_name.upper())
    if seg_code is None:
        return jsonify({"type": "error", "description": f"Invalid exchange segment name: {seg_name}"}), 400

    endT = datetime.now(); startT_dt = endT - timedelta(days=daysLB)
    startT_str = startT_dt.strftime("%b %d %Y %H%M"); endT_str = endT.strftime("%b %d %Y %H%M")

    log.info(f"Fetching OHLC for strategy {strategy_name} on {sName} (Seg:{seg_code}, ID:{iid}) TF:{tf} from {startT_str} to {endT_str}")
    ohlcResp = client.get_ohlc(seg_code, iid, startT_str, endT_str, compV_minutes)

    if not (ohlcResp and ohlcResp.get("type") == "success" and "result" in ohlcResp):
        desc = ohlcResp.get('description', 'OHLC fetch failed') if isinstance(ohlcResp, dict) else 'OHLC API error'
        err_code = ohlcResp.get("code", 500) if isinstance(ohlcResp, dict) else 500
        log.error(f"OHLC API error for {sName}: {desc}")
        return jsonify({"type":"error", "description":desc, "details": ohlcResp}), err_code

    ohlc_text_data = ohlcResp["result"].get("dataReponse", "")
    if not ohlc_text_data:
        log.info(f"Empty dataReponse from API for strategy data on {sName}.")
        return jsonify({"ohlc": [], "strategy": {}})

    formatted_ohlc_for_frontend = format_ohlc_data_for_frontend(ohlc_text_data, compV_minutes)
    strategy_data_output = { 'supertrend_line': [], 'buy_signals': [], 'sell_signals': [] }

    if strategy_name.lower() == 'supertrend':
        if formatted_ohlc_for_frontend:
            strategy_input_df = pd.DataFrame(formatted_ohlc_for_frontend)
            if not strategy_input_df.empty and all(col in strategy_input_df.columns for col in ['time', 'high', 'low', 'close']):
                strategy_output_df = calculate_supertrend(strategy_input_df.copy(), period=st_period, multiplier=st_multiplier)

                if not strategy_output_df.empty:
                    st_line_data = strategy_output_df[['time', 'supertrend']].copy()
                    st_line_data.rename(columns={'supertrend': 'value'}, inplace=True)
                    st_line_data.dropna(subset=['value'], inplace=True)
                    strategy_data_output['supertrend_line'] = st_line_data.to_dict(orient='records')

                    buy_signals_df = strategy_output_df[strategy_output_df['buy_signal_time'].notna()]
                    if not buy_signals_df.empty:
                        buy_markers = pd.merge(buy_signals_df[['buy_signal_time']],
                                               strategy_input_df[['time', 'close']],
                                               left_on='buy_signal_time', right_on='time', how='left')
                        strategy_data_output['buy_signals'] = buy_markers.rename(
                            columns={'buy_signal_time': 'time', 'close': 'price'}
                        )[['time', 'price']].to_dict(orient='records')

                    sell_signals_df = strategy_output_df[strategy_output_df['sell_signal_time'].notna()]
                    if not sell_signals_df.empty:
                        sell_markers = pd.merge(sell_signals_df[['sell_signal_time']],
                                                strategy_input_df[['time', 'close']],
                                                left_on='sell_signal_time', right_on='time', how='left')
                        strategy_data_output['sell_signals'] = sell_markers.rename(
                            columns={'sell_signal_time': 'time', 'close': 'price'}
                        )[['time', 'price']].to_dict(orient='records')
            else: log.warning(f"DataFrame for Supertrend is empty or missing columns for {sName}.")
        else: log.warning(f"No formatted OHLC data for Supertrend for {sName}.")

    return jsonify({
        "ohlc": formatted_ohlc_for_frontend,
        "strategy": strategy_data_output
    })

# --- Socket.IO Event Handlers ---
@sio_server.event
def connect(sid, environ):
    with flask_app.request_context(environ):
        try:
            platform_user_id = session.get('platform_user_id')
            xts_session_active_in_flask = platform_user_id and \
                                          session.get('xts_logged_in', False) and \
                                          session.get('xts_token') and \
                                          session.get('xts_userID')
            xts_session_valid_in_store = platform_user_id and \
                                         platform_user_id in user_xts_sessions and \
                                         user_xts_sessions[platform_user_id].get('token') == session.get('xts_token')

            log.info(f"Flask WS Client connect attempt: SID {sid}, User: {platform_user_id}, XTS Active (Flask Session): {xts_session_active_in_flask}, XTS Valid (Store): {xts_session_valid_in_store}")

            if not (xts_session_active_in_flask and xts_session_valid_in_store):
                log.warning(f"SocketIO connect REJECTED for SID {sid}: User '{platform_user_id}' not authenticated or XTS session invalid/mismatched.")
                sio_server.emit('auth_error', {'message': 'Authentication rejected. Please re-login to broker.'}, room=sid)
                return False

            sio_server.save_session(sid, {
                'platform_user_id': platform_user_id,
                'xts_logged_in': True,
                'xts_broker_userID': session.get('xts_userID')
            })
            log.info(f"Stored platform_user_id '{platform_user_id}' in Socket.IO session for SID {sid}")
            sio_server.emit('server_message', {'data': f'Welcome {platform_user_id}! WebSocket connected.'}, room=sid)
            return True
        except Exception as e:
            log.error(f"Exception in Socket.IO connect handler for SID {sid}: {e}", exc_info=True)
            return False

@sio_server.event
def disconnect(sid):
    sio_session_data = sio_server.get_session(sid) if sio_server else {}
    platform_user_id = sio_session_data.get('platform_user_id', 'Unknown user')
    log.info(f"Flask WS Client disconnect: SID {sid}, Platform User: {platform_user_id}")

@sio_server.on('subscribe_instrument')
def handle_subscribe_instrument(sid, data):
    sio_session_data = sio_server.get_session(sid)
    platform_user_id = sio_session_data.get('platform_user_id')
    xts_is_logged_in_sio = sio_session_data.get('xts_logged_in', False)

    if not platform_user_id or not xts_is_logged_in_sio:
        sio_server.emit('subscription_error',{'error':'Authentication required for subscription.'},room=sid); return

    api_client = get_xts_client_by_platform_id(platform_user_id)
    if not api_client:
        sio_server.emit('subscription_error',{'error':'Broker client unavailable for subscription commands.'},room=sid); return

    md_feed_client = md_socket_clients_per_user.get(platform_user_id)
    if not md_feed_client or not md_feed_client.xts_client.connected:
        log.warning(f"User {platform_user_id}'s XTS Market Data socket (MDSocket_io_ClientAdapter) is not connected. Live data may not flow.")
        sio_server.emit('subscription_error', {'error': 'Market data feed connection is not active.'}, room=sid)
        # Not returning, as HTTP subscription might still be useful for some XTS setups.

    try:
        segment_name = data['exchangeSegment']
        instrument_id_str = str(data['exchangeInstrumentID'])
        instrument_id_int = int(instrument_id_str)
        segment_code = XTS_SEGMENT_CODE_MAP.get(segment_name.upper())

        if segment_code is None:
            sio_server.emit('subscription_error', {'error': f"Unknown segment: {segment_name}"}, room=sid); return

        room_name = f"instrument_{segment_code}_{instrument_id_int}"
        sio_server.enter_room(sid, room_name)
        log.info(f"SID {sid} (User: {platform_user_id}) entered room {room_name} for {segment_name}:{instrument_id_int}")

        xts_inst_payload = [{'exchangeSegment': segment_code, 'exchangeInstrumentID': instrument_id_int}]
        codes_to_subscribe = [1501, 1502] # LTP, Quote

        all_ok, responses = True, {}
        for code in codes_to_subscribe:
            r = api_client.send_subscription(xts_inst_payload, code=code)
            responses[str(code)] = r
            if not (r and r.get('type')=='success'):
                if r and r.get('code') == 'e-session-0002' and "Already Subscribed" in r.get('description',''):
                    log.info(f"XTS info for {platform_user_id}, {xts_inst_payload}, code {code}: {r.get('description')}")
                else:
                    all_ok = False;
                    log.error(f"XTS Subscription API Error for {platform_user_id}, {xts_inst_payload}, code {code}: {r}")
        
        critical_error_found = any(
            resp.get('type') != 'success' and not (resp.get('code') == 'e-session-0002' and "Already Subscribed" in resp.get('description',''))
            for resp in responses.values()
        )

        if not critical_error_found:
            sio_server.emit('subscription_ack', {'instrument': data, 'room': room_name, 'message': f'Subscription confirmed for {data.get("displayName", instrument_id_str)}.'}, room=sid)
        else:
            sio_server.emit('subscription_error',{'error':'XTS subscription to one or more services failed.','details':responses}, room=sid)

    except KeyError as e:
        log.error(f"KeyError in handle_subscribe_instrument (SID {sid}, User {platform_user_id}): Missing key {e} in data: {data}", exc_info=True)
        sio_server.emit('subscription_error', {'error': f'Server error: Missing expected data field {e}.'}, room=sid)
    except ValueError as e:
        log.error(f"ValueError in handle_subscribe_instrument (SID {sid}, User {platform_user_id}): Invalid data format. {e}. Data: {data}", exc_info=True)
        sio_server.emit('subscription_error', {'error': 'Server error: Invalid data format for instrument ID.'}, room=sid)
    except Exception as e:
        log.error(f"General Error in handle_subscribe_instrument (SID {sid}, User {platform_user_id}): {e}", exc_info=True)
        sio_server.emit('subscription_error', {'error': 'Server error during subscription processing.'}, room=sid)

@sio_server.on('unsubscribe_instrument')
def handle_unsubscribe_instrument(sid, data):
    sio_session_data = sio_server.get_session(sid)
    platform_user_id = sio_session_data.get('platform_user_id')
    if not platform_user_id:
        sio_server.emit('unsubscription_error',{'error':'Not authenticated for unsubscription.'},room=sid); return

    api_client = get_xts_client_by_platform_id(platform_user_id)
    if not api_client:
        sio_server.emit('unsubscription_error',{'error':'Broker client unavailable for unsubscription commands.'},room=sid); return

    try:
        segment_name = data['exchangeSegment']
        instrument_id_str = str(data['exchangeInstrumentID'])
        instrument_id_int = int(instrument_id_str)
        segment_code = XTS_SEGMENT_CODE_MAP.get(segment_name.upper())

        if segment_code is None:
            sio_server.emit('unsubscription_error', {'error': f"Unknown segment for unsubscription: {segment_name}"}, room=sid); return

        room_name = f"instrument_{segment_code}_{instrument_id_int}"
        sio_server.leave_room(sid, room_name)
        log.info(f"SID {sid} (User: {platform_user_id}) left room {room_name} for {segment_name}:{instrument_id_int}")
        
        # Simplified unsubscription: client leaves room, XTS unsubscription not sent to avoid affecting other tabs.
        sio_server.emit('unsubscription_ack', {'instrument': data, 'room': room_name, 'message': f'Left instrument room for {data.get("displayName", instrument_id_str)}.'}, room=sid)

    except KeyError as e:
        log.error(f"KeyError in handle_unsubscribe_instrument (SID {sid}): Missing key {e} in data: {data}", exc_info=True)
        sio_server.emit('unsubscription_error', {'error': f'Server error: Missing expected data field {e} for unsubscription.'}, room=sid)
    except ValueError as e:
        log.error(f"ValueError in handle_unsubscribe_instrument (SID {sid}): {e}. Data: {data}", exc_info=True)
        sio_server.emit('unsubscription_error', {'error': 'Server error: Invalid data format for unsubscription.'}, room=sid)
    except Exception as e:
        log.error(f"Error in handle_unsubscribe_instrument (SID {sid}): {e}", exc_info=True)
        sio_server.emit('unsubscription_error', {'error': 'Server error during unsubscription processing.'}, room=sid)

# --- Server Startup ---
if __name__ == '__main__':
    create_dir_if_not_exists(MASTER_CONTRACT_DIR)
    log.info(f"Starting Flask app with SocketIO. Config:'{CONFIG_FILE_NAME}' Debug: {flask_app.debug}")
    host = os.environ.get('FLASK_RUN_HOST', '127.0.0.1')
    port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    
    try:
        run_simple(host, port, application,
                   use_reloader=False, 
                   use_debugger=flask_app.debug,
                   threaded=True)
    except Exception as e:
        log.critical(f"Failed to start Werkzeug development server: {e}", exc_info=True)