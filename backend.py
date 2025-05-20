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

# --- Configuration & Globals ---
CONFIG_FILE_NAME = 'config.ini'
MASTER_CONTRACT_DIR = "csv"
users_db = {} 
user_broker_credentials_db = {}
user_xts_sessions = {} # Stores {'token': ..., 'userID': ...} keyed by platform_user_id

# This map needs to be accurate based on XTS API documentation/constants
XTS_SEGMENT_CODE_MAP = {
    "NSECM": 1, "NSEFO": 2, "NSECUR": 3, "MCXFO": 4, "NCDEXFO": 5, 
    "BSECM": 13, "BSEFO": 14, "BSECDS": 15, "NSEIDX": 20 
}

# Initialize Flask app instance
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
    _ssl_flag = True 
    _default_timeout = 7
    
    try:
        if not cfg.read(CONFIG_FILE_NAME): log.error(f"CRITICAL: {CONFIG_FILE_NAME} not found. Using defaults.")
        else:
            _default_root_uri = cfg.get('root_url', 'root', fallback=_default_root_uri)
            _ssl_flag = cfg.getboolean('SSL', 'disable_ssl', fallback=_ssl_flag)
    except Exception as e: log.error(f"Error reading {CONFIG_FILE_NAME}: {e}. Using defaults.")

    _routes = {
        "market.login": "/apimarketdata/auth/login", "market.logout": "/apimarketdata/auth/logout",
        "market.instruments.master": "/apimarketdata/instruments/master",
        "market.instruments.subscription": "/apimarketdata/instruments/subscription",
        "market.instruments.unsubscription": "/apimarketdata/instruments/subscription", 
        "market.instruments.ohlc": "/apimarketdata/instruments/ohlc",
        "market.search.instrumentsbystring": '/apimarketdata/search/instruments',
    }

    def __init__(self, api_key=None, secret_key=None, source="WEBAPI", root=None, timeout=None, disable_ssl=None, token=None, userID=None):
        self.apiKey = api_key; self.secretKey = secret_key; self.source = source
        self.root = root or self._default_root_uri; self.timeout = timeout or self._default_timeout
        self.verify_ssl = not (disable_ssl if disable_ssl is not None else self._ssl_flag) 
        super().__init__(token, userID)
        self.reqsession = requests.Session()
        if not self.verify_ssl:
            try: requests.packages.urllib3.disable_warnings()
            except AttributeError: log.warning("Cannot disable urllib3 warnings.")

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
            if r.status_code >= 400 or resp_json.get("type")=="error": return {"type":"error", "description":resp_json.get("description",f"HTTP {r.status_code}"), "code":resp_json.get("code",r.status_code), "details":resp_json.get("result",resp_json)}
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
            '1505-json-full':'md_1505_full', '1510-json-full':'md_1510_full', 
            '1512-json-full':'md_1512_full'
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
        except Exception as e:
            log.error(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS connection failed: {e}", exc_info=True)
            return False

    def disconnect_xts(self):
        if self.xts_client and self.xts_client.connected:
            log.info(f"MDS_Adapter ({self.platform_user_id_ref}): Disconnecting from XTS WS.")
            self.xts_client.disconnect()
        else:
            log.info(f"MDS_Adapter ({self.platform_user_id_ref}): XTS WS already disconnected or not initialized.")

    def _fwd(self, fe, d, xe): 
        data_payload_orig = d 
        try:
            data_payload = d
            if isinstance(d, str):
                try:
                    data_payload = json.loads(d)
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
                ts = int(parts[0]); o, h, l, c = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]); v = int(parts[5])
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
        if not api_key or not api_secret: return render_template('broker_setup.html', error="API Key and Secret are required.", user=platform_user_id)
        
        user_broker_credentials_db[platform_user_id] = {"api_key": api_key, "api_secret": api_secret}
        
        temp_xts_client = XTSConnect(api_key, api_secret)
        xts_resp = temp_xts_client.marketdata_login()

        if xts_resp and xts_resp.get('type') == 'success' and temp_xts_client.token:
            user_xts_sessions[platform_user_id] = {"token": temp_xts_client.token, "userID": temp_xts_client.userID}
            session['xts_token'] = temp_xts_client.token
            session['xts_userID'] = temp_xts_client.userID
            session['xts_logged_in'] = True
            
            if platform_user_id in md_socket_clients_per_user:
                md_socket_clients_per_user[platform_user_id].disconnect_xts()
            
            md_user_client = MDSocket_io_ClientAdapter(
                temp_xts_client.token, 
                temp_xts_client.userID, 
                sio_server,
                platform_user_id
            )
            if md_user_client.connect_xts(): # connect_xts is a blocking call if not threaded
                md_socket_clients_per_user[platform_user_id] = md_user_client
                log.info(f"Successfully initiated XTS market data socket connection for user {platform_user_id}.")
            else:
                log.error(f"Failed to connect user {platform_user_id}'s XTS market data socket after broker login.")
            return redirect(url_for('chart_page_route'))
        else:
            desc = xts_resp.get('description', "XTS Login Failed") if isinstance(xts_resp, dict) else "XTS Login Failed"
            session['xts_logged_in'] = False
            if platform_user_id in user_xts_sessions: del user_xts_sessions[platform_user_id]
            return render_template('broker_setup.html', error=f"XTS Login Failed: {desc}", user=platform_user_id, creds_exist=True)

    existing_creds = user_broker_credentials_db.get(platform_user_id)
    xts_active = session.get('xts_logged_in', False) and \
                 (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token'))
    return render_template('broker_setup.html', user=platform_user_id, creds_exist=bool(existing_creds), xts_session_active=xts_active)

@flask_app.route('/chart')
def chart_page_route():
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id: return redirect(url_for('platform_login_route'))
    is_xts_setup_valid = user_broker_credentials_db.get(platform_user_id) and \
                         session.get('xts_logged_in', False) and \
                         (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token'))
    if not is_xts_setup_valid:
        log.warning(f"User {platform_user_id} attempting to access chart page without valid XTS session/setup. Redirecting to broker setup.")
        if not (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token')):
            session['xts_logged_in'] = False
        return redirect(url_for('broker_setup_page_route'))
    return render_template('chart_page.html', username=platform_user_id)

@flask_app.route('/api/download-master-contracts', methods=['POST'])
def download_master_contracts_api():
    if not session.get('platform_user_id') or not session.get('xts_logged_in'): return jsonify({"error":"Authentication required"}), 401
    client = get_current_user_xts_client_instance()
    if not client: return jsonify({"error":"Broker client unavailable or XTS session invalid"}), 500
    segs_req = request.json.get('segments',["NSECM","NSEFO","MCXFO"])
    results={}; create_dir_if_not_exists(MASTER_CONTRACT_DIR)
    for s_name in segs_req:
        r = client.get_master([s_name])
        if r and r.get('type')=='success' and r.get("contentType")=="text/plain":
            try:
                fpath=os.path.join(MASTER_CONTRACT_DIR,f"{s_name.upper()}.csv")
                dat=r['result']; hdr=""
                if "CM" in s_name.upper(): hdr="ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,displayName,ISIN,PriceNumerator,PriceDenominator\n"
                elif "FO" in s_name.upper() or "CUR" in s_name.upper(): hdr="ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,UnderlyingInstrumentId,UnderlyingIndexName,ContractExpiration,StrikePrice,OptionType,displayName,PriceNumerator,PriceDenominator\n"
                else: hdr="ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,displayName,ISIN,PriceNumerator,PriceDenominator\n" # Default general header
                with open(fpath,"w",encoding='utf-8') as f: f.write(hdr + dat.replace("|",","))
                results[s_name]={"type":"success","description":f"Saved {s_name}."}
            except Exception as e: results[s_name]={"type":"error","description":str(e)}; log.error(f"Err save {s_name}:{e}")
        else:
            desc=r.get('description','DL fail') if isinstance(r,dict) else 'DL fail'
            results[s_name]={"type":"error","description":desc,"details":r}; log.error(f"Fail DL {s_name}:{desc}")
    return jsonify(results)

@flask_app.route('/api/scrips', methods=['GET'])
def get_scrips_api():
    if not session.get('platform_user_id'): return jsonify({"error":"Not authenticated"}), 401
    all_scrips = []
    files_map = {"NSECM.CSV":"NSECM", "NSEFO.CSV":"NSEFO", "MCXFO.CSV":"MCXFO", "NSECUR.CSV":"NSECUR", "NSEIDX.CSV":"NSEIDX"} 
    for fname_upper, def_seg_key in files_map.items():
        fpath = os.path.join(MASTER_CONTRACT_DIR, fname_upper)
        if os.path.exists(fpath):
            try:
                with open(fpath, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines(); 
                    if len(lines) < 2: continue
                    header_line = lines[0].strip(); header = [h.strip().lower() for h in header_line.split(',')]
                    col_map = {'displayName': ['displayname', 'symbol', 'name', 'description', 'namewithseries'], 
                               'exchangeInstrumentID': ['exchangeinstrumentid', 'instrumentid', 'token'], 
                               'exchangeSegment': ['exchangesegment']}
                    indices = {}; missing_critical = False
                    for key, aliases in col_map.items():
                        found_idx = -1
                        for alias in aliases:
                            try: found_idx = header.index(alias); break
                            except ValueError: continue
                        if found_idx != -1: indices[key] = found_idx
                        elif key in ['displayName', 'exchangeInstrumentID']: missing_critical=True; log.warning(f"Critical column missing for {key} in {fname_upper}"); break
                    if missing_critical: continue
                    for l in lines[1:]:
                        p = [c.strip() for c in l.split(',')]
                        try:
                            if len(p) > max(indices.values()):
                                dispN = p[indices['displayName']]
                                idS = p[indices['exchangeInstrumentID']]
                                segV_csv = p[indices['exchangeSegment']].upper() if 'exchangeSegment' in indices and indices['exchangeSegment'] < len(p) and p[indices['exchangeSegment']] else def_seg_key
                                if dispN and idS: all_scrips.append({"displayName":dispN, "exchangeInstrumentID":idS, "exchangeSegment": segV_csv})
                        except IndexError: pass
            except Exception as e: log.error(f"Error processing {fpath}: {e}")
    if not all_scrips: all_scrips = [{"displayName":"NIFTY 50","exchangeInstrumentID":"26000","exchangeSegment":"NSEIDX"}] # Default
    else:
        seen=set(); unique_scrips=[]; 
        for s_obj in all_scrips: 
            key = f"{s_obj['exchangeSegment']}_{s_obj['exchangeInstrumentID']}"; 
            if key not in seen: unique_scrips.append(s_obj); seen.add(key)
        all_scrips = sorted(unique_scrips, key=lambda x: x["displayName"])
    return jsonify(all_scrips)

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
        return jsonify({"type":"error", "description":"All parameters required."}), 400

    timeframe_map = {"1D":(1440,730), "1H":(60,90), "15min":(15,30), "5min":(5,15), "1min":(1,7)}
    config = timeframe_map.get(tf)
    if not config: return jsonify({"type":"error", "description":f"Unsupported timeframe: {tf}"}), 400
    compV, daysLB = int(config[0]), int(config[1])
    seg_code = XTS_SEGMENT_CODE_MAP.get(seg_name.upper())
    if seg_code is None:
        return jsonify({"type": "error", "description": f"Invalid exchange segment name: {seg_name}"}), 400

    endT = datetime.now(); startT_dt = endT - timedelta(days=daysLB)
    startT_str = startT_dt.strftime("%b %d %Y %H%M"); endT_str = endT.strftime("%b %d %Y %H%M")
    
    log.info(f"Fetching OHLC for {sName} (SegCode:{seg_code}, ID:{iid}) TF:{tf} ({compV}m) from {startT_str} to {endT_str}")
    ohlcResp = client.get_ohlc(seg_code, iid, startT_str, endT_str, compV)

    if ohlcResp and ohlcResp.get("type") == "success" and "result" in ohlcResp:
        ohlcTxt = ohlcResp["result"].get("dataReponse", "")
        if not ohlcTxt:
            log.info(f"Empty dataReponse from API for {sName} ({seg_name}:{iid}). OHLC Response: {ohlcResp}")
            return jsonify([]) 
        formatted_data = format_ohlc_data_for_frontend(ohlcTxt, compV) 
        return jsonify(formatted_data)
    else:
        desc = ohlcResp.get('description', 'OHLC fetch failed') if isinstance(ohlcResp, dict) else 'OHLC API error'
        status_code_val = ohlcResp.get("code", 500) if isinstance(ohlcResp, dict) else 500
        try: status_code_val = int(status_code_val)
        except ValueError: status_code_val = 500 
        log.error(f"OHLC API error for {sName} ({seg_name}:{iid}): {desc}. Details: {ohlcResp}")
        return jsonify({"type":"error", "description":desc, "details":ohlcResp}), status_code_val

# --- Socket.IO Event Handlers ---
@sio_server.event
def connect(sid, environ):
    with flask_app.request_context(environ):
        try:
            platform_user_id = session.get('platform_user_id')
            xts_session_active = platform_user_id and \
                                 session.get('xts_logged_in', False) and \
                                 (platform_user_id in user_xts_sessions and user_xts_sessions[platform_user_id].get('token'))
            log.info(f"Flask WS Client connect attempt: SID {sid}, User: {platform_user_id}, XTS Active: {xts_session_active}")
            if not xts_session_active:
                log.warning(f"SocketIO connect REJECTED for SID {sid}: User not authenticated or XTS session invalid.")
                return False
            sio_server.save_session(sid, {
                'platform_user_id': platform_user_id,
                'xts_logged_in': True,
                'xts_broker_userID': user_xts_sessions[platform_user_id].get('userID') 
            })
            log.info(f"Stored platform_user_id '{platform_user_id}' in Socket.IO session for SID {sid}")
            sio_server.emit('server_message', {'data': f'Welcome {platform_user_id}! WebSocket connected.'}, room=sid)
            return True
        except Exception as e:
            log.error(f"Exception in Socket.IO connect handler for SID {sid}: {e}", exc_info=True)
            return False

@sio_server.event
def disconnect(sid):
    sio_session_data = sio_server.get_session(sid)
    platform_user_id = sio_session_data.get('platform_user_id', 'Unknown user')
    log.info(f"Flask WS Client disconnect: SID {sid}, Platform User: {platform_user_id}")

@sio_server.on('subscribe_instrument')
def handle_subscribe_instrument(sid, data):
    sio_session_data = sio_server.get_session(sid)
    platform_user_id = sio_session_data.get('platform_user_id')
    xts_is_logged_in = sio_session_data.get('xts_logged_in', False)

    if not platform_user_id or not xts_is_logged_in:
        sio_server.emit('subscription_error',{'error':'Authentication required.'},room=sid); return

    api_client = get_xts_client_by_platform_id(platform_user_id) # HTTP API client for sending subscription requests to XTS
    if not api_client:
        sio_server.emit('subscription_error',{'error':'Broker client unavailable.'},room=sid); return
    
    try:
        segment_name = data['exchangeSegment']
        instrument_id = int(data['exchangeInstrumentID'])
        segment_code = XTS_SEGMENT_CODE_MAP.get(segment_name.upper())

        if segment_code is None:
            sio_server.emit('subscription_error', {'error': f"Unknown segment: {segment_name}"}, room=sid); return

        room_name = f"instrument_{segment_code}_{instrument_id}"
        sio_server.enter_room(sid, room_name) 
        log.info(f"SID {sid} (User: {platform_user_id}) entered room {room_name} for {segment_name}:{instrument_id}")

        xts_inst_payload = [{'exchangeSegment': segment_code, 'exchangeInstrumentID': instrument_id}]
        codes_to_subscribe = [1501, 1502] 
        
        all_ok, responses = True, {}
        md_feed_client = md_socket_clients_per_user.get(platform_user_id)
        if not md_feed_client or not md_feed_client.xts_client.connected:
            log.warning(f"User {platform_user_id}'s XTS Market Data socket (MDSocket_io_ClientAdapter) not connected. Live data may not flow.")
            # Optionally try to reconnect md_feed_client here if desired
        
        for code in codes_to_subscribe:
            r = api_client.send_subscription(xts_inst_payload, code=code) # Use HTTP API client
            responses[str(code)] = r
            if not (r and r.get('type')=='success'): 
                # XTS often returns "Instrument Already Subscribed !" which is type 'error' but not critical.
                if r.get('code') == 'e-session-0002' and "Already Subscribed" in r.get('description',''):
                    log.info(f"XTS info for {platform_user_id}, {xts_inst_payload}, code {code}: {r.get('description')}")
                else:
                    all_ok = False; 
                    log.error(f"XTS Subscription API Error for {platform_user_id}, {xts_inst_payload}, code {code}: {r}")
        
        # Ack is sent even if some are "already subscribed" as long as no other critical errors.
        # If `all_ok` is false due to a real error, then an error is emitted.
        if all_ok: 
            sio_server.emit('subscription_ack', {'instrument': data, 'room': room_name, 'message': f'Subscription request processed for {data.get("displayName", instrument_id)}.'}, room=sid)
        else: 
            sio_server.emit('subscription_error',{'error':'XTS subscription to one or more services failed.','details':responses}, room=sid)

    except Exception as e:
        log.error(f"Error in handle_subscribe_instrument (SID {sid}): {e}", exc_info=True)
        sio_server.emit('subscription_error', {'error': 'Server error during subscription.'}, room=sid)

@sio_server.on('unsubscribe_instrument')
def handle_unsubscribe_instrument(sid, data):
    sio_session_data = sio_server.get_session(sid)
    platform_user_id = sio_session_data.get('platform_user_id')
    if not platform_user_id: sio_server.emit('unsubscription_error',{'error':'Not authenticated.'},room=sid); return

    api_client = get_xts_client_by_platform_id(platform_user_id) # HTTP API client
    if not api_client: sio_server.emit('unsubscription_error',{'error':'Broker client unavailable.'},room=sid); return

    try:
        segment_name = data['exchangeSegment']
        instrument_id = int(data['exchangeInstrumentID'])
        segment_code = XTS_SEGMENT_CODE_MAP.get(segment_name.upper())
        if segment_code is None: sio_server.emit('unsubscription_error', {'error': f"Unknown segment: {segment_name}"}, room=sid); return
        
        room_name = f"instrument_{segment_code}_{instrument_id}"
        sio_server.leave_room(sid, room_name)
        log.info(f"SID {sid} (User: {platform_user_id}) left room {room_name}")
        
        # TODO: Implement robust XTS unsubscription when room is empty
        # This requires checking if any other SIDs are in `room_name`.
        # Example: active_sids_in_room = sio_server.rooms().get(room_name)
        # if not active_sids_in_room or sid not in active_sids_in_room: # Check if this sid was the last one for this room.
        # Note: sio_server.rooms() structure needs verification for flask-socketio.
        # A simpler way is to track subscriptions per room in a global dict.
        
        # For now, we are not sending unsubscription to XTS to keep it simple
        # as multiple users/tabs might be subscribed to the same instrument.
        # Proper unsubscription requires careful state management.

        sio_server.emit('unsubscription_ack', {'instrument': data, 'room': room_name, 'message': 'Left instrument room.'}, room=sid)
    except Exception as e:
        log.error(f"Error in handle_unsubscribe_instrument (SID {sid}): {e}", exc_info=True)
        sio_server.emit('unsubscription_error', {'error': 'Server error during unsubscription.'}, room=sid)

# --- Server Startup ---
if __name__ == '__main__':
    create_dir_if_not_exists(MASTER_CONTRACT_DIR)
    log.info(f"Starting Flask app with SocketIO. Config:'{CONFIG_FILE_NAME}'")
    try:
        run_simple('127.0.0.1', 5000, application,
                   use_reloader=False, use_debugger=True, threaded=True)
    except Exception as e:
        log.critical(f"Failed to start Werkzeug development server: {e}", exc_info=True)