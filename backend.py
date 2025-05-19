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
from flask import Flask, jsonify, request, session, redirect, url_for, render_template
from flask_cors import CORS
from six.moves.urllib.parse import urljoin
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.serving import run_simple # Import run_simple at the top

# --- Configuration & Globals --- (Same as before)
CONFIG_FILE_NAME = 'config.ini'
MASTER_CONTRACT_DIR = "csv"
xts_api_client_global = None
md_socket_client_global = None
users_db = {} 
user_broker_credentials_db = {}
user_xts_sessions = {}

# Initialize Flask app instance
flask_app = Flask(__name__) # Use a distinct name for the Flask instance
flask_app.secret_key = os.urandom(32)
CORS(flask_app) # Apply CORS to the Flask instance

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)
log = logging.getLogger(__name__)

# --- XTS API Interaction Classes (XTSCommon, XTSConnect) ---
# ... (Keep your existing XTSCommon and XTSConnect class definitions exactly as they were) ...
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
        return self._get('market.instruments.ohlc', {'exchangeSegment':seg, 'exchangeInstrumentID':iid, 'startTime':start, 'endTime':end, 'compressionValue':comp_val})
    def send_subscription(self, insts, code=1501):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (send_subscription)"}
        return self._post('market.instruments.subscription', {'instruments':insts, 'xtsMessageCode':code})
    def send_unsubscription(self, insts, code=1501):
        if not self.token: return {"type":"error", "description":"XTS Token Missing (send_unsubscription)"}
        return self._put('market.instruments.unsubscription', {'instruments':insts, 'xtsMessageCode':code})


# --- Market Data Socket Adapter (MDSocket_io_ClientAdapter) ---
# ... (Keep your existing MDSocket_io_ClientAdapter class definition exactly as it was) ...
class MDSocket_io_ClientAdapter:
    def __init__(self, token, user_id, flask_sio_server_instance): # Renamed arg for clarity
        self.token, self.user_id, self.flask_sio = token, user_id, flask_sio_server_instance
        cfg = configparser.ConfigParser(); cfg.read(CONFIG_FILE_NAME)
        root_uri = cfg.get('root_url', 'root', fallback=XTSConnect._default_root_uri)
        pub_fmt = cfg.get('root_url', 'publishformat', fallback='JSON')
        bc_mode = cfg.get('root_url', 'broadcastmode', fallback='Full')
        self.conn_url = f"{root_uri}/?token={token}&userID={user_id}&publishFormat={pub_fmt}&broadcastMode={bc_mode}"
        self.xts_client = socketio.Client(logger=False, engineio_logger=False, reconnection_attempts=3, reconnection_delay=5)
        self._reg_handlers()
    def _reg_handlers(self):
        client = self.xts_client; client.on('connect', self._on_xts_connect); client.on('disconnect', self._on_xts_disconnect); client.on('error', self._on_xts_error); client.on('connect_error', self._on_xts_conn_err)
        ev_map = {'1501-json-full':'md_1501_full', '1502-json-full':'md_1502_full', '1505-json-full':'md_1505_full', '1510-json-full':'md_1510_full', '1512-json-full':'md_1512_full'}
        for xts_ev, flask_ev_suffix in ev_map.items(): client.on(xts_ev, lambda d, fe_sfx=flask_ev_suffix, xe=xts_ev: self._fwd(f"market_data_{fe_sfx}", d, xe))
    def connect_xts(self, path='/apimarketdata/socket.io'):
        if not self.token: return False
        try: self.xts_client.connect(self.conn_url, transports=['websocket'], socketio_path=path); return True
        except Exception: return False
    def disconnect_xts(self):
        if self.xts_client.connected: self.xts_client.disconnect()
    def _fwd(self, fe, d, xe): self.flask_sio.emit(fe,d)
    def _on_xts_connect(self): log.info(f"MDS_Adapter: XTS WS Connected for {self.user_id}! SID:{self.xts_client.sid}")
    def _on_xts_disconnect(self): log.info(f"MDS_Adapter: XTS WS Disconnected for {self.user_id}!")
    def _on_xts_error(self,d): log.error(f"MDS_Adapter: XTS WS Error for {self.user_id}: {d}")
    def _on_xts_conn_err(self,d): log.error(f"MDS_Adapter: XTS WS ConnectError for {self.user_id}: {d}")

# --- Socket.IO Server Instance ---
# This is the python-socketio Server instance
sio_server = socketio.Server(async_mode='threading', cors_allowed_origins="*")

# --- WSGI Application Composition ---
# Create a new WSGI app that combines the Socket.IO server with the Flask app
# The Flask app instance (flask_app) will handle standard HTTP requests.
# Socket.IO requests will be handled by sio_server.
application = socketio.WSGIApp(sio_server, flask_app)


# --- Utility Functions ---
def create_dir_if_not_exists(d): # ... (Same as before)
    if not os.path.exists(d): os.makedirs(d); log.info(f"Created dir: {d}")

def get_current_user_xts_client_instance(): # ... (Same as before)
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id or not session.get('xts_logged_in'): return None    
    creds = user_broker_credentials_db.get(platform_user_id)
    xts_session_data = user_xts_sessions.get(platform_user_id)
    if creds and xts_session_data:
        return XTSConnect(api_key=creds['api_key'], secret_key=creds['api_secret'], token=xts_session_data['token'], userID=xts_session_data['userID'])
    log.warning(f"Could not get XTS client for {platform_user_id}. Broker creds or XTS session data missing.")
    return None

def format_ohlc_data_for_frontend(ohlc_text): # ... (Same as before - the ETL version)
    raw_parsed_points = []
    if not ohlc_text: return []
    data_point_strings = ohlc_text.split(',')
    if not data_point_strings or (len(data_point_strings) == 1 and not data_point_strings[0].strip()): return []
    for i, point_str in enumerate(data_point_strings):
        if not point_str.strip(): continue
        parts = point_str.split('|')
        if len(parts) >= 6:
            try:
                ts = int(parts[0]); o, h, l, c = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]); v = int(parts[5])
                if not all(math.isfinite(val) for val in [ts, o, h, l, c, v]): continue
                if v < 0: v = 0 
                raw_parsed_points.append({"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
            except ValueError: pass
        else: pass    
    if not raw_parsed_points: return []
    raw_parsed_points.sort(key=lambda x: x["time"])
    validated_data = []; last_timestamp = -1
    for point in raw_parsed_points:
        if point["high"] < point["low"]: continue
        if point["time"] <= last_timestamp: continue
        validated_data.append(point); last_timestamp = point["time"]
    log.info(f"format_ohlc_data: Initial {len(data_point_strings)}, parsed {len(raw_parsed_points)}, final validated {len(validated_data)}.")
    return validated_data

# --- Flask Routes (Decorators now use 'flask_app') ---
@flask_app.route('/')
def landing_page_route(): # ... (Same as before)
    if session.get('platform_user_id'):
        if user_broker_credentials_db.get(session['platform_user_id']) and session.get('xts_logged_in'):
            return redirect(url_for('chart_page_route'))
        return redirect(url_for('broker_setup_page_route'))
    return render_template('landing.html')

@flask_app.route('/signup', methods=['GET', 'POST'])
def platform_signup_route(): # ... (Same as before)
    if request.method == 'POST':
        username = request.form.get('username'); password = request.form.get('password')
        if not username or not password: return render_template('platform_signup.html', error="Username and password are required.")
        if username in users_db: return render_template('platform_signup.html', error="Username already exists.")
        users_db[username] = generate_password_hash(password)
        session['platform_user_id'] = username; session['logged_in'] = True
        return redirect(url_for('broker_setup_page_route'))
    return render_template('platform_signup.html')

@flask_app.route('/login', methods=['GET', 'POST']) # Platform Login
def platform_login_route(): # ... (Same as before)
    if request.method == 'POST':
        username = request.form.get('username'); password = request.form.get('password')
        user_hash = users_db.get(username)
        if user_hash and check_password_hash(user_hash, password):
            session['platform_user_id'] = username; session['logged_in'] = True
            if user_broker_credentials_db.get(username) and user_xts_sessions.get(username):
                 session['xts_logged_in'] = True
                 return redirect(url_for('chart_page_route'))
            return redirect(url_for('broker_setup_page_route'))
        else: return render_template('platform_login.html', error="Invalid username or password.")
    return render_template('platform_login.html')

@flask_app.route('/logout', methods=['POST']) # Platform Logout
def platform_logout_route(): # ... (Same as before, ensure md_socket_client_global is handled)
    global md_socket_client_global
    platform_user_id = session.get('platform_user_id')
    if platform_user_id and platform_user_id in user_xts_sessions: del user_xts_sessions[platform_user_id]
    # Simplified global md_socket_client handling for logout
    if md_socket_client_global and md_socket_client_global.xts_client.connected:
        md_socket_client_global.disconnect_xts()
    md_socket_client_global = None
    session.clear()
    return redirect(url_for('landing_page_route'))

@flask_app.route('/broker_setup', methods=['GET', 'POST'])
def broker_setup_page_route(): # ... (Same as before, ensure md_socket_client_global is handled)
    global xts_api_client_global, md_socket_client_global
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
            session['xts_token'] = temp_xts_client.token; session['xts_userID'] = temp_xts_client.userID
            session['xts_logged_in'] = True; session['xts_api_key_used'] = api_key; session['xts_secret_key_used'] = api_secret
            xts_api_client_global = temp_xts_client
            if md_socket_client_global and md_socket_client_global.xts_client.connected:
                if md_socket_client_global.user_id != temp_xts_client.userID or md_socket_client_global.token != temp_xts_client.token:
                    md_socket_client_global.disconnect_xts()
                    md_socket_client_global = MDSocket_io_ClientAdapter(temp_xts_client.token, temp_xts_client.userID, sio_server)
                    threading.Thread(target=md_socket_client_global.connect_xts, daemon=True).start()
            else:
                md_socket_client_global = MDSocket_io_ClientAdapter(temp_xts_client.token, temp_xts_client.userID, sio_server)
                threading.Thread(target=md_socket_client_global.connect_xts, daemon=True).start()
            return redirect(url_for('chart_page_route'))
        else:
            desc = xts_resp.get('description', "XTS Login Failed") if isinstance(xts_resp, dict) else "XTS Login Failed"
            session['xts_logged_in'] = False
            return render_template('broker_setup.html', error=f"XTS Login Failed: {desc}", user=platform_user_id, creds_exist=True)
    existing_creds = user_broker_credentials_db.get(platform_user_id)
    xts_active = session.get('xts_logged_in', False) and (platform_user_id in user_xts_sessions)
    return render_template('broker_setup.html', user=platform_user_id, creds_exist=bool(existing_creds), xts_session_active=xts_active)

@flask_app.route('/chart') # Chart Page
def chart_page_route(): # ... (Same protection logic as before)
    if not session.get('platform_user_id'): return redirect(url_for('platform_login_route'))
    if not user_broker_credentials_db.get(session['platform_user_id']) or not session.get('xts_logged_in'):
        return redirect(url_for('broker_setup_page_route'))
    return render_template('chart_page.html', username=session['platform_user_id'])

# --- Protected Data API Endpoints (use @flask_app.route) ---
@flask_app.route('/api/download-master-contracts', methods=['POST'])
def download_master_contracts_api(): # ... (Same as before, using get_current_user_xts_client_instance)
    if not session.get('platform_user_id') or not session.get('xts_logged_in'): return jsonify({"error":"Authentication required"}), 401
    client = get_current_user_xts_client_instance()
    if not client: return jsonify({"error":"Broker client unavailable"}), 500
    # (Your existing download logic using 'client.get_master')
    segs=request.json.get('segments',["NSECM","NSEFO","MCXFO"]); results={}; create_dir_if_not_exists(MASTER_CONTRACT_DIR)
    for s in segs:
        r=client.get_master([s]) 
        if r and r.get('type')=='success' and r.get("contentType")=="text/plain":
            try:
                fpath=os.path.join(MASTER_CONTRACT_DIR,f"{s}.csv"); dat=r['result']; hdr=""
                if "CM" in s.upper(): hdr="ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,displayName,ISIN,PriceNumerator,PriceDenominator\n"
                elif "FO" in s.upper(): hdr="ExchangeSegment,ExchangeInstrumentID,InstrumentType,Name,Description,Series,NameWithSeries,InstrumentID,PriceBand.High,PriceBand.Low,FreezeQty,TickSize,LotSize,Multiplier,UnderlyingInstrumentId,UnderlyingIndexName,ContractExpiration,StrikePrice,OptionType,displayName,PriceNumerator,PriceDenominator\n"
                with open(fpath,"w",encoding='utf-8') as f: f.write(hdr + dat.replace("|",","))
                results[s]={"type":"success","description":f"Saved {s}."}
            except Exception as e: results[s]={"type":"error","description":str(e)}; log.error(f"Err save {s}:{e}")
        else:
            desc=r.get('description','DL fail') if isinstance(r,dict) else 'DL fail'
            results[s]={"type":"error","description":desc,"details":r}; log.error(f"Fail DL {s}:{desc}")
    return jsonify(results)

@flask_app.route('/api/scrips', methods=['GET'])
def get_scrips_api(): # ... (Same as before)
    if not session.get('platform_user_id'): return jsonify({"error":"Not authenticated"}), 401
    # (Your existing get_scrips logic from CSVs)
    all_scrips, files_map = [], {"NSECM.csv":"NSECM", "NSEFO.csv":"NSEFO", "MCXFO.csv":"MCXFO"}
    for fname, def_seg_key in files_map.items():
        fpath = os.path.join(MASTER_CONTRACT_DIR, fname)
        if os.path.exists(fpath):
            try:
                with open(fpath, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines(); 
                    if len(lines) < 2: continue
                    header_line = lines[0].strip(); header = [h.strip().lower() for h in header_line.split(',')]
                    col_map = {'displayName': ['displayname', 'symbol', 'name', 'description', 'nameswithseries'], 'exchangeInstrumentID': ['exchangeinstrumentid', 'instrumentid', 'token'], 'exchangeSegment': ['exchangesegment', 'segment']}
                    indices = {}; missing_critical = False
                    for key, aliases in col_map.items():
                        found_idx = -1
                        for alias in aliases:
                            try: found_idx = header.index(alias); break
                            except ValueError: continue
                        if found_idx != -1: indices[key] = found_idx
                        else: missing_critical=True; break
                    if missing_critical: continue
                    for i, l in enumerate(lines[1:]):
                        p = [c.strip() for c in l.split(',')];
                        try:
                            if len(p) > max(indices.values()):
                                dispN = p[indices['displayName']]; idS = p[indices['exchangeInstrumentID']]; segV_csv = p[indices['exchangeSegment']] if indices['exchangeSegment'] < len(p) else None
                                if dispN and idS: all_scrips.append({"displayName":dispN, "exchangeInstrumentID":idS, "exchangeSegment": segV_csv or def_seg_key})
                        except Exception: pass
            except Exception as e: log.error(f"Error processing {fpath}: {e}")
    if not all_scrips: all_scrips = [{"displayName":"NIFTY 50","exchangeInstrumentID":"26000","exchangeSegment":"NSEIDX"}]
    else:
        seen=set(); unique_scrips=[]; 
        for s_obj in all_scrips: key = f"{s_obj['exchangeSegment']}_{s_obj['exchangeInstrumentID']}"; _ = not (key in seen or seen.add(key)) and unique_scrips.append(s_obj)
        all_scrips = sorted(unique_scrips, key=lambda x: x["displayName"])
    log.info(f"Total unique scrips for /api/scrips: {len(all_scrips)}")
    return jsonify(all_scrips)

@flask_app.route('/api/historical-data', methods=['GET'])
def get_historical_data_api(): # ... (Same as before, using get_current_user_xts_client_instance and new format_ohlc_data_for_frontend)
    if not session.get('platform_user_id') or not session.get('xts_logged_in'): return jsonify({"type":"error","description":"Authentication required."}), 401
    client = get_current_user_xts_client_instance()
    if not client: return jsonify({"type":"error","description":"Broker client not available."}), 500
    sName, tf, seg, iid = request.args.get('scrip'), request.args.get('timeframe'), request.args.get('exchangeSegment'), request.args.get('exchangeInstrumentID')
    if not (sName and tf and seg and iid): return jsonify({"type":"error", "description":"All parameters required."}), 400
    timeframe_map = {"1D":(1440,730), "1H":(60,90), "15min":(15,30), "5min":(5,15), "1min":(1,7)}
    config = timeframe_map.get(tf)
    if not config: return jsonify({"type":"error", "description":f"Unsupported TF: {tf}"}), 400
    compV, daysLB = int(config[0]), int(config[1])
    endT = datetime.now(); startT_dt = endT - timedelta(days=daysLB)
    startT_str = startT_dt.strftime("%b %d %Y %H%M"); endT_str = endT.strftime("%b %d %Y %H%M")
    ohlcResp = client.get_ohlc(seg, iid, startT_str, endT_str, compV)
    if ohlcResp and ohlcResp.get("type")=="success" and "result" in ohlcResp:
        ohlcTxt = ohlcResp["result"].get("dataReponse","")
        formatted_data = format_ohlc_data_for_frontend(ohlcTxt)
        return jsonify(formatted_data)
    else:
        desc = ohlcResp.get('description','OHLC fetch failed') if isinstance(ohlcResp,dict) else 'OHLC error'
        return jsonify({"type":"error", "description":desc, "details":ohlcResp}), 500


# --- Socket.IO Event Handlers (now attach to 'sio_server') ---
@sio_server.event
def connect(sid, environ):
    platform_user_id = session.get('platform_user_id') # Flask session should be accessible
    log.info(f"Flask WS Client connect: SID {sid}, Platform User: {platform_user_id}")
    if not platform_user_id:
        log.warning(f"SocketIO connect from SID {sid} without platform_user_id in session. Auth needed.")
        return False # Reject connection if no platform user
    sio_server.emit('server_message', {'data': f'Welcome {platform_user_id}! SID: {sid}'}, room=sid)

@sio_server.event
def disconnect(sid):
    log.info(f"Flask WS Client disconnect: SID {sid}")

@sio_server.on('subscribe_instrument')
def handle_subscribe_instrument(sid, data):
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id or not session.get('xts_logged_in'):
        sio_server.emit('subscription_error',{'error':'Authentication required for subscription.'},room=sid); return

    client = get_current_user_xts_client_instance() # Get XTS client for this user
    if not client or not client.token:
        sio_server.emit('subscription_error',{'error':'Broker client/token error.'},room=sid); return
    
    log.info(f"User {platform_user_id} (SID {sid}) sub request: {data}")
    # ... (rest of your subscription logic, using 'client' instance)
    inst = [{'exchangeSegment':data['exchangeSegment'], 'exchangeInstrumentID':int(data['exchangeInstrumentID'])}]
    codes_to_subscribe = [1501,1505,1512,1510]; all_ok = True; responses = {}
    for c_msg_code in codes_to_subscribe:
        r = client.send_subscription(inst, code=c_msg_code)
        responses[str(c_msg_code)] = r
        if not (r and r.get('type')=='success'): all_ok = False
    if all_ok: sio_server.emit('subscription_ack', {'instrument':inst[0]},room=sid)
    else: sio_server.emit('subscription_error',{'error':'One+ XTS sub reqs failed.','details':responses},room=sid)


@sio_server.on('unsubscribe_instrument')
def handle_unsubscribe_instrument(sid, data):
    # Similar checks and client retrieval as subscribe
    platform_user_id = session.get('platform_user_id')
    if not platform_user_id or not session.get('xts_logged_in'):
        sio_server.emit('unsubscription_error',{'error':'Authentication required.'},room=sid); return
    client = get_current_user_xts_client_instance()
    if not client or not client.token:
        sio_server.emit('unsubscription_error',{'error':'Broker client/token error.'},room=sid); return
    # ... (rest of your unsubscription logic using 'client')


# --- Server Startup ---
if __name__ == '__main__':
    create_dir_if_not_exists(MASTER_CONTRACT_DIR)
    log.info(f"Starting Flask app with SocketIO. Config:'{CONFIG_FILE_NAME}'")
    try:
        # Run the combined_wsgi_app using Werkzeug's development server
        run_simple('127.0.0.1', 5000, application, # Use the 'application' (combined app)
                   use_reloader=False, use_debugger=True, threaded=True)
    except Exception as e:
        log.critical(f"Failed to start Werkzeug development server: {e}", exc_info=True)