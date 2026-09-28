"""
PS Internal Ops Dashboard — Backend Server
Run: python3 server.py
Serves the dashboard on http://localhost:8080 and handles real SQS dispatch.
Requires: pip install flask boto3 flask-cors authlib python-dotenv

Login is via Google Workspace SSO (Sign in with Google), restricted to the
@bidgely.com domain. Set these before running — easiest via a .env file in
this same directory (loaded automatically at startup):
    GOOGLE_CLIENT_ID       - OAuth 2.0 Client ID from Google Cloud Console
    GOOGLE_CLIENT_SECRET   - OAuth 2.0 Client Secret
    FLASK_SECRET_KEY       - any long random string, used to sign the session cookie
    HER_REVOKE_ACCESS_TOKEN - OPTIONAL fallback access token for the notifications-revoke
                              API used by the HER Revocation tab. This token is shared
                              across the team and regenerated fresh every day, so day to
                              day it's set from the tab itself (Admin role, "Revoke Access
                              Token" card) — that's stored in her_revocation.py's
                              .her_revoke_token.json, not here. This env var only matters
                              on a fresh install before anyone has pasted a token yet.

Example .env file (create as `.env` next to server.py):
    GOOGLE_CLIENT_ID=123456789-abc.apps.googleusercontent.com
    GOOGLE_CLIENT_SECRET=GOCSPX-xxxxxxxxxxxxxxxx
    FLASK_SECRET_KEY=some-long-random-string

Setup (one-time, in Google Cloud Console):
  1. APIs & Services -> Credentials -> Create Credentials -> OAuth client ID
  2. Application type: Web application
  3. Authorized redirect URI: http://localhost:8080/auth/callback
     (add your real deployed URL's /auth/callback too, once hosted elsewhere)
  4. Copy the Client ID and Client Secret into the .env file above
  5. On the OAuth consent screen, restrict to Internal / your Workspace org if prompted

Testing locally without Google OAuth set up: set OPSHUB_LOCAL_NO_AUTH=1 in your .env
to skip SSO entirely — every request is then treated as a fixed local admin user
(override with OPSHUB_LOCAL_USER_EMAIL / _NAME / _ROLE). Never set this anywhere
other than your own machine — there is no login screen to get past once it's on.
"""

from flask import Flask, request, jsonify, send_from_directory, session, redirect, url_for
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix
from functools import wraps
import boto3
from botocore.exceptions import ClientError
import os
import json
import time
import logging
import secrets as pysecrets
import threading
import uuid
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

# Load variables from a .env file sitting next to this script, if one exists.
# This means GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / FLASK_SECRET_KEY no longer
# depend on which terminal tab/session you happen to be running `python3 server.py`
# from — export in your shell still works too and takes priority if both are set.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # falls back to whatever's already in the shell environment (or nothing)

# HER Revocation lives in its own script (her_revocation.py, next to this file) —
# it's a standalone module you can also run by hand from the command line, and
# this server just imports it rather than duplicating its logic inline.
from her_revocation import (
    REVOKE_API_CONFIG, get_env_token as get_her_revoke_token, revoke_many,
    get_token_status, set_token as set_her_revoke_token,
    record_audit_entry as record_her_audit_entry, get_audit_log as get_her_audit_log,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')
CORS(app, supports_credentials=True)

# When this app sits behind a reverse proxy / Kubernetes Ingress / load balancer
# that terminates TLS (public URL is https://, but Flask itself only ever sees
# plain http:// traffic internally), url_for(..., _external=True) would otherwise
# build the OAuth redirect URI with the WRONG scheme — causing a
# "redirect_uri_mismatch" even when the right URL is registered in Google Cloud
# Console. ProxyFix makes Flask trust the X-Forwarded-* headers the proxy sets,
# so it correctly reconstructs https://ops-hub.bidgely.com/auth/callback etc.
# x_proto=1 / x_host=1 means "trust one hop" — set higher only if you have
# multiple chained proxies in front of this app.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Session cookie signing key — required for Flask sessions to work.
# Falls back to a random key generated at process start (fine for local/dev use,
# but means every server restart invalidates existing sessions — set
# FLASK_SECRET_KEY explicitly for anything longer-lived than local testing).
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or pysecrets.token_hex(32)

# Only mark the session cookie "Secure" (HTTPS-only) when actually served over
# HTTPS — browsers silently drop Secure cookies sent over plain http://, which
# would break local http://localhost:8080 testing if this were always on.
# Set SESSION_COOKIE_SECURE=true in the hosted environment's env/Secret.
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', 'false').lower() == 'true'
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # needed for the Google OAuth redirect flow to carry the session cookie back

# ─── GOOGLE SSO CONFIG ───────────────────────────────────────────
ALLOWED_DOMAIN = 'bidgely.com'

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'},
)

# ─── ROLE GROUPS ─────────────────────────────────────────────────
# Membership here — not a password — is what determines access.
# Anyone at @bidgely.com not listed below still gets in, but only as 'delivery'.
ADMIN_EMAILS = {
    'ranjeetverma@bidgely.com',
    'rkamat@bidgely.com',
    'jithin@bidgely.com',
    'muniyaswanth@bidgely.com',
}
PS_EMAILS = {
    'atul@bidgely.com',
    'ayush@bidgely.com',
    'chaitanya@bidgely.com',
    'darshit@bidgely.com',
    'gtarasia@bidgely.com',
    'hemant@bidgely.com',
    'pshetty@bidgely.com',
    'vishnuj@bidgely.com',
}

def resolve_role(email):
    email = (email or '').lower()
    if email in ADMIN_EMAILS:
        return 'admin'
    if email in PS_EMAILS:
        return 'user'
    return 'delivery'


# ─── LOCAL DEV: optional auth bypass ─────────────────────────────
# Set OPSHUB_LOCAL_NO_AUTH=1 to skip Google SSO entirely and treat every
# request as a fixed local user — for running this on your own machine
# without setting up a Google OAuth client. NEVER set this anywhere other
# than your own laptop; it is off by default and there is no login screen
# to get past once it's on. Role defaults to 'admin' so every tab (incl.
# HER Revocation) is visible; override with OPSHUB_LOCAL_USER_ROLE if you
# want to test as 'user' or 'delivery' instead.
LOCAL_NO_AUTH = os.environ.get('OPSHUB_LOCAL_NO_AUTH', '').strip().lower() in ('1', 'true', 'yes')
LOCAL_DEV_USER = {
    'email': os.environ.get('OPSHUB_LOCAL_USER_EMAIL', 'local-dev@bidgely.com'),
    'name':  os.environ.get('OPSHUB_LOCAL_USER_NAME', 'Local Dev'),
    'role':  os.environ.get('OPSHUB_LOCAL_USER_ROLE', 'admin'),
}


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if LOCAL_NO_AUTH:
            session.setdefault('user', LOCAL_DEV_USER)
            return f(*args, **kwargs)
        if not session.get('user'):
            return jsonify({'status': 'error', 'message': 'Not logged in'}), 401
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if LOCAL_NO_AUTH:
            session.setdefault('user', LOCAL_DEV_USER)
        if not session.get('user'):
            return jsonify({'status': 'error', 'message': 'Not logged in'}), 401
        if session['user']['role'] != 'admin':
            return jsonify({'status': 'error', 'message': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return wrapper


def admin_or_ps_required(f):
    """Viewing the shared Run History is open to admin + PS (user role) —
    matches ROLE_CONFIG on the frontend. Destructive actions (clearing it)
    stay behind admin_required instead."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if LOCAL_NO_AUTH:
            session.setdefault('user', LOCAL_DEV_USER)
        if not session.get('user'):
            return jsonify({'status': 'error', 'message': 'Not logged in'}), 401
        if session['user']['role'] not in ('admin', 'user'):
            return jsonify({'status': 'error', 'message': 'Access required'}), 403
        return f(*args, **kwargs)
    return wrapper


# ─── SHARED RUN LOG ──────────────────────────────────────────────
# This used to live in each browser's localStorage (nobody could see anyone
# else's runs), then briefly on the container's local disk (lost on every
# pod restart/reschedule, and not shared across replicas). Both problems are
# solved by storing it in S3 instead — the same bucket/pattern already used
# for the BC Schedule Tracker proxy, just a different key.
#
# Remaining caveat, worth knowing: every write here is a full read-modify-write
# (GET the whole file, append, PUT it back), guarded by a lock that only
# protects THIS process — if you ever scale to more than one replica, two
# pods writing at nearly the same moment could still race and one write could
# clobber the other. Fine for a single-instance internal tool; if this needs
# to be safe under real concurrent writers, move to DynamoDB (or S3 with
# conditional writes) instead of a single JSON blob.
RUN_LOG_S3_BUCKET = 'bidgely-support-ps'
RUN_LOG_S3_KEY     = 'opshub/run_log.json'
RUN_LOG_RETENTION_DAYS = 120  # ~4 months
_run_log_lock = threading.Lock()


def _load_run_log():
    try:
        s3 = get_s3_client()
        obj = s3.get_object(Bucket=RUN_LOG_S3_BUCKET, Key=RUN_LOG_S3_KEY)
        return json.loads(obj['Body'].read())
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return []  # first run ever — nothing written yet, not an error
        log.exception('Failed to read run log from S3 — treating as empty')
        return []
    except Exception:
        log.exception('Failed to read/parse run log from S3 — treating as empty')
        return []


def _save_run_log(entries):
    s3 = get_s3_client()
    s3.put_object(
        Bucket=RUN_LOG_S3_BUCKET,
        Key=RUN_LOG_S3_KEY,
        Body=json.dumps(entries).encode('utf-8'),
        ContentType='application/json',
    )


def _prune_run_log(entries):
    cutoff = datetime.now(timezone.utc) - timedelta(days=RUN_LOG_RETENTION_DAYS)
    kept = []
    for e in entries:
        try:
            ts = datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00'))
        except (KeyError, ValueError):
            kept.append(e)  # keep anything we can't parse rather than silently losing it
            continue
        if ts >= cutoff:
            kept.append(e)
    return kept


@app.route('/api/logs', methods=['GET'])
@admin_or_ps_required
def get_logs():
    return jsonify({'status': 'ok', 'logs': _load_run_log()})


@app.route('/api/logs/summary', methods=['GET'])
@login_required
def get_logs_summary():
    """Lightweight, shared stats for the per-tab 'Last Run / Total Runs /
    Last Status' cards — open to EVERY logged-in role (including delivery),
    unlike /api/logs itself, since it only returns aggregate counts for one
    service type, not the raw history (tickets, other users, queue names)."""
    run_type = request.args.get('type')
    entries = _load_run_log()
    if run_type:
        entries = [e for e in entries if e.get('type') == run_type]

    total_sent = sum(e.get('sent', 0) for e in entries)
    last = entries[0] if entries else None  # newest-first already

    return jsonify({
        'status': 'ok',
        'totalRuns': len(entries),
        'totalSent': total_sent,
        'lastRun': last,
    })


@app.route('/api/logs', methods=['POST'])
@login_required
def post_log():
    """Any logged-in user can log THEIR OWN run — user/username come from the
    server-side session, never from the request body, so nobody can spoof
    who actually ran something."""
    data = request.json or {}
    user = session['user']

    entry = {
        'id': uuid.uuid4().hex,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'user': user.get('name') or user['email'],
        'username': user['email'],
        'type': data.get('type', '—'),
        'ticket': data.get('ticket') or '—',
        'env': data.get('env', '—'),
        'queue': data.get('queue', '—'),
        'measurement': data.get('measurement') or '—',
        'entries': data.get('entries', 0),
        'sent': data.get('sent', 0),
        'failed': data.get('failed', 0),
        'status': (
            'TERMINATED' if data.get('terminated')
            else 'OK' if data.get('failed', 0) == 0
            else 'FAILED' if data.get('sent', 0) == 0
            else 'PARTIAL'
        ),
    }

    with _run_log_lock:
        entries = _load_run_log()
        entries.insert(0, entry)
        entries = _prune_run_log(entries)
        _save_run_log(entries)

    return jsonify({'status': 'ok', 'entry': entry})


@app.route('/api/logs', methods=['DELETE'])
@admin_required
def delete_logs():
    with _run_log_lock:
        _save_run_log([])
    return jsonify({'status': 'ok'})


# ─── AUTH ROUTES ─────────────────────────────────────────────────

@app.route('/login')
def login():
    redirect_uri = url_for('auth_callback', _external=True)
    # hd restricts Google's account chooser to the given Workspace domain up front;
    # the server-side domain check below is what actually enforces it, since hd
    # is only a UI hint and can't be trusted on its own.
    return google.authorize_redirect(redirect_uri, hd=ALLOWED_DOMAIN)


@app.route('/auth/callback')
def auth_callback():
    try:
        token = google.authorize_access_token()
        userinfo = token.get('userinfo') or google.parse_id_token(token)
    except Exception as e:
        log.exception('Google OAuth callback failed')
        return redirect('/?login_error=1')

    email = (userinfo.get('email') or '').lower()
    email_verified = userinfo.get('email_verified', False)
    domain = email.split('@')[-1] if '@' in email else ''

    if not email_verified or domain != ALLOWED_DOMAIN:
        session.clear()
        log.warning(f'Login rejected: {email or "(unknown)"} — not a verified @{ALLOWED_DOMAIN} account')
        return redirect('/?login_error=1')

    session['user'] = {
        'email': email,
        'name': userinfo.get('name') or email.split('@')[0],
        'role': resolve_role(email),
    }
    log.info(f"Login OK: {email} -> role={session['user']['role']}")
    return redirect('/')


@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'status': 'ok'})


@app.route('/api/me')
def api_me():
    if LOCAL_NO_AUTH:
        session.setdefault('user', LOCAL_DEV_USER)
        return jsonify({'status': 'ok', 'user': session['user']})
    user = session.get('user')
    if not user:
        return jsonify({'status': 'error', 'message': 'Not logged in'}), 401
    return jsonify({'status': 'ok', 'user': user})


# ─── QUEUE CONFIGS ─────────────────────────────────────────────
ENV_CONFIG = {
    'aggregation': {
        'NA2': {'region': 'us-east-1',    'account': '857283459404', 'profile': None},
        'NA':  {'region': 'us-east-1',    'account': '857283459404', 'profile': None},
        'CA':  {'region': 'ca-central-1', 'account': '076900401824', 'profile': 'ca'},
        'EU':  {'region': 'eu-central-1', 'account': '967871724166', 'profile': 'EU'},
    },
    'disaggregation': {
        'NA2': {'region': 'us-east-1',    'account': '857283459404', 'profile': 'na'},
        'NA':  {'region': 'us-east-1',    'account': '857283459404', 'profile': None},
        'CA':  {'region': 'ca-central-1', 'account': '076900401824', 'profile': 'ca'},
        'EU':  {'region': 'eu-central-1', 'account': '967871724166', 'profile': 'EU'},
    },
    'rate_comparison': {
        'NA2': {'region': 'us-east-1', 'account': '857283459404', 'profile': 'na'},
        'NA':  {'region': 'us-east-1', 'account': '857283459404', 'profile': 'na'},
    },
}

# ─── BC TRACKER (S3) CONFIG ─────────────────────────────────────
BC_DASHBOARD_S3_BUCKET  = 'bidgely-support-ps'
BC_DASHBOARD_S3_KEY     = 'scripts/bc_dashboard_final_status.json'
BC_DASHBOARD_S3_PROFILE = None          # set to a profile name here if this bucket needs one, like 'ca'/'EU' above
BC_DASHBOARD_S3_REGION  = 'us-east-1'   # adjust if the bucket lives in a different region
BC_DASHBOARD_CACHE_TTL  = 60            # seconds — avoids hitting S3 on every tab render; Refresh button bypasses this

_bc_dashboard_cache = {'data': None, 'fetched_at': 0}


def get_s3_client():
    if BC_DASHBOARD_S3_PROFILE:
        session = boto3.Session(profile_name=BC_DASHBOARD_S3_PROFILE, region_name=BC_DASHBOARD_S3_REGION)
    else:
        session = boto3.Session(region_name=BC_DASHBOARD_S3_REGION)
    return session.client('s3')


def get_sqs_client(script_type, env):
    cfg = ENV_CONFIG[script_type][env]
    if cfg['profile']:
        session = boto3.Session(profile_name=cfg['profile'], region_name=cfg['region'])
    else:
        session = boto3.Session(region_name=cfg['region'])
    return session.client('sqs')


def build_queue_url(script_type, env, queue_name):
    cfg = ENV_CONFIG[script_type][env]
    return f"https://sqs.{cfg['region']}.amazonaws.com/{cfg['account']}/{queue_name}"


# ─── MESSAGE BODY BUILDERS ──────────────────────────────────────

def build_aggregation_msg(uuid, hid, start_ts, end_ts, consumption_types, agg_modes, measurement_type, delete_before_run, send_notifications=False):
    ct_xml  = ''.join(f'<consumptionType>{c}</consumptionType>' for c in consumption_types)
    mode_xml = ''.join(f'<addMode>{m}</addMode>' for m in agg_modes)
    del_tag = '<deleteBeforeRun>true</deleteBeforeRun>' if delete_before_run else ''
    notif_tag = f'<sendnotifications>{"true" if send_notifications else "false"}</sendnotifications>'
    return (
        f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<aggregateUploadEvent>'
        f'<uuid>{uuid}</uuid>'
        f'<hid>{hid}</hid>'
        f'<startTs>{start_ts}</startTs>'
        f'<endTs>{end_ts}</endTs>'
        f'<consumptionTypes>{ct_xml}</consumptionTypes>'
        f'<aggModes>{mode_xml}</aggModes>'
        f'<measurementType>{measurement_type}</measurementType>'
        f'{del_tag}'
        f'{notif_tag}'
        f'</aggregateUploadEvent>'
    )


def build_disaggregation_msg(uuid, hid, start_ts, end_ts):
    return (
        f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<gbTempDataReadyEvent end="{end_ts}" homeOrdinal="{hid}" start="{start_ts}" userId="{uuid}"/>'
    )


def build_rate_comparison_msg(uuid, hid, measurement_type, start_ts=None, end_ts=None):
    dur_start = f'<dataDurationStart>{start_ts}</dataDurationStart>' if start_ts else ''
    dur_end   = f'<dataDurationEnd>{end_ts}</dataDurationEnd>'       if end_ts   else ''
    return (
        f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<rateComparisonEvent>'
        f'<uuid>{uuid}</uuid>'
        f'<hid>{hid}</hid>'
        f'<measurementType>{measurement_type}</measurementType>'
        f'{dur_start}{dur_end}'
        f'</rateComparisonEvent>'
    )


# ─── SEND SINGLE SQS MESSAGE ───────────────────────────────────

def send_message(sqs, queue_url, body):
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
    return resp['MessageId']


# ─── ROUTES ────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/bc-dashboard-status', methods=['GET'])
@login_required
def bc_dashboard_status():
    """
    Proxies s3://bidgely-support-ps/scripts/bc_dashboard_final_status.json for the
    BC Tracker tab. A browser can't address an s3:// URI directly, so this
    route fetches it server-side (using this process's own AWS credentials/
    role — NOT the developer's personal AWS CLI profile) and returns it as-is.
    """
    force_refresh = request.args.get('t') is not None
    now = time.time()

    if not force_refresh and _bc_dashboard_cache['data'] is not None \
       and (now - _bc_dashboard_cache['fetched_at']) < BC_DASHBOARD_CACHE_TTL:
        return jsonify(_bc_dashboard_cache['data'])

    try:
        s3 = get_s3_client()
        obj = s3.get_object(Bucket=BC_DASHBOARD_S3_BUCKET, Key=BC_DASHBOARD_S3_KEY)
        data = json.loads(obj['Body'].read())

        _bc_dashboard_cache['data'] = data
        _bc_dashboard_cache['fetched_at'] = now

        return jsonify(data)
    except Exception as e:
        log.exception('BC dashboard status fetch error')
        return jsonify({'status': 'error', 'message': str(e)}), 502


def check_dispatch_permission(role, script_type, queue_name):
    """Mirrors the frontend's ROLE_CONFIG so it can't be bypassed with a raw API call.
    'delivery' role only ever has the Aggregation Re-Run tab, restricted to the priority queue.
    'her_revocation' is open to admin + PS (user role) — same as everything else except
    'delivery', which stays excluded (delivery only ever gets the Aggregation Re-Run tab)."""
    if role == 'delivery':
        if script_type != 'aggregation':
            return False, 'Your role does not have access to this service'
        if 'priority' not in (queue_name or '').lower():
            return False, 'Your role is restricted to the priority queue only'
    if script_type == 'her_revocation' and role not in ('admin', 'user'):
        return False, 'HER Revocation is restricted to admin and PS team members'
    return True, None


@app.route('/dispatch/aggregation', methods=['POST'])
@login_required
def dispatch_aggregation():
    data = request.json
    role = session['user']['role']
    ok, err = check_dispatch_permission(role, 'aggregation', data.get('queue_name', ''))
    if not ok:
        return jsonify({'status': 'error', 'message': err}), 403
    env          = data['env']
    queue_name   = data['queue_name']
    uuids        = data['uuids']          # list of uuid strings
    hid          = data.get('hid', 1)
    start_ts_raw = data.get('start_ts', 0)
    end_ts_raw   = data.get('end_ts', 0)
    ct           = data.get('consumption_types', ['ENERGY_CONSUMPTION'])
    modes        = data.get('agg_modes', ['HOUR', 'DAY', 'MONTH'])
    measurement  = data.get('measurement_type', 'ELECTRIC')
    delete_first = data.get('delete_before_run', False)
    send_notifs  = data.get('send_notifications', False)
    threads      = data.get('threads', 2)

    # start_ts / end_ts arrive as EITHER a single value applied to every uuid
    # (manual time mode) OR a list with one value per uuid, positionally
    # matched to `uuids` (per-row time mode, from an uploaded file's own
    # start/end columns). Normalize to a per-uuid list either way, so each
    # uuid always gets its own scalar timestamp. Previously a per-row list
    # was passed straight through as a single value shared by every uuid —
    # Python's f-string then rendered the whole Python list as text, so the
    # actual message sent was start="[170..., 170..., ...]" repeated for
    # every uuid in the batch, which every consumer correctly rejects as
    # malformed.
    start_ts_list = start_ts_raw if isinstance(start_ts_raw, list) else [start_ts_raw] * len(uuids)
    end_ts_list   = end_ts_raw   if isinstance(end_ts_raw, list)   else [end_ts_raw] * len(uuids)
    if len(start_ts_list) != len(uuids) or len(end_ts_list) != len(uuids):
        return jsonify({
            'status': 'error',
            'message': f'start_ts/end_ts count ({len(start_ts_list)}/{len(end_ts_list)}) does not match uuid count ({len(uuids)})',
        }), 400

    try:
        sqs       = get_sqs_client('aggregation', env)
        queue_url = build_queue_url('aggregation', env, queue_name)
        results   = {'sent': [], 'failed': []}

        def send_one(item):
            uuid, start_ts, end_ts = item
            body = build_aggregation_msg(uuid, hid, start_ts, end_ts, ct, modes, measurement, delete_first, send_notifs)
            try:
                mid = send_message(sqs, queue_url, body)
                log.info(f'AGG sent uuid={uuid} messageId={mid}')
                results['sent'].append({'uuid': uuid, 'messageId': mid})
            except Exception as e:
                log.error(f'AGG failed uuid={uuid} error={e}')
                results['failed'].append({'uuid': uuid, 'error': str(e)})

        with ThreadPoolExecutor(max_workers=threads) as ex:
            ex.map(send_one, zip(uuids, start_ts_list, end_ts_list))

        return jsonify({
            'status': 'ok',
            'total':  len(uuids),
            'sent':   len(results['sent']),
            'failed': len(results['failed']),
            'results': results,
        })
    except Exception as e:
        log.exception('Aggregation dispatch error')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/dispatch/disaggregation', methods=['POST'])
@login_required
def dispatch_disaggregation():
    data     = request.json
    role     = session['user']['role']
    ok, err = check_dispatch_permission(role, 'disaggregation', data.get('queue_name', ''))
    if not ok:
        return jsonify({'status': 'error', 'message': err}), 403
    env      = data['env']
    queue_name = data['queue_name']
    uuids    = data['uuids']
    hid      = data.get('hid', 1)
    start_ts_raw = data.get('start_ts', 0)
    end_ts_raw   = data.get('end_ts', 0)
    threads  = data.get('threads', 2)

    # See the identical comment in dispatch_aggregation above — start_ts/end_ts
    # can arrive as a single value (manual mode) or a per-uuid list (per-row
    # mode, from an uploaded file's own start/end columns). This used to pass
    # a per-row list straight through as if it were one shared value, so every
    # uuid in the batch got sent the literal text "[170..., 170..., ...]" as
    # its start/end — which every consumer correctly rejects as malformed.
    start_ts_list = start_ts_raw if isinstance(start_ts_raw, list) else [start_ts_raw] * len(uuids)
    end_ts_list   = end_ts_raw   if isinstance(end_ts_raw, list)   else [end_ts_raw] * len(uuids)
    if len(start_ts_list) != len(uuids) or len(end_ts_list) != len(uuids):
        return jsonify({
            'status': 'error',
            'message': f'start_ts/end_ts count ({len(start_ts_list)}/{len(end_ts_list)}) does not match uuid count ({len(uuids)})',
        }), 400

    try:
        sqs       = get_sqs_client('disaggregation', env)
        queue_url = build_queue_url('disaggregation', env, queue_name)
        results   = {'sent': [], 'failed': []}

        def send_one(item):
            uuid, start_ts, end_ts = item
            body = build_disaggregation_msg(uuid, hid, start_ts, end_ts)
            try:
                mid = send_message(sqs, queue_url, body)
                log.info(f'DISAGG sent uuid={uuid} messageId={mid}')
                results['sent'].append({'uuid': uuid, 'messageId': mid})
            except Exception as e:
                log.error(f'DISAGG failed uuid={uuid} error={e}')
                results['failed'].append({'uuid': uuid, 'error': str(e)})

        with ThreadPoolExecutor(max_workers=threads) as ex:
            ex.map(send_one, zip(uuids, start_ts_list, end_ts_list))

        return jsonify({
            'status': 'ok',
            'total':  len(uuids),
            'sent':   len(results['sent']),
            'failed': len(results['failed']),
            'results': results,
        })
    except Exception as e:
        log.exception('Disaggregation dispatch error')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/dispatch/rate_comparison', methods=['POST'])
@login_required
def dispatch_rate_comparison():
    data        = request.json
    role        = session['user']['role']
    ok, err = check_dispatch_permission(role, 'rate_comparison', data.get('queue_name', ''))
    if not ok:
        return jsonify({'status': 'error', 'message': err}), 403
    env         = data['env']
    queue_name  = data['queue_name']
    uuids       = data['uuids']
    hid         = data.get('hid', 1)
    measurement = data.get('measurement_type', 'ELECTRIC')
    start_ts    = data.get('start_ts') or None
    end_ts      = data.get('end_ts')   or None
    threads     = data.get('threads', 2)

    try:
        sqs       = get_sqs_client('rate_comparison', env)
        queue_url = build_queue_url('rate_comparison', env, queue_name)
        results   = {'sent': [], 'failed': []}

        def send_one(uuid):
            body = build_rate_comparison_msg(uuid, hid, measurement, start_ts, end_ts)
            try:
                mid = send_message(sqs, queue_url, body)
                log.info(f'RC sent uuid={uuid} messageId={mid}')
                results['sent'].append({'uuid': uuid, 'messageId': mid})
            except Exception as e:
                log.error(f'RC failed uuid={uuid} error={e}')
                results['failed'].append({'uuid': uuid, 'error': str(e)})

        with ThreadPoolExecutor(max_workers=threads) as ex:
            ex.map(send_one, uuids)

        return jsonify({
            'status': 'ok',
            'total':  len(uuids),
            'sent':   len(results['sent']),
            'failed': len(results['failed']),
            'results': results,
        })
    except Exception as e:
        log.exception('Rate comparison dispatch error')
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ─── HER REVOCATION ──────────────────────────────────────────────
# The actual revoke logic, token storage, and audit log all live in
# her_revocation.py — these routes are a thin admin-only wrapper around it.

@app.route('/api/her/token-status', methods=['GET'])
@admin_or_ps_required
def her_token_status():
    """Never returns the token itself — only whether one is set and who/when it was
    last updated via the UI, so this is safe to poll from the HER Revocation tab."""
    return jsonify({'status': 'ok', **get_token_status()})


@app.route('/api/her/token', methods=['POST'])
@admin_or_ps_required
def her_set_token():
    data = request.json or {}
    try:
        meta = set_her_revoke_token(data.get('token', ''), session['user']['email'])
        log.info(f"HER revoke token updated by {session['user']['email']}")
        return jsonify({'status': 'ok', 'updated_by': meta['updated_by'], 'updated_at': meta['updated_at']})
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400


@app.route('/dispatch/her_revocation', methods=['POST'])
@admin_or_ps_required
def dispatch_her_revocation():
    data = request.json
    role = session['user']['role']
    ok, err = check_dispatch_permission(role, 'her_revocation', '')
    if not ok:
        return jsonify({'status': 'error', 'message': err}), 403

    env               = data.get('env', 'NA2')
    notification_ids  = data['notification_ids']   # list of notification-id strings
    forced_revoke     = data.get('forced_revoke', True)
    threads           = data.get('threads', 2)
    ticket_number     = (data.get('ticket_number') or '').strip()

    if env not in REVOKE_API_CONFIG:
        return jsonify({'status': 'error', 'message': f'No revoke host configured for env {env} yet'}), 400

    if not ticket_number:
        return jsonify({'status': 'error', 'message': 'A ticket number is required for every revocation run'}), 400

    token = get_her_revoke_token()
    if not token:
        log.error('HER revoke attempted with no HER_REVOKE_ACCESS_TOKEN set')
        return jsonify({'status': 'error', 'message': 'No revoke token configured on the server — ask an admin to set today\'s token'}), 500

    actor = session['user']['email']
    log.info(f'HER revoke requested by {actor} — ticket={ticket_number} env={env} count={len(notification_ids)} forced_revoke={forced_revoke}')
    try:
        # revoke_many (from her_revocation.py) does the actual work — threaded
        # calls against the external API, with a built-in single retry per id.
        results = revoke_many(notification_ids, token, env=env, forced_revoke=forced_revoke, threads=threads)
        log.info(f'HER revoke by {actor} complete — sent={len(results["sent"])} failed={len(results["failed"])}')

        # One audit row per dispatch call — a multi-batch UI run ends up with one
        # row per batch, same granularity as the log lines above. This is what
        # backs the tab's Revocation History card, so any admin can see who
        # revoked what, on which ticket, regardless of whose browser ran it.
        record_her_audit_entry(
            actor=actor, ticket=ticket_number, env=env,
            total=len(notification_ids), sent=len(results['sent']), failed=len(results['failed']),
            forced_revoke=forced_revoke,
            failed_ids=[f['notification_id'] for f in results['failed']],
        )

        return jsonify({
            'status': 'ok',
            'total':  len(notification_ids),
            'sent':   len(results['sent']),
            'failed': len(results['failed']),
            'results': results,
        })
    except Exception as e:
        log.exception('HER revocation dispatch error')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/her/audit', methods=['GET'])
@admin_or_ps_required
def her_audit_log():
    ticket = request.args.get('ticket') or None
    env    = request.args.get('env') or None
    actor  = request.args.get('actor') or None
    limit  = request.args.get('limit', 200)
    try:
        limit = int(limit)
    except ValueError:
        limit = 200
    entries = get_her_audit_log(limit=limit, ticket=ticket, env=env, actor=actor)
    return jsonify({'status': 'ok', 'entries': entries})


if __name__ == '__main__':
    if LOCAL_NO_AUTH:
        print('\n  ⚠ OPSHUB_LOCAL_NO_AUTH is on — Google SSO is BYPASSED.')
        print(f"    Every request is treated as {LOCAL_DEV_USER['email']} (role={LOCAL_DEV_USER['role']}).")
        print('    Do not set this anywhere other than your own machine.\n')
    else:
        missing = [v for v in ('GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET') if not os.environ.get(v)]
        if missing:
            print('\n  ⚠ Missing required environment variable(s): ' + ', '.join(missing))
            print('  Google sign-in will fail with "invalid_client" until these are set.')
            print('  Set them in a .env file next to server.py, e.g.:')
            print('    GOOGLE_CLIENT_ID=...')
            print('    GOOGLE_CLIENT_SECRET=...')
            print('    FLASK_SECRET_KEY=...')
            print('  ...or set OPSHUB_LOCAL_NO_AUTH=1 in .env to skip login entirely for local testing.\n')
    if not get_token_status()['set']:
        print('  ⚠ No HER revoke token set — HER Revocation tab will fail until an admin')
        print('    pastes today\'s token there, or HER_REVOKE_ACCESS_TOKEN is set as a fallback.\n')
    print('\n  PS Internal Ops Dashboard')
    print('  http://localhost:8080\n')
    app.run(host='0.0.0.0', port=8080, debug=False)
