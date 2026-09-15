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
"""

from flask import Flask, request, jsonify, send_from_directory, session, redirect, url_for
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from functools import wraps
import boto3
import os
import json
import time
import logging
import secrets as pysecrets
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')
CORS(app, supports_credentials=True)

# Session cookie signing key — required for Flask sessions to work.
# Falls back to a random key generated at process start (fine for local/dev use,
# but means every server restart invalidates existing sessions — set
# FLASK_SECRET_KEY explicitly for anything longer-lived than local testing).
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or pysecrets.token_hex(32)

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


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('user'):
            return jsonify({'status': 'error', 'message': 'Not logged in'}), 401
        return f(*args, **kwargs)
    return wrapper


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
    'delivery' role only ever has the Aggregation Re-Run tab, restricted to the priority queue."""
    if role == 'delivery':
        if script_type != 'aggregation':
            return False, 'Your role does not have access to this service'
        if 'priority' not in (queue_name or '').lower():
            return False, 'Your role is restricted to the priority queue only'
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
    start_ts     = data.get('start_ts', 0)
    end_ts       = data.get('end_ts', 0)
    ct           = data.get('consumption_types', ['ENERGY_CONSUMPTION'])
    modes        = data.get('agg_modes', ['HOUR', 'DAY', 'MONTH'])
    measurement  = data.get('measurement_type', 'ELECTRIC')
    delete_first = data.get('delete_before_run', False)
    send_notifs  = data.get('send_notifications', False)
    threads      = data.get('threads', 2)

    try:
        sqs       = get_sqs_client('aggregation', env)
        queue_url = build_queue_url('aggregation', env, queue_name)
        results   = {'sent': [], 'failed': []}

        def send_one(uuid):
            body = build_aggregation_msg(uuid, hid, start_ts, end_ts, ct, modes, measurement, delete_first, send_notifs)
            try:
                mid = send_message(sqs, queue_url, body)
                log.info(f'AGG sent uuid={uuid} messageId={mid}')
                results['sent'].append({'uuid': uuid, 'messageId': mid})
            except Exception as e:
                log.error(f'AGG failed uuid={uuid} error={e}')
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
    start_ts = data.get('start_ts', 0)
    end_ts   = data.get('end_ts', 0)
    threads  = data.get('threads', 2)

    try:
        sqs       = get_sqs_client('disaggregation', env)
        queue_url = build_queue_url('disaggregation', env, queue_name)
        results   = {'sent': [], 'failed': []}

        def send_one(uuid):
            body = build_disaggregation_msg(uuid, hid, start_ts, end_ts)
            try:
                mid = send_message(sqs, queue_url, body)
                log.info(f'DISAGG sent uuid={uuid} messageId={mid}')
                results['sent'].append({'uuid': uuid, 'messageId': mid})
            except Exception as e:
                log.error(f'DISAGG failed uuid={uuid} error={e}')
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


if __name__ == '__main__':
    missing = [v for v in ('GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET') if not os.environ.get(v)]
    if missing:
        print('\n  ⚠ Missing required environment variable(s): ' + ', '.join(missing))
        print('  Google sign-in will fail with "invalid_client" until these are set.')
        print('  Set them in a .env file next to server.py, e.g.:')
        print('    GOOGLE_CLIENT_ID=...')
        print('    GOOGLE_CLIENT_SECRET=...')
        print('    FLASK_SECRET_KEY=...\n')
    print('\n  PS Internal Ops Dashboard')
    print('  http://localhost:8080\n')
    app.run(host='0.0.0.0', port=8080, debug=False)
