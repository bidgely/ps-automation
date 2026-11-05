"""
Starburst (Trino) lookup of HER notification IDs for the HER Revocation tab.

Instead of exporting a CSV by hand and uploading it, the tab asks this module
for the notification IDs of one pilot over a date range. The IDs are only
FETCHED here — nothing is revoked. They are loaded into the same list a file
upload fills, the operator previews the count, and revoking still needs the
normal "start" button.

What gets returned (same pattern the team's other Starburst queries use):
    notification_type = 'HER'
    status            = 'GENERATED'
    ...and no REVOKED row exists for that notification_id (already revoked
    ones are skipped, so a re-run never double-revokes).

Two ways to log in, tried in this order:
  1. A PERSONAL login sent with the request (the person's own Starburst
     email/role + password, typed into the Fetch card). It is used for that one
     query and then dropped — never stored, never logged — so the lookup runs
     with that person's own Starburst permissions and shows under their name in
     Starburst's query history.
  2. A shared SERVICE ACCOUNT from server environment variables. One login for
     every environment:  STARBURST_USER / STARBURST_PASSWORD
     or a different one per environment (these win over the shared pair):
        DB_NA_USER / DB_NA_PASSWORD    (NA2 and NA)
        DB_CA_USER / DB_CA_PASSWORD    (CA)
        DB_EU_USER / DB_EU_PASSWORD    (EU)
     (DB_<X>_HOST optionally overrides the default cluster host.)
Optional per-env catalog override: STARBURST_CATALOG_<ENV> (e.g. STARBURST_CATALOG_CA).

Only NA2 -> delta_prodna is confirmed from the team's example query; NA is
assumed to use the same cluster.

The `trino` package is imported lazily inside _connect(), so a missing install
fails that one lookup with a clear message instead of stopping the whole
ops-hub from starting.
"""

import os
import re
import datetime
import logging

log = logging.getLogger(__name__)

# env -> (env-var prefix, default catalog, default cluster host). Hosts and
# catalogs are from the team's "Starburst JDBC URL List" Confluence page (one
# Galaxy cluster per region); NA is assumed to share NA2's prod-na cluster.
# The host is not a secret, so it needs no env var — DB_<X>_HOST only overrides it.
STARBURST_ENV_CONFIG = {
    'NA2': ('DB_NA', 'delta_prodna', 'bidgely-prod-na-cluster.trino.galaxy.starburst.io'),
    'NA':  ('DB_NA', 'delta_prodna', 'bidgely-prod-na-cluster.trino.galaxy.starburst.io'),
    'CA':  ('DB_CA', 'delta_prodca', 'bidgely-prod-ca-cluster.trino.galaxy.starburst.io'),
    'EU':  ('DB_EU', 'delta_prodeu', 'bidgely-prod-eu-cluster.trino.galaxy.starburst.io'),
}

SCHEMA = 'public'
TABLE = 'user_notification_status_data'
STARBURST_PORT = 443

# Hard ceiling so one careless date range can't pull millions of IDs into the
# browser. Hitting it is an error, never a silent truncation — revoking a
# partial list without realizing it would be worse than refusing.
MAX_IDS = 500_000

_PILOT_RE = re.compile(r'^[A-Za-z0-9_-]{1,32}$')


def _env_settings(env):
    if env not in STARBURST_ENV_CONFIG:
        raise ValueError(f"Unknown environment '{env}'")
    prefix, default_catalog, default_host = STARBURST_ENV_CONFIG[env]
    return {
        'host': os.environ.get(f'{prefix}_HOST', '').strip() or default_host,
        # DB_<X>_USER/PASSWORD for one environment, else STARBURST_USER/PASSWORD
        # (one shared login that covers every environment).
        'user': (os.environ.get(f'{prefix}_USER', '') or os.environ.get('STARBURST_USER', '')).strip(),
        'password': os.environ.get(f'{prefix}_PASSWORD', '') or os.environ.get('STARBURST_PASSWORD', ''),
        'catalog': os.environ.get(f'STARBURST_CATALOG_{env}', '').strip() or default_catalog,
        'prefix': prefix,
    }


def get_status():
    """Which environments have Starburst credentials configured. Never includes
    any credential value — safe to hand to the frontend."""
    out = {}
    for env in STARBURST_ENV_CONFIG:
        s = _env_settings(env)
        configured = bool(s['host'] and s['user'] and s['password'])
        out[env] = {
            'configured': configured,   # a shared service account is set; personal logins always work
            'host': s['host'],
            'catalog': s['catalog'],
            'missing': [] if configured else [
                f"STARBURST_{k}" for k, v in
                (('USER', s['user']), ('PASSWORD', s['password'])) if not v
            ],
        }
    return out


def _parse_date(value, label):
    try:
        return datetime.datetime.strptime((value or '').strip(), '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        raise ValueError(f"{label} must be a date like 2026-09-30")


def _parse_time(value, label):
    """Optional HH:MM or HH:MM:SS (UTC). Blank -> None."""
    value = (value or '').strip()
    if not value:
        return None
    for fmt in ('%H:%M', '%H:%M:%S'):
        try:
            return datetime.datetime.strptime(value, fmt).time()
        except ValueError:
            pass
    raise ValueError(f"{label} must be a time like 14:30")


def _pilot_predicate(col, pilot_id, pilot_type):
    """Compares pilot_id with a literal of the column's own type so Starburst can
    skip data (a CAST on the column defeats that and makes the scan far slower).
    pilot_type is the column's detected Trino type; unknown -> safe-but-slow CAST."""
    t = (pilot_type or '').lower()
    if t in ('integer', 'bigint', 'smallint', 'tinyint') and pilot_id.isdigit():
        return f"{col} = {int(pilot_id)}"
    if t.startswith(('varchar', 'char')):
        return f"{col} = '{pilot_id}'"
    return f"CAST({col} AS VARCHAR) = '{pilot_id}'"


def build_query(pilot_id, start_date, end_date, start_time=None, end_time=None, pilot_type=None):
    """Returns (sql, start_dt, end_dt). Inputs are validated strictly and then
    inlined as literals — pilot_id against a tight pattern, dates/times parsed into
    epoch millis — so there is nothing user-controlled left to inject.
    All UTC. Without times, the range is whole days (end date inclusive). With a
    start time, the range begins at that moment of the start date; with an end
    time, it runs through the end of that minute (or second, if seconds given)
    of the end date."""
    pilot_id = (pilot_id or '').strip()
    if not _PILOT_RE.match(pilot_id):
        raise ValueError('Pilot ID may only contain letters, digits, - and _')
    start = _parse_date(start_date, 'Start date')
    end = _parse_date(end_date, 'End date')
    if end < start:
        raise ValueError('End date is before start date')
    st = _parse_time(start_time, 'Start time')
    et_raw = (end_time or '').strip()
    et = _parse_time(end_time, 'End time')
    if st is not None:
        start = start.replace(hour=st.hour, minute=st.minute, second=st.second)
    if et is not None:
        end = end.replace(hour=et.hour, minute=et.minute, second=et.second)
        end_excl = end + datetime.timedelta(seconds=1 if et_raw.count(':') == 2 else 60)
    else:
        end_excl = end + datetime.timedelta(days=1)   # whole end day
    if end_excl <= start:
        raise ValueError('The end is before the start — check the times')
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end_excl.timestamp() * 1000)  # exclusive upper bound

    sql = (
        f"SELECT DISTINCT h.notification_id, h.user_id "
        f"FROM {SCHEMA}.{TABLE} h "
        f"WHERE {_pilot_predicate('h.pilot_id', pilot_id, pilot_type)} "
        f"AND h.notification_type = 'HER' "
        f"AND h.status = 'GENERATED' "
        f"AND h.generation_timestamp >= {start_ms} "
        f"AND h.generation_timestamp < {end_ms} "
        f"AND NOT EXISTS ("
        f"SELECT 1 FROM {SCHEMA}.{TABLE} r "
        f"WHERE {_pilot_predicate('r.pilot_id', pilot_id, pilot_type)} "
        f"AND r.status = 'REVOKED' AND r.notification_id = h.notification_id"
        f")"
    )
    return sql, start, end_excl


def _connect(env, user=None, password=None):
    s = _env_settings(env)
    if user and password:                      # personal login for this one request
        s['user'], s['password'] = user.strip(), password
    if not (s['host'] and s['user'] and s['password']):
        raise RuntimeError(
            f"No Starburst login for {env} — enter your Starburst user and password in the card, "
            f"or have the server set STARBURST_USER and STARBURST_PASSWORD"
        )
    try:
        import trino
    except ImportError:
        raise RuntimeError("The 'trino' Python package isn't installed on the server (pip install trino)")
    return trino.dbapi.connect(
        host=s['host'], port=STARBURST_PORT, user=s['user'],
        http_scheme='https', auth=trino.auth.BasicAuthentication(s['user'], s['password']),
        catalog=s['catalog'], schema=SCHEMA,
    )


def _detect_pilot_type(conn):
    """Type of the pilot_id column, so the filter can match it exactly. Cheap
    (LIMIT 1). Any problem -> None, which falls back to the slower CAST form."""
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT typeof(pilot_id) FROM {SCHEMA}.{TABLE} LIMIT 1")
        rows = cur.fetchall()
        return str(rows[0][0]) if rows and rows[0] else None
    except Exception:
        log.warning('Could not detect pilot_id column type — using the slower CAST filter')
        return None


def fetch_her_notification_ids(env, pilot_id, start_date, end_date, user=None, password=None,
                               start_time=None, end_time=None):
    """Runs the lookup. Returns {'ids': [...], 'user_ids': [...] (same order), 'count': n, 'catalog': ..., 'query': sql}.
    Raises ValueError for bad input, RuntimeError for config/connection problems."""
    build_query(pilot_id, start_date, end_date, start_time, end_time)   # validate before connecting
    conn = _connect(env, user, password)
    sql, _, _ = build_query(pilot_id, start_date, end_date, start_time, end_time,
                            pilot_type=_detect_pilot_type(conn))
    ids = []
    user_ids = []   # parallel to ids — the user's UUID, for validating the list; never revoked on
    try:
        cur = conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            for r in rows:
                if r and r[0]:
                    ids.append(str(r[0]))
                    user_ids.append(str(r[1]) if len(r) > 1 and r[1] else '')
            if len(ids) > MAX_IDS:
                raise ValueError(
                    f"More than {MAX_IDS:,} notifications match — narrow the date range and fetch in smaller chunks"
                )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    seen = set()
    pairs = [(i, u) for i, u in zip(ids, user_ids) if not (i in seen or seen.add(i))]
    ids = [i for i, _ in pairs]
    user_ids = [u for _, u in pairs]
    return {'ids': ids, 'user_ids': user_ids, 'count': len(ids),
            'catalog': _env_settings(env)['catalog'], 'query': sql}


def test_login(env, user=None, password=None):
    """Connects and runs SELECT 1 — verifies the login and cluster without touching
    any notification data. Returns {'catalog', 'host', 'user'} on success."""
    conn = _connect(env, user, password)
    try:
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.fetchall()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    s = _env_settings(env)
    return {'catalog': s['catalog'], 'host': s['host'], 'user': (user or s['user'] or '').strip()}
