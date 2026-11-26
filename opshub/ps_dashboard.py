"""
PS Daily Operations Dashboard — Jira data source for the OpsHub "PS Dashboard" tab.

Runs the Jira filters behind the original dashboard
(https://bidgely.atlassian.net/jira/dashboards/21793) and returns tickets + counts
as JSON. server.py exposes it at GET /api/ps-dashboard.

Config (env vars — add to the `opshub-secrets` Kubernetes secret, which the
deployment already loads with envFrom):
    JIRA_EMAIL   - Atlassian account email the API token belongs to
                   (ideally a shared/service account that can see all the filters)
    JIRA_TOKEN   - Atlassian API token for that account
    JIRA_SITE    - OPTIONAL, defaults to https://bidgely.atlassian.net

Results are cached for CACHE_TTL seconds and shared by every viewer, so any number
of open dashboards costs at most one round of Jira calls per CACHE_TTL.
"""

import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger(__name__)

JIRA_SITE = os.environ.get('JIRA_SITE', 'https://bidgely.atlassian.net').rstrip('/')
CACHE_TTL = 30             # seconds a result is reused for everyone
MIN_FORCE_SECONDS = 5      # "Refresh now" can't hit Jira more often than this
PAGE_SIZE = 100
MAX_ISSUES = 500           # per filter; the count stays exact beyond this
HTTP_TIMEOUT = 25
FIELDS = ['summary', 'priority', 'status', 'assignee', 'created']

# section -> order on the page. Filter ids are the saved Jira filters used by the
# original dashboard.
FILTERS = [
    # ── Action needed ──
    {'id': 34518, 'name': 'Unassigned',           'section': 'action'},
    {'id': 46986, 'name': 'Action – Blocker',     'section': 'action'},
    {'id': 46987, 'name': 'Action – Critical',    'section': 'action'},
    {'id': 46988, 'name': 'Action – Normal',      'section': 'action'},
    {'id': 46990, 'name': 'BII – Open',           'section': 'action'},
    {'id': 46984, 'name': 'Created in last 24h',  'section': 'action'},
    {'id': 47001, 'name': 'Aging queue (all action)', 'section': 'action'},
    # ── Pending on others ──
    {'id': 46991, 'name': 'Pending – Blocker',    'section': 'pending'},
    {'id': 46992, 'name': 'Pending – Critical',   'section': 'pending'},
    {'id': 46993, 'name': 'Pending – Normal',     'section': 'pending'},
    {'id': 46994, 'name': 'BII – Pending',        'section': 'pending'},
    # ── Monitoring ──
    {'id': 45225, 'name': 'Queue stuck',          'section': 'monitoring'},
    {'id': 40966, 'name': 'Ingestion gap',        'section': 'monitoring'},
    {'id': 46982, 'name': 'HER',                  'section': 'monitoring'},
    {'id': 46983, 'name': 'BRE',                  'section': 'monitoring'},
    {'id': 45357, 'name': 'NISC onboarding',      'section': 'monitoring'},
    {'id': 41065, 'name': 'Missing files',        'section': 'monitoring'},
]


class NotConfigured(Exception):
    """JIRA_EMAIL / JIRA_TOKEN are not set."""


_cache = {'data': None, 'at': 0.0}
_lock = threading.Lock()


def _auth():
    email = os.environ.get('JIRA_EMAIL', '').strip()
    token = os.environ.get('JIRA_TOKEN', '').strip()
    if not email or not token:
        raise NotConfigured('JIRA_EMAIL / JIRA_TOKEN are not set on the server')
    return (email, token)


def _post(path, body, auth):
    r = requests.post(
        JIRA_SITE + path, json=body, auth=auth, timeout=HTTP_TIMEOUT,
        headers={'Accept': 'application/json'},
    )
    r.raise_for_status()
    return r.json()


def _row(issue):
    f = issue.get('fields') or {}
    return {
        'key': issue.get('key'),
        'summary': f.get('summary') or '',
        'priority': (f.get('priority') or {}).get('name') or '',
        'status': (f.get('status') or {}).get('name') or '',
        'assignee': (f.get('assignee') or {}).get('displayName') or 'Unassigned',
        'created': f.get('created'),
    }


def _fetch_filter(flt, auth):
    jql = 'filter = %d' % flt['id']
    out = {'id': flt['id'], 'name': flt['name'], 'section': flt['section'],
           'count': 0, 'issues': [], 'error': None, 'truncated': False}
    try:
        issues, token, last = [], None, False
        while len(issues) < MAX_ISSUES:
            body = {'jql': jql, 'fields': FIELDS, 'maxResults': PAGE_SIZE}
            if token:
                body['nextPageToken'] = token
            j = _post('/rest/api/3/search/jql', body, auth)
            issues.extend(_row(i) for i in j.get('issues', []))
            token = j.get('nextPageToken')
            last = bool(j.get('isLast')) or not token
            if last:
                break
        out['issues'] = issues
        out['count'] = len(issues)
        if not last:
            # More tickets than we pull rows for: keep the headline number exact.
            out['truncated'] = True
            try:
                out['count'] = int(_post('/rest/api/3/search/approximate-count', {'jql': jql}, auth).get('count', len(issues)))
            except Exception:
                log.warning('Jira count failed for filter %s', flt['id'])
    except Exception as e:
        # Never leak the token/URL: log the detail, show only a short reason.
        log.warning('PS dashboard: filter %s failed: %s', flt['id'], e)
        resp = getattr(e, 'response', None)
        out['error'] = 'Jira returned %s' % resp.status_code if resp is not None else 'Could not reach Jira'
    return out


def _fetch_all():
    auth = _auth()
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(lambda f: _fetch_filter(f, auth), FILTERS))
    if all(r['error'] for r in results):
        raise RuntimeError(results[0]['error'])
    return {'site': JIRA_SITE, 'updated': int(time.time() * 1000), 'filters': results}


def get_data(force=False):
    """Cached dashboard data. Everyone shares one fetch per CACHE_TTL seconds."""
    ttl = MIN_FORCE_SECONDS if force else CACHE_TTL

    def fresh():
        return _cache['data'] is not None and (time.time() - _cache['at']) < ttl

    if fresh():
        return _cache['data']
    with _lock:
        if fresh():
            return _cache['data']
        data = _fetch_all()
        _cache['data'], _cache['at'] = data, time.time()
        return data
