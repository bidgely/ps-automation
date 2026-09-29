"""
HER Revocation — notifications-revoke API client.

This replaces the old revoke_post_api_na2.py CLI script. It's a standalone module:
- Import it from server.py to power the ops-hub "HER Revocation" tab, or
- Run it directly from the command line, same idea as the original script but fixed
  (no `filespath` typo, no argparse footgun on `--notification_id = <id>`, real
  auto-retry, and no token sitting in the output file).

Fixes vs. the original script:
  - `filespath` typo that crashed the original on a second run in the same folder.
  - `--notification_id=<id>` / `--notification_id <id>` both work; the original's
    documented `--notification_id = <id>` (spaces around `=`) silently corrupted the
    run because argparse read "=" itself as the value.
  - Each call auto-retries once on failure before being recorded as failed (confirmed
    behavior), instead of failing on the first non-200 response.
  - The token is never written into the output file (the original wrote the full
    URL — token included — into every failed row).
  - `failed_queue` (declared but unused in the original) is gone; failures are a
    field on each result instead of something you had to grep the CSV for.

The token itself is normally set from the ops-hub "HER Revocation" tab (admin + PS),
which writes it to .her_revoke_token.json next to this file — not committed to git,
readable only by whatever user runs the server. HER_REVOKE_ACCESS_TOKEN in the
environment is only the fallback for a fresh install where nothing's been saved yet.

Every revocation — from the ops-hub tab or from this file's own CLI — is recorded
via record_audit_entry() to S3 (bucket bidgely-support-ps, key
opshub/her_revocation_audit.json — same bucket/pattern ops-hub's own shared run
log uses), so "who ran this, on what ticket, how many went through" is answerable
by any admin/PS from the tab's Revocation History card — and survives a container
restart, unlike a local file would.

Usage (same shape as the original script):
    python3 her_revocation.py --notification_id=92c293d0-1e01-11ee-b153-8b46ec8d5a3e <token> 1 1
    python3 her_revocation.py --notification_idlist=temp.csv <token> 4 50

Or import it:
    from her_revocation import revoke_notification, revoke_many
    ok, status = revoke_notification('NA2', notification_id, token)
"""

import os
import csv
import json
import time
import getpass
import argparse
import logging
import datetime
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# ─── Token storage ───────────────────────────────────────────────
# The revoke token is shared across the team and regenerated fresh every day, so
# it needs somewhere an admin can update it without a redeploy. A token pasted
# into the ops-hub "HER Revocation" tab is written here (next to this file, never
# committed — see .gitignore) and takes priority; HER_REVOKE_ACCESS_TOKEN in the
# environment is the fallback for a fresh install with nothing saved yet.
TOKEN_STORE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.her_revoke_token.json')


def _read_token_store():
    try:
        with open(TOKEN_STORE_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _write_token_store(token, updated_by):
    data = {'token': token, 'updated_by': updated_by, 'updated_at': time.time()}
    # 0o600: the token file should be readable only by whatever user runs this
    # process, same spirit as a private key or .env file.
    fd = os.open(TOKEN_STORE_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(data, f)
    return data


def set_token(token, updated_by):
    """Saves a new token, replacing whatever was there. Raises ValueError on an
    empty token rather than silently storing one that will just fail every call."""
    token = (token or '').strip()
    if not token:
        raise ValueError('Token cannot be empty')
    return _write_token_store(token, updated_by)


def get_token_status():
    """Metadata about the current token — deliberately never includes the token
    value itself, so this is always safe to hand back to the frontend."""
    stored = _read_token_store()
    if stored and stored.get('token'):
        return {'set': True, 'source': 'ui', 'updated_by': stored.get('updated_by'), 'updated_at': stored.get('updated_at')}
    if os.environ.get('HER_REVOKE_ACCESS_TOKEN'):
        return {'set': True, 'source': 'env', 'updated_by': None, 'updated_at': None}
    return {'set': False, 'source': None, 'updated_by': None, 'updated_at': None}

# All four environments are confirmed.
REVOKE_API_CONFIG = {
    'NA2': {'host': 'naapi2-external.bidgely.com'},
    'NA':  {'host': 'naapi.bidgely.com'},
    'CA':  {'host': 'caapi.bidgely.com'},
    'EU':  {'host': 'euapi.bidgely.com'},
}

DEFAULT_ENV = 'NA2'


def get_env_token():
    """Returns today's shared team token — whichever was saved most recently via the
    ops-hub UI (persisted to disk, so it survives a restart), falling back to the
    HER_REVOKE_ACCESS_TOKEN environment variable when nothing's been saved yet.
    Reads fresh every call rather than caching, so a token saved a moment ago is
    picked up immediately by anything already running."""
    stored = _read_token_store()
    if stored and stored.get('token'):
        return stored['token']
    return os.environ.get('HER_REVOKE_ACCESS_TOKEN', '')


def build_revoke_url(notification_id, token, env=DEFAULT_ENV, forced_revoke=True):
    if env not in REVOKE_API_CONFIG:
        raise ValueError(f"No revoke host configured for env '{env}' yet")
    host = REVOKE_API_CONFIG[env]['host']
    forced = 'true' if forced_revoke else 'false'
    return (
        f'http://{host}/2.1/notifications-revoke/notifications/{notification_id}'
        f'?forcedRevoke={forced}&retry=false&access_token={token}'
    )


def revoke_notification(notification_id, token, env=DEFAULT_ENV, forced_revoke=True, retry_once=True, timeout=30):
    """POSTs a single revoke call; on a non-200 response or network error, retries
    once before giving up. Returns (ok: bool, status_or_error): the HTTP status code
    on a completed request, or a short error string if the request itself failed.
    Never includes the token in what it returns — safe to log or write to a file."""
    url = build_revoke_url(notification_id, token, env, forced_revoke)
    attempts = 2 if retry_once else 1
    last_status = None
    for attempt in range(attempts):
        try:
            resp = requests.post(url, timeout=timeout)
            last_status = resp.status_code
            if resp.status_code == 200:
                return True, resp.status_code
        except requests.RequestException as e:
            last_status = str(e)
        if attempt < attempts - 1:
            log.warning(f'retrying notification_id={notification_id} after attempt {attempt + 1} -> {last_status}')
    return False, last_status


def revoke_many(notification_ids, token, env=DEFAULT_ENV, forced_revoke=True, threads=2):
    """Revokes a list of notification IDs concurrently (bounded by `threads`).
    Returns {'sent': [...], 'failed': [...]} — 'sent' entries have {notification_id,
    status}; 'failed' entries have {notification_id, error}. Never touches the
    filesystem — callers (the CLI below, or ops-hub) decide what to do with the result."""
    results = {'sent': [], 'failed': []}

    def _one(notification_id):
        ok, status = revoke_notification(notification_id, token, env, forced_revoke)
        return notification_id, ok, status

    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        futures = [ex.submit(_one, nid) for nid in notification_ids]
        for fut in as_completed(futures):
            notification_id, ok, status = fut.result()
            if ok:
                results['sent'].append({'notification_id': notification_id, 'status': status})
            else:
                results['failed'].append({'notification_id': notification_id, 'error': str(status)})

    return results


# ─── Audit log ───────────────────────────────────────────────────
# Every revocation is irreversible, so "who ran it, on what ticket, how many
# went through" needs to be answerable by any admin, not just whoever happened
# to be looking at their own browser when it ran. Stored in S3 (same bucket
# and pattern as ops-hub's shared run log) instead of a local file, so it
# survives a container restart and isn't split across replicas — every call
# to record_audit_entry (from the ops-hub dispatch route, or from this file's
# own CLI below) appends one entry, and get_audit_log reads them back for the
# tab's Revocation History card.
#
# Same caveat as ops-hub's run log: each write is a full read-modify-write
# (GET the whole file, append, PUT it back), guarded by a lock that only
# protects one process. Fine for a single instance; if this ever needs to be
# safe under truly concurrent writers (multiple replicas), move to DynamoDB.
AUDIT_S3_BUCKET = 'bidgely-support-ps'
AUDIT_S3_KEY = 'opshub/her_revocation_audit.json'
AUDIT_S3_REGION = 'us-east-1'
_audit_lock = threading.Lock()


def _get_audit_s3_client():
    return boto3.Session(region_name=AUDIT_S3_REGION).client('s3')


def _load_audit_log():
    try:
        s3 = _get_audit_s3_client()
        obj = s3.get_object(Bucket=AUDIT_S3_BUCKET, Key=AUDIT_S3_KEY)
        return json.loads(obj['Body'].read())
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return []  # first revocation ever — nothing written yet, not an error
        log.exception('Failed to read HER audit log from S3 — treating as empty')
        return []
    except Exception:
        log.exception('Failed to read/parse HER audit log from S3 — treating as empty')
        return []


def _save_audit_log(entries):
    s3 = _get_audit_s3_client()
    s3.put_object(
        Bucket=AUDIT_S3_BUCKET,
        Key=AUDIT_S3_KEY,
        Body=json.dumps(entries).encode('utf-8'),
        ContentType='application/json',
    )


def record_audit_entry(actor, ticket, env, total, sent, failed, forced_revoke=True, failed_ids=None):
    """Writes one audit entry. Called once per dispatch — a multi-batch UI run
    calls this once per batch, same as it calls the revoke API once per batch."""
    ticket = (ticket or '').strip() or '—'
    status = 'OK' if failed == 0 else ('FAILED' if sent == 0 else 'PARTIAL')
    entry = {
        'id': uuid.uuid4().hex,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'actor': actor,
        'ticket': ticket,
        'env': env,
        'forced_revoke': bool(forced_revoke),
        'total': total,
        'sent': sent,
        'failed': failed,
        'status': status,
        # Cap how many failed IDs we keep per entry — enough to investigate a
        # run without letting one huge failed batch bloat the file.
        'failed_ids': (failed_ids or [])[:50],
    }
    with _audit_lock:
        entries = _load_audit_log()
        entries.insert(0, entry)  # newest-first, same convention as ops-hub's run log
        _save_audit_log(entries)
    return status


def get_audit_log(limit=200, ticket=None, env=None, actor=None):
    """Most-recent-first audit entries, optionally narrowed by ticket/env/actor
    (ticket and actor match as a substring, env as an exact match)."""
    entries = _load_audit_log()
    if ticket:
        entries = [e for e in entries if ticket.lower() in (e.get('ticket') or '').lower()]
    if env:
        entries = [e for e in entries if e.get('env') == env]
    if actor:
        entries = [e for e in entries if actor.lower() in (e.get('actor') or '').lower()]
    limit = max(1, min(int(limit), 1000))
    return entries[:limit]


# ─── CLI (drop-in replacement for revoke_post_api_na2.py) ───────────────────

def _read_notification_ids(args):
    if args.notification_id:
        return [args.notification_id]
    with open(args.notification_idlist, newline='') as f:
        ids = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]
    # de-dupe + sort, matching the original script's pandas unique().sort() behavior
    return sorted(set(ids))


def _write_results_csv(path, results):
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['status', 'notification_id', 'error'])
        for r in results['sent']:
            writer.writerow([r['status'], r['notification_id'], ''])
        for r in results['failed']:
            writer.writerow(['FAILED', r['notification_id'], r['error']])


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--notification_id', help='single notification_id to revoke')
    parser.add_argument('--notification_idlist', help='CSV file of notification_ids, one per line, no header')
    parser.add_argument('token', help='access token for the revoke API')
    parser.add_argument('max_threads', type=int, help='max concurrent threads')
    parser.add_argument('max_notification_ids', type=int, nargs='?', default=None,
                         help='kept for command-line compatibility with the old script; unused here (threads bound concurrency directly)')
    parser.add_argument('--env', default=DEFAULT_ENV, choices=list(REVOKE_API_CONFIG.keys()),
                         help=f'which environment to revoke against (default: {DEFAULT_ENV})')
    parser.add_argument('--out', default='notification_revoke_output.csv',
                         help='output CSV path (default: notification_revoke_output.csv in the current directory)')
    parser.add_argument('--ticket', default=None,
                         help='ticket number this run is for — recorded in the audit log alongside the ops-hub UI runs. '
                              'Defaults to "CLI-<timestamp>" if not given.')
    args = parser.parse_args()

    if not args.notification_id and not args.notification_idlist:
        parser.error('one of --notification_id or --notification_idlist is required')

    token = args.token.strip()
    notification_ids = _read_notification_ids(args)
    print(f'Total notification_ids found: {len(notification_ids)}')

    out_path = os.path.join(os.getcwd(), args.out)
    if os.path.exists(out_path):
        os.remove(out_path)
        print(f'removed existing output file: {out_path}')

    results = revoke_many(notification_ids, token, env=args.env, threads=args.max_threads)
    _write_results_csv(out_path, results)

    ticket = args.ticket or f'CLI-{int(time.time())}'
    status = record_audit_entry(
        actor=f'{getpass.getuser()} (cli)', ticket=ticket, env=args.env,
        total=len(notification_ids), sent=len(results['sent']), failed=len(results['failed']),
        failed_ids=[f['notification_id'] for f in results['failed']],
    )
    print(f'completed: {len(results["sent"])} sent, {len(results["failed"])} failed')
    print(f'results written to: {out_path}')
    print(f'audit log: recorded as ticket={ticket} status={status} (visible in the ops-hub HER Revocation tab too)')


if __name__ == '__main__':
    main()
