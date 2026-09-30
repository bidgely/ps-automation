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
import re
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
    once before giving up. Returns (ok, status_or_error, body_snippet): the HTTP
    status code (or a short error string if the request itself failed), plus up
    to 300 characters of the response body so a caller can see *why* it failed —
    e.g. an "already revoked"-type message that means the end state is already
    correct, vs. something that actually needs attention — instead of just a
    bare status code. Never includes the token in what it returns — safe to log
    or write to a file."""
    url = build_revoke_url(notification_id, token, env, forced_revoke)
    attempts = 2 if retry_once else 1
    last_status = None
    last_body = ''
    for attempt in range(attempts):
        try:
            resp = requests.post(url, timeout=timeout)
            last_status = resp.status_code
            if resp.status_code == 200:
                return True, resp.status_code, ''
            last_body = (resp.text or '').strip()[:300]
        except requests.RequestException as e:
            last_status = str(e)
            last_body = ''
        if attempt < attempts - 1:
            log.warning(f'retrying notification_id={notification_id} after attempt {attempt + 1} -> {last_status}')
    return False, last_status, last_body


# naapi2's "already revoked" response turned out to carry no readable message
# at all — it's a generic 412 with a support-ticket-style body, e.g.:
#   {"error": {"code": "5006", "message": "An error occurred. Please contact
#    support with error ID: 244cb441-...", "errorId": "244cb441-..."}}
# The message text and errorId are useless for matching (errorId/requestId
# are a fresh random UUID on every single call, even for the exact same
# notification_id retried twice), so we match on the HTTP status plus the
# numeric error code instead. IMPORTANT CAVEAT: because the message is a
# generic "contact support" string, we're inferring that code 5006 means
# "already revoked" from the examples seen so far — we don't have
# confirmation from naapi2/the API team that 5006 is used *exclusively* for
# that case. If a genuinely-failing (not-already-done) call ever also
# returns 412/5006, this would wrongly count it as done. Worth confirming
# with the API owners; until then this is a best-effort match.
ALREADY_DONE_HTTP_STATUS = 412
ALREADY_DONE_ERROR_CODES = {'5006'}

# Fallback for any other response shape that DOES carry a distinctive human-
# readable phrase (case-insensitive substring match against the raw body).
ALREADY_DONE_PHRASES = []


def _looks_already_done(status, body):
    if status == ALREADY_DONE_HTTP_STATUS:
        try:
            parsed = json.loads(body or '')
            code = str((parsed.get('error') or {}).get('code', ''))
            if code in ALREADY_DONE_ERROR_CODES:
                return True
        except (ValueError, AttributeError, TypeError):
            pass
    body_l = (body or '').lower()
    return any(p in body_l for p in ALREADY_DONE_PHRASES)


def revoke_many(notification_ids, token, env=DEFAULT_ENV, forced_revoke=True, threads=2):
    """Revokes a list of notification IDs concurrently (bounded by `threads`).
    Returns {'sent': [...], 'failed': [...], 'already_done': [...]} — 'sent'
    entries have {notification_id, status}; 'failed' entries have
    {notification_id, error, detail} (detail is the response body, if any);
    'already_done' entries (a failed call whose body matched ALREADY_DONE_PHRASES)
    have the same shape as 'failed' but are counted separately since the
    notification is already in the desired end state. Never touches the
    filesystem — callers (the CLI below, or ops-hub) decide what to do with the
    result."""
    results = {'sent': [], 'failed': [], 'already_done': []}

    def _one(notification_id):
        ok, status, body = revoke_notification(notification_id, token, env, forced_revoke)
        return notification_id, ok, status, body

    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        futures = [ex.submit(_one, nid) for nid in notification_ids]
        for fut in as_completed(futures):
            notification_id, ok, status, body = fut.result()
            if ok:
                results['sent'].append({'notification_id': notification_id, 'status': status})
            elif _looks_already_done(status, body):
                results['already_done'].append({'notification_id': notification_id, 'error': str(status), 'detail': body})
            else:
                results['failed'].append({'notification_id': notification_id, 'error': str(status), 'detail': body})

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


def record_audit_entry(actor, ticket, env, total, sent, failed, already_done=0, forced_revoke=True, failed_ids=None):
    """Writes one audit entry. Called once per dispatch — a multi-batch UI run
    calls this once per batch, same as it calls the revoke API once per batch.
    `sent` is notifications freshly revoked by this call; `already_done` is
    ones that were already in the revoked state (see _looks_already_done in
    revoke_many) — kept separate so Revocation History can show both, instead
    of folding already-done ones into `sent` where they'd be indistinguishable
    from ones this run actually revoked."""
    ticket = (ticket or '').strip() or '—'
    # A run counts as FAILED only if nothing ended up in the desired state at
    # all — freshly revoked or already revoked both count as "done" here.
    status = 'OK' if failed == 0 else ('FAILED' if (sent + already_done) == 0 else 'PARTIAL')
    entry = {
        'id': uuid.uuid4().hex,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'actor': actor,
        'ticket': ticket,
        'env': env,
        'forced_revoke': bool(forced_revoke),
        'total': total,
        'sent': sent,
        'already_done': already_done,
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


def start_audit_entry(actor, ticket, env, total, forced_revoke=True):
    """Creates a RUNNING audit entry up front and returns its id, so a long
    multi-batch UI run (hundreds of batches for a large file) can update this
    one row incrementally as batches complete via update_audit_entry(), instead
    of either writing a separate row per batch or only writing a row once the
    entire run finishes — which would lose the whole audit trail if the
    browser tab closes or crashes partway through a big run."""
    ticket = (ticket or '').strip() or '—'
    entry = {
        'id': uuid.uuid4().hex,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'actor': actor,
        'ticket': ticket,
        'env': env,
        'forced_revoke': bool(forced_revoke),
        'total': total,
        'sent': 0,
        'already_done': 0,
        'failed': 0,
        'status': 'RUNNING',
        'failed_ids': [],
        'note': None,
    }
    with _audit_lock:
        entries = _load_audit_log()
        entries.insert(0, entry)
        _save_audit_log(entries)
    return entry['id']


def update_audit_entry(entry_id, sent, failed, already_done=0, status=None, failed_ids=None, note=None):
    """Updates an existing entry (by id, from start_audit_entry) in place with
    new cumulative totals. Called after every batch of a long run so the row
    always reflects real progress, not just the final outcome. `note` is an
    optional short explanation shown alongside the status (currently only used
    for the AUTO_PAUSED text the background worker below writes) — passing
    None clears it, which is correct for every in-progress update since only
    the terminal write should carry one. Returns the status that was recorded,
    or None if entry_id wasn't found (e.g. the audit log was cleared out from
    under a very long-running dispatch)."""
    with _audit_lock:
        entries = _load_audit_log()
        match = None
        for e in entries:
            if e.get('id') == entry_id:
                match = e
                break
        if match is None:
            return None
        match['sent'] = sent
        match['already_done'] = already_done
        match['failed'] = failed
        # See record_audit_entry: a run only counts as FAILED if nothing ended
        # up in the desired state — freshly revoked or already-revoked both count.
        match['status'] = status or ('OK' if failed == 0 else ('FAILED' if (sent + already_done) == 0 else 'PARTIAL'))
        match['note'] = note
        if failed_ids is not None:
            match['failed_ids'] = (failed_ids or [])[:50]
        match['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        _save_audit_log(entries)
        return match['status']


def get_audit_entry(run_id):
    """A single audit entry by id, or None if it doesn't exist. Used for
    lightweight polling of one run's progress (see the /api/her/audit/run/
    <run_id> GET route) instead of re-fetching and re-rendering the whole
    Revocation History table every couple of seconds."""
    entries = _load_audit_log()
    return next((e for e in entries if e.get('id') == run_id), None)


# A row stuck showing RUNNING forever with numbers that never move again —
# because the ops-hub process itself restarted (a redeploy, a crash, an OOM
# kill) while a background run was still executing inside it. See the
# _active_runs comment further down: that registry is in-memory only, so it's
# empty the moment this process starts, regardless of what any audit row still
# says. reconcile_orphaned_run and reconcile_orphaned_runs_on_startup (below,
# once _active_runs/is_run_active exist) are what turn a row like that into a
# clearly-labeled terminal state instead of leaving it stuck forever with no
# way to tell "still going" from "abandoned" and no way to clear it.
INTERRUPTED_NOTE = (
    'This run stopped being tracked — most likely the ops-hub server restarted (a redeploy or a crash) '
    'while it was still in progress. The numbers above are accurate up to that point; nothing after that was '
    'ever attempted. If this used an uploaded file, its position was checkpointed — use "Resume from here" on '
    'that file to continue from where it left off.'
)


# Statuses a Revocation History row can be individually cleared from — never
# OK (a clean success worth keeping as a record) or RUNNING/AUTO_PAUSED (still
# in flight or waiting on someone to look at it), only the "this run is over
# and didn't fully succeed" outcomes that tend to pile up as noise. INTERRUPTED
# (see reconcile_orphaned_run below) belongs here too — it's just as much "over
# and didn't succeed" as TERMINATED/FAILED, the only difference being who
# stopped it.
CLEARABLE_STATUSES = {'TERMINATED', 'PARTIAL', 'FAILED', 'INTERRUPTED'}


def delete_audit_entry(entry_id):
    """Removes one entry from the shared HER audit log by id — used by the
    Revocation History 'Clear' action. The caller (the /api/her/audit/run/<id>
    DELETE route) is responsible for checking the entry's status is in
    CLEARABLE_STATUSES before calling this; this function itself just removes
    whatever id it's given, so it stays reusable without baking in that policy
    twice. Returns the removed entry's status, or None if entry_id wasn't found
    (e.g. it was already cleared, or the log was wiped from under this call)."""
    with _audit_lock:
        entries = _load_audit_log()
        match = None
        remaining = []
        for e in entries:
            if match is None and e.get('id') == entry_id:
                match = e
                continue
            remaining.append(e)
        if match is None:
            return None
        _save_audit_log(remaining)
        return match.get('status')


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


# Each audit entry only keeps a 50-item preview of its failed IDs (see
# record_audit_entry/update_audit_entry above) so one huge-failure run doesn't
# bloat audit_log.json — that file is re-read and re-written in full on every
# single write, by every admin's browser on every tab load. The complete list
# for a run lives in its own small S3 object instead, keyed by run_id, and is
# only written once (at the end of a run), not per batch.
FAILED_IDS_S3_PREFIX = 'opshub/her_revocation_failed_ids/'


def save_failed_ids(run_id, failed_entries):
    """Stores the complete list of failed entries for one run (each a dict
    like {notification_id, error, detail}), not capped at 50. Called once,
    when a UI run finishes — see update_audit_entry for the row itself."""
    s3 = _get_audit_s3_client()
    s3.put_object(
        Bucket=AUDIT_S3_BUCKET,
        Key=f'{FAILED_IDS_S3_PREFIX}{run_id}.json',
        Body=json.dumps(failed_entries or []).encode('utf-8'),
        ContentType='application/json',
    )


def get_failed_ids(run_id):
    """The complete failed-entries list for one run, or [] if none was ever
    saved for it (a run with zero failures, an older run from before this
    existed, or the id just doesn't exist)."""
    try:
        s3 = _get_audit_s3_client()
        obj = s3.get_object(Bucket=AUDIT_S3_BUCKET, Key=f'{FAILED_IDS_S3_PREFIX}{run_id}.json')
        return json.loads(obj['Body'].read())
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return []
        log.exception('Failed to read HER failed-ids list from S3 — treating as empty')
        return []
    except Exception:
        log.exception('Failed to read/parse HER failed-ids list from S3 — treating as empty')
        return []


# ─── Resume checkpoints ──────────────────────────────────────────
# Remembers how far into a given file a run has gotten, keyed by a signature
# the frontend derives from the uploaded file (name + size — see
# herFileSignature() in index.html; cheap and good enough since this is only
# a "resume from here?" suggestion the user can always override, not a
# correctness guarantee). Lets someone re-open the same file later — after a
# closed tab, a crash, or just coming back the next day — and be offered
# "resume from record #N" instead of having to remember and retype the skip
# count themselves.
CHECKPOINT_S3_PREFIX = 'opshub/her_revocation_checkpoints/'


def _checkpoint_s3_key(signature):
    # signature comes from the browser — sanitize it into a safe S3 key
    # segment rather than trusting it outright.
    safe = re.sub(r'[^A-Za-z0-9._-]', '_', signature or '')[:200]
    if not safe:
        raise ValueError('Invalid checkpoint signature')
    return f'{CHECKPOINT_S3_PREFIX}{safe}.json'


def save_checkpoint(signature, position, total, ticket=None, env=None):
    """Records the furthest position reached in this file so far. Called after
    every batch of a UI run — cheap (a tiny, single-object S3 write, not a
    read-modify-write of a shared list like the audit log), so it's fine to
    call this often."""
    key = _checkpoint_s3_key(signature)
    data = {
        'position': int(position),
        'total': int(total),
        'ticket': ticket,
        'env': env,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    s3 = _get_audit_s3_client()
    s3.put_object(
        Bucket=AUDIT_S3_BUCKET,
        Key=key,
        Body=json.dumps(data).encode('utf-8'),
        ContentType='application/json',
    )
    return data


def get_checkpoint(signature):
    """The saved checkpoint for this file signature, or None if there isn't
    one (a file that's never been run before, or one that finished cleanly
    and had its checkpoint cleared)."""
    key = _checkpoint_s3_key(signature)
    try:
        s3 = _get_audit_s3_client()
        obj = s3.get_object(Bucket=AUDIT_S3_BUCKET, Key=key)
        return json.loads(obj['Body'].read())
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return None
        log.exception('Failed to read HER checkpoint from S3 — treating as none')
        return None
    except Exception:
        log.exception('Failed to read/parse HER checkpoint from S3 — treating as none')
        return None


# ─── Background (server-side) multi-batch runs ──────────────────────────────
# The ops-hub UI used to drive a whole multi-batch run from the browser tab's
# own JS: one POST per batch, in a loop, with the loop itself living only in
# that tab's memory. That meant reloading the page, closing the laptop, or the
# tab simply crashing killed the run outright — even though every batch up to
# that point had gone through for real and was sitting in the audit log with
# no way to tell "still going" from "abandoned". The checkpoint feature above
# is the manual recovery for that (re-open the file, resume from the saved
# position) but still means a real rerun.
#
# Running the batch loop here instead means a browser disconnecting — reload,
# closed tab, sleeping laptop, dead wifi — no longer stops the run: it just
# stops *watching* it. The loop below keeps going against naapi2 and keeps
# writing progress to the same audit entry regardless of who is or isn't
# looking at a browser tab. The frontend's job shrinks to: POST once to start
# it, then poll the one audit row for progress.
#
# The one thing that still stops a run outright is this ops-hub *process*
# itself restarting (a redeploy, a crash, an OOM kill) — it runs as a single
# replica (see deployment.yaml), so there's no second instance to hand a job
# to, and this in-memory registry doesn't survive a restart; a run's audit row
# would be left stuck on RUNNING with nothing left updating it. The checkpoint
# feature remains the recovery path for that rarer case — same as before.
_active_runs_lock = threading.Lock()
_active_runs = {}  # run_id -> {'stop_event': threading.Event}

# Two batches can fail for the exact same underlying reason yet never compare
# equal as raw text, because naapi2 embeds a fresh random requestId/errorId in
# every response body. Match on the HTTP status + parsed error code instead,
# falling back to UUID-stripped text — same logic the frontend used to run
# client-side (see the old herFailureSignature in index.html), ported here now
# that the batch loop itself lives on the server.
_UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)


def _failure_signature(entry):
    status = str(entry.get('error')) if entry and entry.get('error') is not None else 'unknown'
    raw = (entry or {}).get('detail') or ''
    try:
        parsed = json.loads(raw)
        code = (parsed.get('error') or {}).get('code') if isinstance(parsed, dict) else None
        if code:
            return f'{status}:code={code}'
    except (ValueError, TypeError):
        pass
    return f'{status}:{_UUID_RE.sub("<id>", raw)[:200]}'


def request_stop(run_id):
    """Signals a background run to stop after its current batch finishes.
    Returns True if a live run was found and signaled, False if run_id isn't
    an active background run right now — already finished, never existed, or
    (after a server restart) no longer tracked in memory even if its audit row
    still says RUNNING."""
    with _active_runs_lock:
        run = _active_runs.get(run_id)
    if run is None:
        return False
    run['stop_event'].set()
    return True


def is_run_active(run_id):
    """Whether run_id is a background run currently executing in this
    process (as opposed to just having a RUNNING-status audit row, which can
    also mean a run was orphaned by a server restart)."""
    with _active_runs_lock:
        return run_id in _active_runs


def reconcile_orphaned_run(run_id):
    """If run_id's audit row says RUNNING but nothing in this process is
    actually executing it, marks it INTERRUPTED with an explanatory note
    instead of leaving it stuck on RUNNING forever — which otherwise happens
    every time the ops-hub process restarts (a redeploy, a crash, an OOM
    kill) while a background run was still going, since _active_runs is
    in-memory only and doesn't survive that. The numbers already recorded are
    real — everything up to the last successful batch actually went through
    against naapi2; only the tracking stopped.

    Called two ways: (1) from /api/her/run/<id>/stop, when someone clicks the
    "Stop" link on a row that turns out to already be dead — that click is
    exactly the signal that it's stuck, so fix it right then instead of just
    saying "can't stop, not active"; (2) from
    reconcile_orphaned_runs_on_startup, sweeping every leftover RUNNING row
    the moment this process starts, since at that point _active_runs is
    unconditionally empty and any RUNNING row can only be left over from a
    previous process instance.

    Returns True if a row was reconciled, False if there was nothing to do
    (genuinely still active, already terminal, or doesn't exist)."""
    if is_run_active(run_id):
        return False
    entry = get_audit_entry(run_id)
    if entry is None or entry.get('status') != 'RUNNING':
        return False
    update_audit_entry(
        run_id, sent=entry.get('sent', 0), failed=entry.get('failed', 0),
        already_done=entry.get('already_done', 0), status='INTERRUPTED',
        failed_ids=entry.get('failed_ids'), note=INTERRUPTED_NOTE,
    )
    return True


def reconcile_orphaned_runs_on_startup():
    """Called once when the ops-hub process starts (see server.py's __main__
    block), before it starts serving requests. _active_runs is guaranteed
    empty at this point — nothing has had a chance to start a run yet — so any
    audit entry still marked RUNNING here cannot legitimately belong to this
    process; it can only be left over from whatever process was handling
    HER Revocation before this one (a redeploy or a crash killed it mid-run).
    Reconciling these automatically on every startup means a stuck-RUNNING row
    like the one this whole mechanism exists for gets fixed by the very
    restart that caused it, without anyone needing to notice and click Stop
    on it manually. Returns how many rows were reconciled."""
    try:
        entries = _load_audit_log()
    except Exception:
        log.exception('Could not read HER audit log to check for orphaned runs at startup')
        return 0
    orphaned_ids = [e['id'] for e in entries if e.get('status') == 'RUNNING']
    count = 0
    for run_id in orphaned_ids:
        try:
            if reconcile_orphaned_run(run_id):
                count += 1
        except Exception:
            log.exception(f'Failed to reconcile orphaned HER run {run_id} at startup')
    return count


# A streak of this many consecutive fully-failed batches auto-pauses a run —
# almost always systemic (an expired token, a network/DNS problem, or the API
# rejecting every call the same way), not the normal handful of per-ID
# failures. Same threshold and rationale the frontend used to apply client-side.
AUTO_PAUSE_THRESHOLD = 5


def _run_worker(run_id, stop_event, notification_ids, token, env, forced_revoke,
                 threads, batch_size, ticket, file_signature, skip_count):
    total = len(notification_ids)
    batch_size = max(1, int(batch_size or total or 1))
    batches = [notification_ids[i:i + batch_size] for i in range(0, total, batch_size)]

    total_sent = total_already = total_failed = 0
    all_failed = []
    fail_streak_sig = None
    fail_streak_count = 0
    auto_paused = False
    stopped = False

    try:
        for b, batch_ids in enumerate(batches):
            if stop_event.is_set():
                stopped = True
                log.info(f'HER background run {run_id} — stopped by user before batch {b + 1}/{len(batches)}')
                break

            batch_added_new_failures = False
            try:
                results = revoke_many(batch_ids, token, env=env, forced_revoke=forced_revoke, threads=threads)
            except Exception:
                log.exception(f'HER background run {run_id} — batch {b + 1}/{len(batches)} raised; counting all as failed')
                total_failed += len(batch_ids)
                sig = 'exception: batch raised an error'
                fail_streak_count = fail_streak_count + 1 if sig == fail_streak_sig else 1
                fail_streak_sig = sig
            else:
                total_sent += len(results['sent'])
                total_already += len(results['already_done'])
                total_failed += len(results['failed'])
                if results['failed']:
                    all_failed.extend(results['failed'])
                    batch_added_new_failures = True

                batch_fully_failed = (
                    not results['sent'] and not results['already_done']
                    and len(results['failed']) == len(batch_ids) and len(batch_ids) > 0
                )
                if batch_fully_failed:
                    sig = _failure_signature(results['failed'][0]) if results['failed'] else 'unknown error'
                    fail_streak_count = fail_streak_count + 1 if sig == fail_streak_sig else 1
                    fail_streak_sig = sig
                else:
                    fail_streak_sig = None
                    fail_streak_count = 0

            try:
                update_audit_entry(
                    run_id, sent=total_sent, failed=total_failed, already_done=total_already,
                    status='RUNNING', failed_ids=[f['notification_id'] for f in all_failed[:50]],
                )
            except Exception:
                log.exception(f'HER background run {run_id} — failed to write progress after batch {b + 1}')

            # The audit row above only ever carries a 50-item PREVIEW of failed
            # IDs (see update_audit_entry) — the "⬇ Failed IDs" download reads
            # the complete list from its own separate, per-run S3 object
            # instead (save_failed_ids), which used to only get written once,
            # after the whole run finished. For a run spanning hundreds of
            # batches over hours, that meant the download stayed empty the
            # entire time it was RUNNING even as the row's Failed count climbed
            # into the thousands — confusing, and unhelpful if someone wants to
            # start investigating failures before the run itself is done.
            # Re-saving it here, only when this batch actually added new
            # failures, keeps it current without rewriting an unchanged list
            # on every batch that had none (including a batch that raised an
            # exception above, which adds to total_failed but — since there's
            # no per-ID detail to report — never touches all_failed itself).
            if batch_added_new_failures:
                try:
                    save_failed_ids(run_id, all_failed)
                except Exception:
                    log.exception(f'HER background run {run_id} — failed to save failed-ids list after batch {b + 1}')

            if file_signature:
                try:
                    new_position = min(skip_count + (b + 1) * batch_size, skip_count + total)
                    save_checkpoint(file_signature, new_position, skip_count + total, ticket, env)
                except Exception:
                    log.exception(f'HER background run {run_id} — failed to save checkpoint after batch {b + 1}')

            if fail_streak_count >= AUTO_PAUSE_THRESHOLD:
                auto_paused = True
                log.warning(f'HER background run {run_id} — auto-paused after batch {b + 1}/{len(batches)}: '
                            f'the last {fail_streak_count} batches all failed with "{fail_streak_sig}"')
                break

        final_status = 'TERMINATED' if stopped else ('AUTO_PAUSED' if auto_paused else None)
        note = (
            f'Auto-paused after {fail_streak_count} consecutive fully-failed batches, all with the same error '
            f'("{fail_streak_sig}"). This looks systemic (an expired token, a network issue, or the API rejecting '
            f'every call the same way) rather than normal per-ID failures. Fix the underlying issue, then re-run — '
            f'this file\'s position has been checkpointed, so "Resume from here" will pick up where this stopped.'
        ) if auto_paused else None
        try:
            update_audit_entry(
                run_id, sent=total_sent, failed=total_failed, already_done=total_already,
                status=final_status, failed_ids=[f['notification_id'] for f in all_failed[:50]], note=note,
            )
        except Exception:
            log.exception(f'HER background run {run_id} — failed to write final status')

        if all_failed:
            try:
                save_failed_ids(run_id, all_failed)
            except Exception:
                log.exception(f'HER background run {run_id} — failed to save the full failed-ids list')
    except Exception:
        # Belt-and-suspenders: make absolutely sure an unexpected bug in this
        # worker can't leave a run silently stuck on RUNNING forever with no
        # trace of what happened.
        log.exception(f'HER background run {run_id} — worker crashed unexpectedly')
        try:
            update_audit_entry(
                run_id, sent=total_sent, failed=total_failed, already_done=total_already,
                status='FAILED', note='This run stopped unexpectedly due to a server-side error — check the ops-hub logs.',
            )
        except Exception:
            log.exception(f'HER background run {run_id} — could not even write the crash status')
    finally:
        with _active_runs_lock:
            _active_runs.pop(run_id, None)


def start_background_run(actor, ticket, env, notification_ids, token, forced_revoke=True,
                          threads=2, batch_size=500, file_signature=None, skip_count=0):
    """Starts a multi-batch revoke run on a background thread inside this
    process and returns its run_id immediately. The caller (the ops-hub
    /api/her/run/start route) hands run_id back to the browser, which polls
    GET /api/her/audit/run/<run_id> for progress instead of driving the batch
    loop itself — see the module comment above _active_runs for why that's
    what lets a run survive a reloaded or closed browser tab."""
    run_id = start_audit_entry(actor=actor, ticket=ticket, env=env,
                                total=len(notification_ids), forced_revoke=forced_revoke)
    stop_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {'stop_event': stop_event}
    t = threading.Thread(
        target=_run_worker,
        args=(run_id, stop_event, notification_ids, token, env, forced_revoke,
              threads, batch_size, ticket, file_signature, skip_count),
        daemon=True,
        name=f'her-revoke-{run_id[:8]}',
    )
    t.start()
    return run_id


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
        total=len(notification_ids), sent=len(results['sent']),
        already_done=len(results['already_done']), failed=len(results['failed']),
        failed_ids=[f['notification_id'] for f in results['failed']],
    )
    print(f'completed: {len(results["sent"])} sent, {len(results["already_done"])} already revoked, {len(results["failed"])} failed')
    print(f'results written to: {out_path}')
    print(f'audit log: recorded as ticket={ticket} status={status} (visible in the ops-hub HER Revocation tab too)')


if __name__ == '__main__':
    main()
