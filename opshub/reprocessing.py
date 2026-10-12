"""
Reprocessing — re-triggers ingestion for utility files stuck mid-pipeline.

This replaces the family of hand-maintained shell scripts (one per environment:
NA2, NA, CA, EU) that ops/PS used to run manually, editing which pilot's S3
bucket lines were commented in/out before every run. Those scripts all did the
same thing:
  1. List the raw files sitting in a pilot's "incoming" S3 location.
  2. For each file, ask the gbingestion API for the status of its processed
     chunks.
  3. Any chunk that's stuck — created but never picked up (CHUNKED), or that
     failed mid-processing (PROCESSING_FAILED) — gets "touched" (re-uploaded
     with a fresh utility_file_name metadata tag) to retrigger the pipeline.
     Chunks already SUCCESSFUL, REPORTED, ARCHIVED, or permanently
     LINES_FAILED are left alone.

This module ports that logic into ops-hub as a proper feature instead of a
script someone has to remember the right commented-out lines for:
  - Each known pilot's bucket pair is a config entry (PILOT_CONFIGS below),
    picked from a dropdown instead of hand-edited shell comments.
  - The "download, then re-upload" step is one S3 copy_object call
    (same-key, metadata replaced) instead of round-tripping the file's bytes
    through local disk — same effect, no temp files, works fine inside a
    container.
  - Runs happen on a background thread (see start_background_run), so they
    survive a reloaded or closed browser tab, the same way HER Revocation's
    runs do — see her_revocation.py's module comment for the full rationale;
    it applies here unchanged.

Only 4 pilots are configured below (one per environment) — the ones already
confirmed correct. The original scripts had ~30 total pilot configs between
them (mostly commented out), but the script itself never recorded which
"pilot" argument value went with which bucket pair — that only ever lived in
the operator's head. Rather than guess at that mapping and risk moving a
real production file to the wrong bucket, new pilots get added here
one at a time, each explicitly confirmed, via PILOT_CONFIGS below.

Usage (import it):
    from reprocessing import start_background_run
    run_id = start_background_run(actor, ticket, env, pilot_key, pilot_id, token)
"""

import os
import re
import json
import time
import logging
import datetime
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


# ─── API config ──────────────────────────────────────────────────
# Same host family HER Revocation uses (see her_revocation.REVOKE_API_CONFIG)
# — gbingestion and notifications-revoke both sit behind the same per-env API
# gateway. Only the EU host has actually been confirmed against a real
# invocation of the original script (`./Reprocessing_chunks_EU.sh
# https://euapi.bidgely.com ...`); NA2/NA/CA are carried over from HER
# Revocation's config on the assumption they're the same gateway. If a run
# against one of those three 404s immediately, that's the first thing to
# check.
REPROCESS_API_CONFIG = {
    'NA2': {'host': 'naapi2-external.bidgely.com'},
    'NA':  {'host': 'naapi.bidgely.com'},
    'CA':  {'host': 'caapi.bidgely.com'},
    'EU':  {'host': 'euapi.bidgely.com'},
}

# AWS region + named-profile-fallback per environment, for S3 access to the
# *customer* buckets (not ops-hub's own internal audit bucket, see
# AUDIT_S3_* below, which always uses this process's own default identity).
# Mirrors server.py's ENV_CONFIG['aggregation'] block, since that's the same
# shape of cross-account access this needs (CA/EU live in separate AWS
# accounts from this process's own). Kept as its own copy here, the same way
# her_revocation.py doesn't import server.py either — if server.py's config
# changes, update this alongside it.
AWS_ENV_CONFIG = {
    'NA2': {'region': 'us-east-1',    'profile': None},
    'NA':  {'region': 'us-east-1',    'profile': None},
    'CA':  {'region': 'ca-central-1', 'profile': 'ca'},
    'EU':  {'region': 'eu-central-1', 'profile': 'EU'},
}

DEFAULT_ENV = 'NA2'


def _get_boto3_session(profile, region):
    """Same two-path credential resolution server.py's _get_boto3_session
    uses: prefer a <PROFILE>_ROLE_ARN env var (Kubernetes-native, assumes the
    role via STS using this pod's own identity), fall back to a named AWS
    CLI profile (local dev). Duplicated here rather than imported from
    server.py to keep this module standalone/importable on its own, same as
    her_revocation.py."""
    if not profile:
        return boto3.Session(region_name=region)
    role_arn = os.environ.get(f'{profile.upper()}_ROLE_ARN')
    if role_arn:
        sts = boto3.client('sts')
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName='opshub-reprocessing')
        creds = resp['Credentials']
        return boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken'],
            region_name=region,
        )
    return boto3.Session(profile_name=profile, region_name=region)


def _s3_client_for_env(env):
    cfg = AWS_ENV_CONFIG[env]
    return _get_boto3_session(cfg['profile'], cfg['region']).client('s3')


# ─── Pilot configs ───────────────────────────────────────────────
# Each entry is one "which buckets does this pilot use" choice in the UI
# dropdown. `listing_bucket`/`listing_prefix` is where raw incoming files are
# listed from; `storage_bucket`/`storage_prefix` is where a stuck chunk
# actually lives and gets re-tagged. Both prefix templates may reference
# `{pilot_id}` (the pilot ID value typed in at run time — some pilots are
# identified in S3 by that same ID, e.g. the shared "ingestion adhoc"
# buckets; most aren't) and, for storage_prefix, `{path}` (the chunkPrefix
# the gbingestion API reports for a given stuck chunk).
#
# Confirmed against the original scripts + explicit follow-up with the pilot
# owner (two mismatches in the original scripts were caught and fixed here,
# not carried over):
#   - CA: the original script downloaded from bidgely-ingestion-adhoc-ca but
#     uploaded back to bidgely-hydroone-prod-ca. Confirmed both legs should
#     use bidgely-ingestion-adhoc-ca.
#   - NA: the original script had two *simultaneously active* download lines
#     for the PROCESSING_FAILED case (bidgely-ingestion-adhoc and
#     bidgely-pseg-prod-na) with only the former's upload active. Confirmed
#     both legs should use bidgely-ingestion-adhoc.
#
# To add another pilot: confirm its exact listing bucket/prefix and storage
# bucket/prefix (don't infer from a commented-out script line — that's
# exactly the mistake the two fixes above were catching), then add an entry
# here under the right environment.
PILOT_CONFIGS = {
    'NA2': [
        {
            'key': 'ameren_smb',
            'label': 'Ameren — SMB',
            'listing_bucket': 'bidgely-ameren-prod-external',
            'listing_prefix': 'SMB/Incoming/USER',
            'storage_bucket': 'bidgely-amerensmb-prod-na-2',
            'storage_prefix': '{path}',
        },
    ],
    'NA': [
        {
            'key': 'ingestion_adhoc',
            'label': 'Ingestion Adhoc (NA)',
            'listing_bucket': 'bidgely-ingestion-external-adhoc',
            'listing_prefix': '{pilot_id}/USER',
            'storage_bucket': 'bidgely-ingestion-adhoc',
            'storage_prefix': '{pilot_id}/{path}',
        },
    ],
    'CA': [
        {
            'key': 'ingestion_adhoc_ca',
            'label': 'Ingestion Adhoc (CA)',
            'listing_bucket': 'bidgely-ingestion-external-adhoc-ca',
            'listing_prefix': '{pilot_id}/RAW',
            'storage_bucket': 'bidgely-ingestion-adhoc-ca',
            'storage_prefix': '{pilot_id}/{path}',
        },
    ],
    'EU': [
        {
            'key': 'sorgenia',
            'label': 'Sorgenia',
            'listing_bucket': 'bidgely-sorgenia-prod-eu-external',
            'listing_prefix': 'Incoming/RAW',
            'storage_bucket': 'bidgely-sorgenia-prod-eu',
            'storage_prefix': '{path}',
        },
    ],
}


def get_pilot_configs(env):
    """Dropdown options for one environment — the frontend never sees bucket
    names, just {key, label}, so a pilot's actual S3 location can change here
    without touching index.html."""
    return [{'key': p['key'], 'label': p['label']} for p in PILOT_CONFIGS.get(env, [])]


def _get_pilot_config(env, pilot_key):
    for p in PILOT_CONFIGS.get(env, []):
        if p['key'] == pilot_key:
            return p
    raise ValueError(f"No pilot config '{pilot_key}' for env '{env}'")


# ─── gbingestion API calls ───────────────────────────────────────
TERMINAL_OK_STATUSES = {'LINES_FAILED', 'SUCCESSFUL', 'REPORTED', 'ARCHIVED'}


def _chunks_url(host, pilot_id, file_name, token, status_only):
    suffix = '/status' if status_only else ''
    return (f'https://{host}/2.0/gbingestion/utilities/{pilot_id}'
            f'/utility-files/{file_name}/chunks{suffix}?access_token={token}')


def get_chunk_details(host, pilot_id, file_name, token, status_only=True, timeout=30):
    """Returns the chunkDetails list the API reports for one file — each
    entry has chunkFileName/status/chunkPrefix. Raises on a network error or
    non-200 response rather than swallowing it, same as the original
    script's curl would've surfaced via empty jq output — except here the
    caller actually finds out why."""
    resp = requests.get(_chunks_url(host, pilot_id, file_name, token, status_only), timeout=timeout)
    resp.raise_for_status()
    return (resp.json() or {}).get('chunkDetails') or []


def _resolve_stuck_chunk_path(host, pilot_id, file_name, token, chunk):
    """Given one stuck chunk entry (status CHUNKED or PROCESSING_FAILED),
    returns the S3 path segment it currently lives at, or None if it can't
    be determined. For CHUNKED, the status call already carries chunkPrefix.
    For PROCESSING_FAILED, the original script re-queries the *unfiltered*
    chunks list and looks for a different entry with the same chunk name
    but status CHUNKED — the processing pipeline appears to leave a fresh
    CHUNKED record behind even after marking the original attempt
    PROCESSING_FAILED, and that fresh record's path is the one still
    retrievable. Ported as-is rather than simplified, since this is the
    pipeline's actual behavior, not an artifact of the shell script."""
    status = chunk.get('status')
    if status == 'CHUNKED':
        return chunk.get('chunkPrefix')
    if status == 'PROCESSING_FAILED':
        full = get_chunk_details(host, pilot_id, file_name, token, status_only=False)
        match = next((c for c in full
                      if c.get('chunkFileName') == chunk.get('chunkFileName') and c.get('status') == 'CHUNKED'), None)
        return match.get('chunkPrefix') if match else None
    return None


# ─── S3 listing + requeue ────────────────────────────────────────

def list_raw_files(env, pilot_cfg, pilot_id):
    """Every file sitting directly under this pilot's raw-incoming S3
    location — equivalent to the original `aws s3 ls ... | awk '{print $4}'`
    (immediate objects only, not a recursive walk)."""
    bucket = pilot_cfg['listing_bucket']
    prefix = pilot_cfg['listing_prefix'].format(pilot_id=pilot_id).rstrip('/') + '/'
    s3 = _s3_client_for_env(env)
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for obj in page.get('Contents', []):
            name = obj['Key'][len(prefix):]
            if name:  # skip the "directory marker" object some tools create at the prefix itself
                files.append(name)
    return files


def requeue_chunk(env, pilot_cfg, pilot_id, path, chunk_name, file_name):
    """Re-tags a stuck chunk in place to retrigger the ingestion pipeline —
    a same-key S3 copy with the metadata replaced, which is what the
    original script's download-then-reupload actually achieved (the bytes
    never needed to change, only the metadata). No local temp file, no
    bytes round-tripped through this process — just the one API call."""
    bucket = pilot_cfg['storage_bucket']
    prefix = pilot_cfg['storage_prefix'].format(pilot_id=pilot_id, path=path).rstrip('/')
    key = f'{prefix}/{chunk_name}'
    s3 = _s3_client_for_env(env)
    s3.copy_object(
        Bucket=bucket, Key=key,
        CopySource={'Bucket': bucket, 'Key': key},
        Metadata={'utility_file_name': file_name},
        MetadataDirective='REPLACE',
    )


def reprocess_file(env, host, pilot_cfg, pilot_id, file_name, token):
    """Checks one raw file's chunks and requeues whichever are stuck.
    Returns {'file', 'requeued': [...], 'failed': [...]} — 'requeued' lists
    chunk names successfully re-tagged, 'failed' lists {chunk, error} for
    anything that couldn't be checked or requeued. A file with both empty is
    "clean" — nothing needed reprocessing."""
    requeued, failed = [], []
    try:
        chunks = get_chunk_details(host, pilot_id, file_name, token, status_only=True)
    except Exception as e:
        return {'file': file_name, 'requeued': [], 'failed': [{'chunk': None, 'error': f'status check failed: {e}'}]}

    stuck = [c for c in chunks if c.get('status') not in TERMINAL_OK_STATUSES]
    for chunk in stuck:
        chunk_name = chunk.get('chunkFileName')
        status = chunk.get('status')
        if status not in ('CHUNKED', 'PROCESSING_FAILED'):
            # The original script only ever acted on these two statuses —
            # anything else (an unrecognized in-flight state) falls through
            # untouched there too. Surfaced here as a failure instead of
            # silently skipped, so it's visible that something needs a human
            # look rather than disappearing.
            failed.append({'chunk': chunk_name, 'error': f'unrecognized status "{status}" — not requeued, needs a manual look'})
            continue
        try:
            path = _resolve_stuck_chunk_path(host, pilot_id, file_name, token, chunk)
            if not path:
                failed.append({'chunk': chunk_name, 'error': f'could not resolve a current path for this {status} chunk'})
                continue
            requeue_chunk(env, pilot_cfg, pilot_id, path, chunk_name, file_name)
            requeued.append(chunk_name)
        except Exception as e:
            failed.append({'chunk': chunk_name, 'error': str(e)})

    return {'file': file_name, 'requeued': requeued, 'failed': failed}


def reprocess_many(files, env, host, pilot_cfg, pilot_id, token, threads=2):
    """Reprocesses a list of files concurrently. Returns {'sent': [...],
    'failed': [...], 'already_done': [...]} — shaped like
    her_revocation.revoke_many so the rest of this module's background-run
    machinery (ported from there) can stay structurally the same. 'sent' =
    files with at least one chunk successfully requeued and none that
    failed; 'failed' = files with at least one chunk that failed to requeue
    (even if others succeeded) or whose status check itself failed;
    'already_done' = files with nothing stuck to begin with."""
    results = {'sent': [], 'failed': [], 'already_done': []}

    def _one(file_name):
        return reprocess_file(env, host, pilot_cfg, pilot_id, file_name, token)

    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        futures = [ex.submit(_one, f) for f in files]
        for fut in as_completed(futures):
            r = fut.result()
            if r['failed']:
                results['failed'].append(r)
            elif r['requeued']:
                results['sent'].append(r)
            else:
                results['already_done'].append(r)

    return results


# ─── Audit log ───────────────────────────────────────────────────
# Same bucket/pattern her_revocation.py uses for its own audit log — see
# that file's module comment for the full rationale (S3 instead of a local
# file so it survives a container restart and isn't split across replicas).
AUDIT_S3_BUCKET = 'bidgely-support-ps'
AUDIT_S3_KEY = 'opshub/reprocessing_audit.json'
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
            return []
        log.exception('Failed to read reprocessing audit log from S3 — treating as empty')
        return []
    except Exception:
        log.exception('Failed to read/parse reprocessing audit log from S3 — treating as empty')
        return []


def _save_audit_log(entries):
    s3 = _get_audit_s3_client()
    s3.put_object(
        Bucket=AUDIT_S3_BUCKET,
        Key=AUDIT_S3_KEY,
        Body=json.dumps(entries).encode('utf-8'),
        ContentType='application/json',
    )


def start_audit_entry(actor, ticket, env, pilot_key, pilot_id, total):
    ticket = (ticket or '').strip() or '—'
    entry = {
        'id': uuid.uuid4().hex,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'actor': actor,
        'ticket': ticket,
        'env': env,
        'pilot_key': pilot_key,
        'pilot_id': pilot_id,
        'total': total,
        'sent': 0,
        'already_done': 0,
        'failed': 0,
        'status': 'RUNNING',
        'failed_files': [],
        'note': None,
    }
    with _audit_lock:
        entries = _load_audit_log()
        entries.insert(0, entry)
        _save_audit_log(entries)
    return entry['id']


def update_audit_entry(entry_id, sent, failed, already_done=0, status=None, failed_files=None, note=None):
    with _audit_lock:
        entries = _load_audit_log()
        match = next((e for e in entries if e.get('id') == entry_id), None)
        if match is None:
            return None
        match['sent'] = sent
        match['already_done'] = already_done
        match['failed'] = failed
        match['status'] = status or ('OK' if failed == 0 else ('FAILED' if (sent + already_done) == 0 else 'PARTIAL'))
        match['note'] = note
        if failed_files is not None:
            match['failed_files'] = (failed_files or [])[:50]
        match['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        _save_audit_log(entries)
        return match['status']


def get_audit_entry(run_id):
    entries = _load_audit_log()
    return next((e for e in entries if e.get('id') == run_id), None)


def get_audit_log(limit=200, ticket=None, env=None, actor=None):
    entries = _load_audit_log()
    if ticket:
        entries = [e for e in entries if ticket.lower() in (e.get('ticket') or '').lower()]
    if env:
        entries = [e for e in entries if e.get('env') == env]
    if actor:
        entries = [e for e in entries if actor.lower() in (e.get('actor') or '').lower()]
    limit = max(1, min(int(limit), 1000))
    return entries[:limit]


def delete_audit_entry(entry_id):
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


INTERRUPTED_NOTE = (
    'This run stopped being tracked — most likely the ops-hub server restarted (a redeploy or a crash) '
    'while it was still in progress. The numbers above are accurate up to that point; nothing after that was '
    'ever attempted. Reprocessing is safe to re-run from scratch for this pilot — it only acts on files that '
    'are still actually stuck, so anything already fixed will simply show up clean.'
)

CLEARABLE_STATUSES = {'TERMINATED', 'PARTIAL', 'FAILED', 'INTERRUPTED'}


# ─── Background (server-side) runs ───────────────────────────────
# Ported from her_revocation.py's identical mechanism — see that file's
# module comment above _active_runs for the full rationale. The short
# version: the batch loop runs on a thread inside this process, so a
# reloaded/closed browser tab only stops *watching* a run, not the run
# itself; the one thing that still stops it outright is this ops-hub
# process restarting, which is what reconcile_orphaned_run(s)_on_startup
# below is for.
_active_runs_lock = threading.Lock()
_active_runs = {}  # run_id -> {'stop_event': threading.Event}

AUTO_PAUSE_THRESHOLD = 5


def request_stop(run_id):
    with _active_runs_lock:
        run = _active_runs.get(run_id)
    if run is None:
        return False
    run['stop_event'].set()
    return True


def is_run_active(run_id):
    with _active_runs_lock:
        return run_id in _active_runs


def reconcile_orphaned_run(run_id):
    if is_run_active(run_id):
        return False
    entry = get_audit_entry(run_id)
    if entry is None or entry.get('status') != 'RUNNING':
        return False
    update_audit_entry(
        run_id, sent=entry.get('sent', 0), failed=entry.get('failed', 0),
        already_done=entry.get('already_done', 0), status='INTERRUPTED',
        failed_files=entry.get('failed_files'), note=INTERRUPTED_NOTE,
    )
    return True


def reconcile_orphaned_runs_on_startup():
    try:
        entries = _load_audit_log()
    except Exception:
        log.exception('Could not read reprocessing audit log to check for orphaned runs at startup')
        return 0
    orphaned_ids = [e['id'] for e in entries if e.get('status') == 'RUNNING']
    count = 0
    for run_id in orphaned_ids:
        try:
            if reconcile_orphaned_run(run_id):
                count += 1
        except Exception:
            log.exception(f'Failed to reconcile orphaned reprocessing run {run_id} at startup')
    return count


def _failure_signature(entry):
    """entry is a failed-file result dict (see reprocess_file) — signature
    groups by the first failure's error text, UUID-stripped so e.g. a fresh
    errorId per call doesn't defeat the match."""
    first = (entry.get('failed') or [{}])[0]
    text = str(first.get('error') or 'unknown error')
    return re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<id>', text, flags=re.IGNORECASE)[:200]


def _run_worker(run_id, stop_event, files, env, host, pilot_cfg, pilot_id, token, threads, batch_size, ticket):
    total = len(files)
    batch_size = max(1, int(batch_size or total or 1))
    batches = [files[i:i + batch_size] for i in range(0, total, batch_size)]

    total_sent = total_already = total_failed = 0
    all_failed = []
    fail_streak_sig = None
    fail_streak_count = 0
    auto_paused = False
    stopped = False

    try:
        for b, batch_files in enumerate(batches):
            if stop_event.is_set():
                stopped = True
                log.info(f'Reprocessing run {run_id} — stopped by user before batch {b + 1}/{len(batches)}')
                break

            try:
                results = reprocess_many(batch_files, env, host, pilot_cfg, pilot_id, token, threads=threads)
            except Exception:
                log.exception(f'Reprocessing run {run_id} — batch {b + 1}/{len(batches)} raised; counting all as failed')
                total_failed += len(batch_files)
                sig = 'exception: batch raised an error'
                fail_streak_count = fail_streak_count + 1 if sig == fail_streak_sig else 1
                fail_streak_sig = sig
            else:
                total_sent += len(results['sent'])
                total_already += len(results['already_done'])
                total_failed += len(results['failed'])
                if results['failed']:
                    all_failed.extend(results['failed'])

                batch_fully_failed = (
                    not results['sent'] and not results['already_done']
                    and len(results['failed']) == len(batch_files) and len(batch_files) > 0
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
                    status='RUNNING', failed_files=[f['file'] for f in all_failed[:50]],
                )
            except Exception:
                log.exception(f'Reprocessing run {run_id} — failed to write progress after batch {b + 1}')

            if fail_streak_count >= AUTO_PAUSE_THRESHOLD:
                auto_paused = True
                log.warning(f'Reprocessing run {run_id} — auto-paused after batch {b + 1}/{len(batches)}: '
                            f'the last {fail_streak_count} batches all failed with "{fail_streak_sig}"')
                break

        final_status = 'TERMINATED' if stopped else ('AUTO_PAUSED' if auto_paused else None)
        note = (
            f'Auto-paused after {fail_streak_count} consecutive fully-failed batches, all with the same error '
            f'("{fail_streak_sig}"). This looks systemic (an expired token, a network issue, or the API rejecting '
            f'every call the same way) rather than normal per-file failures. Fix the underlying issue, then re-run — '
            f'reprocessing only acts on files still actually stuck, so anything already fixed will just show up clean.'
        ) if auto_paused else None
        try:
            update_audit_entry(
                run_id, sent=total_sent, failed=total_failed, already_done=total_already,
                status=final_status, failed_files=[f['file'] for f in all_failed[:50]], note=note,
            )
        except Exception:
            log.exception(f'Reprocessing run {run_id} — failed to write final status')
    except Exception:
        log.exception(f'Reprocessing run {run_id} — worker crashed unexpectedly')
        try:
            update_audit_entry(
                run_id, sent=total_sent, failed=total_failed, already_done=total_already,
                status='FAILED', note='This run stopped unexpectedly due to a server-side error — check the ops-hub logs.',
            )
        except Exception:
            log.exception(f'Reprocessing run {run_id} — could not even write the crash status')
    finally:
        with _active_runs_lock:
            _active_runs.pop(run_id, None)


def start_background_run(actor, ticket, env, pilot_key, pilot_id, token, threads=2, batch_size=50):
    """Lists the pilot's raw files, starts a background thread to reprocess
    them, and returns the run_id immediately — the caller polls the audit
    entry for progress, same pattern as HER Revocation. Raises ValueError
    (env/pilot_key not configured) or whatever the S3 listing call raises
    (bad credentials, wrong bucket) *before* creating any audit entry, so a
    run that can't even start doesn't leave a stray RUNNING row behind."""
    if env not in REPROCESS_API_CONFIG:
        raise ValueError(f"No API host configured for env '{env}'")
    pilot_cfg = _get_pilot_config(env, pilot_key)
    host = REPROCESS_API_CONFIG[env]['host']

    files = list_raw_files(env, pilot_cfg, pilot_id)

    run_id = start_audit_entry(actor=actor, ticket=ticket, env=env, pilot_key=pilot_key,
                                pilot_id=pilot_id, total=len(files))
    stop_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {'stop_event': stop_event}
    t = threading.Thread(
        target=_run_worker,
        args=(run_id, stop_event, files, env, host, pilot_cfg, pilot_id, token, threads, batch_size, ticket),
        daemon=True,
        name=f'reprocess-{run_id[:8]}',
    )
    t.start()
    return run_id
