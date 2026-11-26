"""
Seasonal Email Trigger — lives next to server.py, like her_revocation.py.

Replaces the manual flow (hand-written script + CSV in a Utils PR under
prod-one-time-scripts/<YYYYMMDD>/, then a manual run of run-script-<env>-ps)
with the Seasonal Email tab in index.html:

    form (pilot ID, season, environment, send date/time in IST, ticket)
      -> user list: Starburst user_meta_data query, or uploaded UUID list
      -> prod-one-time-scripts/<today IST>/<pilotId>_<season>[_n].sh + <pilotId>_<season>[_n].csv
      -> PR on bidgely/Utils, reviewed and merged as today
      -> live DevTools IP (EC2 Name tag) + run-script-<env>-ps Jenkins build
      -> build status and console on the tab, comments on the pilot ticket

server.py wires it in with one call, after admin_or_ps_required and
_get_cross_account_client are defined:

    import seasonal_email
    seasonal_email.register_routes(app, admin_or_ps_required,
                                   get_user=lambda: session['user']['email'],
                                   ec2_client_factory=...)

GitHub, Jenkins and Jira are each optional: with no token configured that step
becomes manual (download the folder / paste the Jenkins parameters / no ticket
comments) and switches to automatic once the token is set.

Local testing: SEASONAL_DRY_RUN=1 stubs Starburst, GitHub, Jira and EC2;
point SEASONAL_JENKINS_URL at local/mock_jenkins.py.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import re
import threading
import time
import uuid as uuidlib
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, time as dtime, timezone
from pathlib import Path
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("opshub.seasonal_email")

IST = ZoneInfo("Asia/Kolkata")
HERE = Path(__file__).resolve().parent


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _flag(name: str) -> bool:
    return _env(name, "0").lower() in ("1", "true", "yes")


# ─── Static configuration ───────────────────────────────────────────────────

EVENT_TYPES = ["WINTER_ALERT", "SUMMER_ALERT"]
SEASON = {"WINTER_ALERT": "winter", "SUMMER_ALERT": "summer"}
EMAIL_PROCESSOR = "SeasonalAlertUserProcessor"
DEVTOOL_SCRIPT = "/opt/bidgely/devToolServices/sh/triggerSeasonalEmail"

TICKET_RE = re.compile(r"^[A-Z][A-Z0-9]+-\d+$")
SREINFRA_RE = re.compile(r"^SREINFRA-\d+$")
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
IPV4_RE = re.compile(r"^(25[0-5]|2[0-4]\d|1?\d?\d)(\.(25[0-5]|2[0-4]\d|1?\d?\d)){3}$")

# Keyed by the same codes the HER Revocation tab uses (NA2 / NA / CA / EU).
# Any value can be overridden with SEASONAL_<KEY>_<CODE>, e.g. SEASONAL_AWS_REGION_CA.
ENVIRONMENTS = {
    "NA2": {"script_env": "prod-na-2", "jenkins_job": "run-script-na-2-ps",
            "catalog": "delta_prodna", "aws_region": "us-east-1"},
    "NA":  {"script_env": "prod-na", "jenkins_job": "run-script-na-ps",
            "catalog": "delta_prodna", "aws_region": "us-east-1"},
    "CA":  {"script_env": "prod-ca", "jenkins_job": "run-script-ca-ps",
            "catalog": "delta_prodca", "aws_region": "ca-central-1"},
    "EU":  {"script_env": "prod-eu", "jenkins_job": "run-script-prod-eu-ps",
            "catalog": "delta_prodeu", "aws_region": "eu-central-1"},
}
DEFAULTS = {"devtools_tag": "devtoolservices-od", "run_as": "bprod"}


class ValidationError(Exception):
    """User-facing input problem. Shown on the tab."""


class IntegrationError(Exception):
    """An external system (Starburst, GitHub, Jenkins, Jira, AWS) failed."""


def env_cfg(code: str) -> dict:
    code = (code or "").strip().upper()
    if code not in ENVIRONMENTS:
        raise ValidationError("Choose the environment: NA2, NA, CA or EU.")
    cfg = {**DEFAULTS, **ENVIRONMENTS[code], "code": code}
    for key in list(cfg):
        override = _env(f"SEASONAL_{key.upper()}_{code}")
        if override:
            cfg[key] = override
    cfg["api_token"] = _env(f"SEASONAL_API_TOKEN_{code}")   # optional: overrides the job's saved API_TOKEN
    return cfg


def modes() -> dict:
    """Which steps are automatic. Each switches on when its token is set."""
    dry = _flag("SEASONAL_DRY_RUN")
    return {
        "github": dry or bool(_env("SEASONAL_GITHUB_TOKEN")),
        "jenkins": dry or bool(_env("SEASONAL_JENKINS_TOKEN")),
        "jira": dry or bool(_env("SEASONAL_JIRA_TOKEN")),
    }


SCRIPTS_DIR = lambda: _env("SEASONAL_SCRIPTS_DIR", "prod-one-time-scripts").strip("/")
SREINFRA_ID = lambda: _env("SEASONAL_SREINFRA_ID", "SREINFRA-1234")
MAX_ROWS_PER_FILE = lambda: int(_env("SEASONAL_MAX_ROWS_PER_FILE", "1000000"))


# ─── Pilot profiles (optional; only names and defaults) ────────────────────

def load_profiles() -> dict:
    path = Path(_env("SEASONAL_PILOTS_FILE", str(HERE / "config" / "seasonal_pilots.json")))
    if not path.exists():
        return {}
    with open(path) as fh:
        return {str(p["pilot_id"]): p for p in json.load(fh).get("pilots", [])}


def resolve_pilot(pilot_id) -> dict:
    raw = str(pilot_id or "").strip()
    if not raw.isdigit() or not (4 <= len(raw) <= 7):
        raise ValidationError("Enter the pilot ID as a number, for example 10106.")
    p = load_profiles().get(raw)
    if p:
        return {**p, "pilot_id": int(raw)}
    return {"pilot_id": int(raw), "name": f"Pilot {raw}"}


# ─── Pure helpers ───────────────────────────────────────────────────────────

def to_epoch_ist(send_date: str, send_time: str) -> int:
    """'2026-10-26', '22:30' (IST) -> 1793034000."""
    try:
        d, t = date.fromisoformat(send_date), dtime.fromisoformat(send_time)
    except Exception:
        raise ValidationError("Enter the send date and time.")
    return int(datetime.combine(d, t, tzinfo=IST).timestamp())


def describe_epoch(epoch: int) -> dict:
    ist = datetime.fromtimestamp(epoch, IST)
    utc = datetime.fromtimestamp(epoch, timezone.utc)
    us_east = datetime.fromtimestamp(epoch, ZoneInfo("America/New_York"))
    return {
        "epoch": epoch,
        "ist": ist.strftime("%d %b %Y, %H:%M IST"),
        "utc": utc.strftime("%d %b %Y, %H:%M UTC"),
        "us_east": us_east.strftime("%d %b %Y, %H:%M ") + us_east.tzname(),
    }


def today_folder(now: Optional[datetime] = None) -> str:
    """FOLDER = the day the run is created in OpsHub, in IST (YYYYMMDD),
    matching prod-one-time-scripts/<date>/ for the day the PR is raised."""
    return (now or datetime.now(timezone.utc)).astimezone(IST).strftime("%Y%m%d")


def validate_ticket(ticket: str) -> str:
    ticket = (ticket or "").strip().upper()
    if not TICKET_RE.match(ticket):
        raise ValidationError("Enter the ticket as PROJECT-NUMBER, for example EEOPS-70.")
    return ticket


def parse_uuid_list(raw: bytes) -> dict:
    """One UUID per line (a 'uuid' header line is tolerated and dropped)."""
    text = raw.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    seen, uuids, invalid, dupes = set(), [], [], 0
    for i, r in enumerate(rows):
        v = (r[0] if r else "").strip()
        if not v or (i == 0 and v.lower() == "uuid"):
            continue
        if not UUID_RE.match(v):
            invalid.append(v)
            continue
        if v in seen:
            dupes += 1
            continue
        seen.add(v)
        uuids.append(v)
    if not uuids:
        raise ValidationError("No valid UUIDs in the file. Put one UUID per line.")
    return {"uuids": uuids, "duplicates": dupes, "invalid_count": len(invalid), "invalid_sample": invalid[:5]}


def split_parts(uuids: list, max_rows: int = None) -> list:
    max_rows = max_rows or MAX_ROWS_PER_FILE()
    return [uuids[i:i + max_rows] for i in range(0, len(uuids), max_rows)] or [[]]


def render_csv(uuids: list) -> str:
    return "\n".join(uuids) + "\n"          # UUIDs only, no header


def file_names(pilot_id: str, event_type: str, n_parts: int, existing: set) -> tuple:
    """Named from the pilot ID: ('10106_winter.sh', ['10106_winter.csv']), adding
    _2, _3 ... when the date folder already has a run with that name."""
    label = str(pilot_id)
    label = re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_") or "pilot"
    base = f"{label}_{SEASON.get(event_type, event_type.lower())}"
    existing_l = {e.lower() for e in existing}
    n = 1
    while True:
        stem = base if n == 1 else f"{base}_{n}"
        csvs = ([f"{stem.lower()}.csv"] if n_parts == 1 else
                [f"{stem.lower()}_part{i}.csv" for i in range(1, n_parts + 1)])
        if f"{stem}.sh".lower() not in existing_l and not any(c in existing_l for c in csvs):
            return f"{stem}.sh", csvs
        n += 1


def build_script(*, run_id: str, script_env: str, run_as: str, pilot: dict, event_type: str,
                 epoch: int, skip_subscription_check: bool, csv_files: list, ticket: str,
                 created_by: str, user_count: int) -> str:
    """Same command as the hand-written script. POSIX sh, so it works whether
    Jenkins runs it with sh or bash. Adds a log per CSV part and fails the build
    if the tool exits with an error in its first 30 seconds."""
    when = describe_epoch(epoch)
    L = [
        "#!/bin/bash",
        "# Generated by OpsHub (Seasonal Email tab). Create a new run instead of editing.",
        f"# Run:     {run_id}",
        f"# Pilot:   {pilot['name']} ({pilot['pilot_id']})",
        f"# Event:   {event_type}",
        f"# Send at: {when['ist']} / {when['utc']} / epoch {epoch}",
        f"# Users:   {user_count:,} in {len(csv_files)} file(s)",
        f"# Ticket:  {ticket}",
        f"# By:      {created_by}",
        "",
        f"ENV={script_env}",
        f'USER="{run_as}"',
        'DIR=$(cd "$(dirname "$0")" && pwd)',
        f"TOOL={DEVTOOL_SCRIPT}",
        "",
        'case "$ENV" in',
        "  prod-na|prod-na-2|prod-ca|prod-eu) ;;",
        '  *) echo "Please specify the correct environment. Exiting ..."; exit 1 ;;',
        "esac",
        '[ -x "$TOOL" ] || { echo "Seasonal email tool not found at $TOOL"; exit 1; }',
        "",
    ]
    for i, f in enumerate(csv_files, 1):
        log_name = f.rsplit(".", 1)[0] + ".log"
        L += [
            f'[ -s "$DIR/{f}" ] || {{ echo "User file {f} is missing or empty"; exit 1; }}',
            'sudo -u "$USER" nohup "$TOOL" run'
            f" -scheduledTime {epoch}"
            f" -eventType {event_type}"
            f" -skipUserSubscriptionsCheck {'true' if skip_subscription_check else 'false'}"
            f" -emailProcessorName {EMAIL_PROCESSOR}"
            f' -filePath "$DIR/{f}" > "$DIR/{log_name}" 2>&1 &',
            f"PID{i}=$!",
            f'echo "Started {f} as pid $PID{i}"',
            "",
        ]
    L += ["# Give the tool time to fail fast on bad input, then report.", "sleep 30", "STATUS=0"]
    for i, f in enumerate(csv_files, 1):
        log_name = f.rsplit(".", 1)[0] + ".log"
        L += [
            f'echo "----- {log_name} (last 40 lines) -----"',
            f'tail -n 40 "$DIR/{log_name}" || true',
            f'if kill -0 "$PID{i}" 2>/dev/null; then',
            f'  echo "{f}: still running (pid $PID{i})."',
            "else",
            f'  wait "$PID{i}"; RC=$?',
            f'  echo "{f}: exited with code $RC."',
            '  [ "$RC" -eq 0 ] || STATUS=1',
            "fi",
        ]
    L += ["exit $STATUS", ""]
    return "\n".join(L)


# ─── Run records ────────────────────────────────────────────────────────────

# PR_OPEN -> READY (merged) -> QUEUED -> RUNNING -> SUCCESS | FAILED
# Manual Jenkins: READY -> MANUAL -> SUCCESS | FAILED (marked on the tab)
POLLED_STATES = {"PR_OPEN", "QUEUED", "RUNNING"}


@dataclass
class Run:
    run_id: str
    created_at: str
    created_by: str
    env: str
    pilot_id: str
    pilot_name: str
    event_type: str
    send_date: str
    send_time: str
    epoch: int
    ticket: str
    user_source: str
    filters: dict
    skip_subscription_check: bool
    user_count: int
    folder: str
    script_name: str
    csv_files: list
    state: str = "PR_OPEN"
    pr_number: Optional[int] = None
    pr_url: Optional[str] = None
    files_zip: Optional[str] = None
    target_ip: Optional[str] = None
    target_tag: Optional[str] = None
    jenkins_job: Optional[str] = None
    jenkins_queue_url: Optional[str] = None
    jenkins_build_number: Optional[int] = None
    jenkins_build_url: Optional[str] = None
    manual_params: Optional[dict] = None
    logged_to_history: bool = False
    triggered_by: Optional[str] = None
    triggered_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None
    history: list = field(default_factory=list)

    def note(self, msg: str):
        self.history.append({"at": _now(), "msg": msg})


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class RunStore:
    """JSON per run in S3 (default bucket shared with OpsHub's audit logs), or a
    local folder when SEASONAL_LOCAL_STORE is set (local testing)."""

    def __init__(self):
        self.local = _env("SEASONAL_LOCAL_STORE")
        self.bucket = _env("SEASONAL_S3_BUCKET", "bidgely-support-ps")
        self.prefix = _env("SEASONAL_S3_PREFIX", "opshub/seasonal-email/runs/")
        self._lock = threading.Lock()
        if self.local:
            Path(self.local).mkdir(parents=True, exist_ok=True)

    def _s3(self):
        import boto3
        return boto3.client("s3")

    def _put(self, name: str, data: bytes, ctype: str):
        if self.local:
            (Path(self.local) / name).write_bytes(data)
        else:
            self._s3().put_object(Bucket=self.bucket, Key=self.prefix + name, Body=data,
                                  ContentType=ctype, ServerSideEncryption="AES256")

    def _get(self, name: str) -> bytes:
        if not re.match(r"^[A-Za-z0-9._-]+$", name):
            raise ValidationError("Invalid name.")
        if self.local:
            p = Path(self.local) / name
            if not p.exists():
                raise ValidationError("Not found.")
            return p.read_bytes()
        try:
            return self._s3().get_object(Bucket=self.bucket, Key=self.prefix + name)["Body"].read()
        except Exception as e:
            raise IntegrationError(f"Could not read {name} from S3: {e}")

    def save(self, run: Run):
        with self._lock:
            self._put(f"{run.run_id}.json", json.dumps(asdict(run), indent=2).encode(), "application/json")

    def get(self, run_id: str) -> Run:
        return Run(**json.loads(self._get(f"{run_id}.json")))

    def save_blob(self, name: str, data: bytes):
        self._put(name, data, "application/zip")

    def get_blob(self, name: str) -> bytes:
        return self._get(name)

    def list(self, limit: int = 100) -> list:
        runs = []
        try:
            if self.local:
                for p in Path(self.local).glob("*.json"):
                    runs.append(Run(**json.loads(p.read_text())))
            else:
                s3 = self._s3()
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix):
                    for o in page.get("Contents", []):
                        if o["Key"].endswith(".json"):
                            runs.append(Run(**json.loads(s3.get_object(Bucket=self.bucket, Key=o["Key"])["Body"].read())))
        except Exception as e:
            raise IntegrationError(f"Could not load seasonal email runs: {e}")
        runs.sort(key=lambda r: r.created_at, reverse=True)
        return runs[:limit]


store = RunStore()


# ─── Integrations ───────────────────────────────────────────────────────────

class StarburstClient:
    """Enabled users for a pilot — the same query PS runs by hand. Verified
    24 Nov 2026: identical UUIDs to gold.user_master for 10106 and 10127.

    Connects through starburst_ids.py, exactly like HER Revocation's Starburst
    lookup: same clusters/catalogs per NA2/NA/CA/EU, and the same logins —
      1. a personal Starburst login typed on the tab (used for this query only,
         never stored or logged), else
      2. the shared service account: DB_NA_* / DB_CA_* / DB_EU_* or
         STARBURST_USER / STARBURST_PASSWORD.
    """

    def __init__(self, env_code: str, user: Optional[str] = None, password: Optional[str] = None):
        self.cfg = env_cfg(env_code)
        self.user = (user or "").strip() or None
        self.password = password or None
        if bool(self.user) != bool(self.password):
            raise ValidationError("Enter both your Starburst login and password, or leave both blank "
                                  "to use OpsHub's shared Starburst account.")

    def catalog(self) -> str:
        try:
            import starburst_ids
            return starburst_ids._env_settings(self.cfg["code"])["catalog"]
        except ImportError:
            return self.cfg["catalog"]

    @staticmethod
    def build_query(catalog: str, pilot_id: int, filters: dict, count_only: bool) -> str:
        # pilot_id is validated as digits and user_meta_data.pilot_id is an integer,
        # so it's inlined as an int literal (same approach as starburst_ids.py).
        if not re.match(r"^[a-z_0-9]+$", catalog):
            raise ValidationError("Invalid catalog in configuration.")
        where = [f"pilot_id = {int(pilot_id)}", "user_status = 'ENABLED'"]
        if filters.get("exclude_no_email"):
            where.append("is_emailid_empty = false")
        if filters.get("exclude_notifications_disabled"):
            where.append("coalesce(notification_user_type, '') <> 'DISABLED'")
        if filters.get("exclude_test_users"):
            where.append("coalesce(is_test_user, false) = false")
        sel = "count(DISTINCT uuid)" if count_only else "DISTINCT uuid"
        return f"SELECT {sel} FROM {catalog}.public.user_meta_data WHERE " + " AND ".join(where)

    def _cursor(self):
        try:
            import starburst_ids
        except ImportError:
            raise IntegrationError("starburst_ids.py (HER Revocation's Starburst module) isn't next to seasonal_email.py.")
        try:
            conn = starburst_ids._connect(self.cfg["code"], self.user, self.password)
        except Exception as e:
            msg = str(e)
            if not self.user and not starburst_ids.get_status().get(self.cfg["code"], {}).get("configured"):
                raise ValidationError("No Starburst login: enter your own Starburst login on the tab, or set "
                                      "STARBURST_USER / STARBURST_PASSWORD (same as HER Revocation).")
            raise IntegrationError(f"Couldn’t connect to Starburst ({self.cfg['code']}): {msg}")
        return conn.cursor()

    def _run(self, sql: str):
        cur = self._cursor()
        cur.execute(sql)
        return cur

    def count(self, pilot_id: int, filters: dict) -> int:
        if _flag("SEASONAL_DRY_RUN"):
            return 1200
        try:
            return int(self._run(self.build_query(self.catalog(), pilot_id, filters, True)).fetchone()[0])
        except (ValidationError, IntegrationError):
            raise
        except Exception as e:
            raise IntegrationError(f"Starburst count failed: {e}")

    def fetch(self, pilot_id: int, filters: dict) -> list:
        if _flag("SEASONAL_DRY_RUN"):
            return [str(uuidlib.UUID(int=i + 1)) for i in range(1200)]
        try:
            cur = self._run(self.build_query(self.catalog(), pilot_id, filters, False))
            out = []
            while True:
                rows = cur.fetchmany(50000)
                if not rows:
                    return out
                out.extend(r[0] for r in rows)
        except (ValidationError, IntegrationError):
            raise
        except Exception as e:
            raise IntegrationError(f"Starburst user list failed: {e}")


class GitHubClient:
    API = "https://api.github.com"

    def __init__(self):
        self.repo = _env("SEASONAL_GITHUB_REPO", "bidgely/Utils")
        self.base = _env("SEASONAL_GITHUB_BASE", "master")
        self.token = _env("SEASONAL_GITHUB_TOKEN")
        self.dry = _flag("SEASONAL_DRY_RUN")
        self.dry_dir = Path(_env("SEASONAL_DRY_RUN_DIR", "/tmp/opshub_seasonal_dryrun"))

    def _call(self, method, path, ok404=False, **kw):
        if not self.token:
            raise IntegrationError("GitHub token is not configured (SEASONAL_GITHUB_TOKEN).")
        r = requests.request(method, f"{self.API}/repos/{self.repo}{path}", timeout=120,
                             headers={"Authorization": f"Bearer {self.token}",
                                      "Accept": "application/vnd.github+json"}, **kw)
        if ok404 and r.status_code == 404:
            return None
        if r.status_code >= 300:
            raise IntegrationError(f"GitHub {method} {path} failed ({r.status_code}): {r.text[:300]}")
        return r.json()

    def existing_files(self, folder: str) -> set:
        """Names already in prod-one-time-scripts/<folder>/ on the base branch."""
        if self.dry:
            d = self.dry_dir / SCRIPTS_DIR() / folder
            return {p.name for p in d.iterdir()} if d.exists() else set()
        if not self.token:
            return set()
        items = self._call("GET", f"/contents/{SCRIPTS_DIR()}/{folder}?ref={self.base}", ok404=True) or []
        return {i["name"] for i in items if isinstance(i, dict)}

    def open_pr(self, folder: str, files: dict, title: str, body: str, branch: str) -> tuple:
        """files: {name: (content, executable)} -> (pr_number, pr_url)."""
        if self.dry:
            d = self.dry_dir / SCRIPTS_DIR() / folder
            d.mkdir(parents=True, exist_ok=True)
            for name, (content, exe) in files.items():
                (d / name).write_text(content)
                if exe:
                    (d / name).chmod(0o755)
            return 0, f"file://{d}"
        base_sha = self._call("GET", f"/git/ref/heads/{self.base}")["object"]["sha"]
        base_tree = self._call("GET", f"/git/commits/{base_sha}")["tree"]["sha"]
        tree = []
        for name, (content, exe) in files.items():
            blob = self._call("POST", "/git/blobs", json={
                "content": base64.b64encode(content.encode()).decode(), "encoding": "base64"})
            tree.append({"path": f"{SCRIPTS_DIR()}/{folder}/{name}", "mode": "100755" if exe else "100644",
                         "type": "blob", "sha": blob["sha"]})
        new_tree = self._call("POST", "/git/trees", json={"base_tree": base_tree, "tree": tree})["sha"]
        commit = self._call("POST", "/git/commits", json={"message": title, "tree": new_tree,
                                                          "parents": [base_sha]})["sha"]
        self._call("POST", "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit})
        pr = self._call("POST", "/pulls", json={"title": title, "head": branch, "base": self.base, "body": body})
        return pr["number"], pr["html_url"]

    def pr_state(self, number: int) -> str:
        if self.dry:
            return "merged"
        pr = self._call("GET", f"/pulls/{number}")
        return "merged" if pr.get("merged") else pr.get("state", "open")


# server.py sets this to add finished runs to OpsHub's shared Run History tab.
RUN_HISTORY_WRITER: Optional[Callable[[dict], None]] = None

# server.py sets this to use _get_cross_account_client and <ENV>_DESTINATION_ROLE_ARN.
EC2_CLIENT_FACTORY: Optional[Callable[[str, str], object]] = None   # (env_code, region) -> ec2 client


class DevToolsLocator:
    """Live DevTools server for an environment. Servers rotate weekly, so the IP
    is read from the EC2 Name tag at trigger time."""

    def __init__(self, env_code: str):
        self.cfg = env_cfg(env_code)
        self.verified = True

    def _ec2(self):
        if EC2_CLIENT_FACTORY:
            return EC2_CLIENT_FACTORY(self.cfg["code"], self.cfg["aws_region"])
        import boto3
        return boto3.client("ec2", region_name=self.cfg["aws_region"])

    def live_ips(self) -> list:
        if _flag("SEASONAL_DRY_RUN"):
            return ["127.0.0.1"]
        try:
            resp = self._ec2().describe_instances(Filters=[
                {"Name": "tag:Name", "Values": [self.cfg["devtools_tag"]]},
                {"Name": "instance-state-name", "Values": ["running"]}])
        except Exception as e:
            raise IntegrationError(f"Couldn’t look up the DevTools server in {self.cfg['code']}: {e}")
        return sorted({i["PrivateIpAddress"] for r in resp["Reservations"] for i in r["Instances"]
                       if i.get("PrivateIpAddress")})

    def resolve(self, override: Optional[str] = None) -> str:
        override = (override or "").strip() or None
        if override and not IPV4_RE.match(override):
            raise ValidationError("Enter the IP as a plain address, for example 10.2.3.6.")
        try:
            ips = self.live_ips()
        except IntegrationError as e:
            if override:
                self.verified = False
                return override
            raise ValidationError(f"{e} Enter the DevTools IP in the IP box.")
        tag = self.cfg["devtools_tag"]
        if override:
            if override not in ips:
                raise ValidationError(f"{override} isn’t a running {tag} server in {self.cfg['code']}. "
                                      f"Live: {', '.join(ips) or 'none'}.")
            return override
        if len(ips) == 1:
            return ips[0]
        if not ips:
            raise ValidationError(f"No running {tag} server in {self.cfg['code']}. A deployment may be in progress.")
        raise ValidationError(f"More than one live {tag} server ({', '.join(ips)}). Enter the one to use.")


class JenkinsClient:
    def __init__(self):
        self.base = _env("SEASONAL_JENKINS_URL", "http://jenkins.bidgely.com").rstrip("/")
        self.auth = (_env("SEASONAL_JENKINS_USER", ""), _env("SEASONAL_JENKINS_TOKEN", ""))

    def job_url(self, job: str) -> str:
        return f"{self.base}/job/{job}/"

    def trigger(self, job: str, params: dict) -> str:
        try:
            r = requests.post(f"{self.base}/job/{job}/buildWithParameters", data=params, auth=self.auth,
                              timeout=60, allow_redirects=False)
        except requests.RequestException as e:
            raise IntegrationError(f"Couldn’t reach Jenkins: {e}")
        if r.status_code not in (200, 201):
            raise IntegrationError(f"Jenkins rejected the build ({r.status_code}): {r.text[:300]}")
        loc = r.headers.get("Location")
        if not loc:
            raise IntegrationError("Jenkins accepted the build but returned no queue location.")
        return loc if loc.endswith("/") else loc + "/"

    def queue_status(self, queue_url: str) -> dict:
        r = requests.get(f"{queue_url}api/json", auth=self.auth, timeout=30)
        r.raise_for_status()
        return r.json()

    def build_status(self, build_url: str) -> dict:
        r = requests.get(f"{build_url.rstrip('/')}/api/json?tree=building,result,number,url", auth=self.auth, timeout=30)
        r.raise_for_status()
        return r.json()

    def console(self, build_url: str) -> str:
        r = requests.get(f"{build_url.rstrip('/')}/consoleText", auth=self.auth, timeout=30)
        r.raise_for_status()
        text = r.text
        for secret in filter(None, [self.auth[1]] + [env_cfg(c)["api_token"] for c in ENVIRONMENTS]):
            text = text.replace(secret, "****")
        return text[-20000:]


class JiraClient:
    def __init__(self):
        self.base = _env("SEASONAL_JIRA_URL", "https://bidgely.atlassian.net").rstrip("/")
        self.auth = (_env("SEASONAL_JIRA_EMAIL", ""), _env("SEASONAL_JIRA_TOKEN", ""))
        self.dry = _flag("SEASONAL_DRY_RUN")

    def check(self, key: str, pilot: dict) -> dict:
        if not modes()["jira"]:
            return {"summary": "", "status": "not checked",
                    "warnings": ["Jira isn’t connected yet, so the ticket wasn’t checked and won’t be updated."]}
        if self.dry:
            return {"summary": f"{pilot['name']} seasonal email (dry run)", "status": "Open", "warnings": []}
        try:
            r = requests.get(f"{self.base}/rest/api/2/issue/{key}?fields=summary,status", auth=self.auth, timeout=30)
        except requests.RequestException as e:
            raise IntegrationError(f"Couldn’t reach Jira: {e}")
        if r.status_code == 404:
            raise ValidationError(f"Ticket {key} doesn’t exist, or OpsHub can’t see it.")
        if r.status_code >= 300:
            raise IntegrationError(f"Jira lookup failed ({r.status_code}).")
        f = r.json()["fields"]
        summary, status = f.get("summary", ""), f["status"]["name"]
        warnings = []
        if f["status"].get("statusCategory", {}).get("key") == "done":
            warnings.append(f"{key} is already {status}. Updates will still be posted to it.")
        names = [pilot["name"], str(pilot["pilot_id"])] + pilot.get("aliases", [])
        if not any(n and n.lower() in summary.lower() for n in names):
            warnings.append(f"{key} (“{summary}”) doesn’t mention {pilot['name']}. Check it’s the right ticket.")
        return {"summary": summary, "status": status, "warnings": warnings}

    def comment(self, key: str, text: str):
        if not modes()["jira"]:
            return
        if self.dry:
            log.info("[dry-run] Jira comment on %s:\n%s", key, text)
            return
        try:
            r = requests.post(f"{self.base}/rest/api/2/issue/{key}/comment", json={"body": text},
                              auth=self.auth, timeout=30)
            if r.status_code >= 300:
                log.warning("Jira comment on %s failed (%s)", key, r.status_code)
        except requests.RequestException as e:
            log.warning("Jira comment on %s failed: %s", key, e)


# ─── Flow ───────────────────────────────────────────────────────────────────

def _yes(v) -> bool:
    return str(v or "").lower() in ("1", "true", "on", "yes")


def validate_request(form: dict, now_epoch: Optional[int] = None) -> dict:
    pilot = resolve_pilot(form.get("pilot_id"))
    cfg = env_cfg(form.get("env"))
    event_type = form.get("event_type", "")
    if event_type not in EVENT_TYPES:
        raise ValidationError("Choose the season.")
    epoch = to_epoch_ist(form.get("send_date", ""), form.get("send_time", ""))
    lead = int(_env("SEASONAL_MIN_LEAD_MINUTES", "30")) * 60
    if epoch < (now_epoch or int(time.time())) + lead:
        raise ValidationError(f"Pick a send time at least {lead // 60} minutes from now, "
                              "so there’s time to review and merge the PR.")
    source = form.get("user_source", "starburst")
    if source not in ("starburst", "upload"):
        raise ValidationError("Choose where the user list comes from.")
    skip = _yes(form.get("skip_subscription_check"))
    if skip and not _yes(form.get("skip_ack")):
        raise ValidationError("Confirm that skipping the subscription check is intended.")
    filters = {k: _yes(form.get(k)) for k in ("exclude_no_email", "exclude_notifications_disabled", "exclude_test_users")}
    return {"pilot": pilot, "cfg": cfg, "env": cfg["code"], "event_type": event_type, "epoch": epoch,
            "send_date": form["send_date"], "send_time": form["send_time"],
            "ticket": validate_ticket(form.get("ticket", "")),
            "user_source": source, "filters": filters, "skip": skip}


def count_users(form: dict) -> dict:
    """Live count for the User List card: needs only pilot ID, environment and
    filters (no ticket/date), so the tab can show it as soon as those are set."""
    pilot = resolve_pilot(form.get("pilot_id"))
    cfg = env_cfg(form.get("env"))
    filters = {k: _yes(form.get(k)) for k in ("exclude_no_email", "exclude_notifications_disabled", "exclude_test_users")}
    sb = StarburstClient(cfg["code"], form.get("sb_user"), form.get("sb_password"))
    count = sb.count(pilot["pilot_id"], filters)
    enabled = sb.count(pilot["pilot_id"], {}) if any(filters.values()) else count
    return {"pilot_name": pilot["name"], "env": cfg["code"], "catalog": sb.catalog(),
            "user_count": count, "enabled_count": enabled}


def preview(form: dict, upload: Optional[bytes]) -> dict:
    v = validate_request(form)
    when = describe_epoch(v["epoch"])
    out = {"when": when, "env": v["env"], "script_env": v["cfg"]["script_env"],
           "jenkins_job": v["cfg"]["jenkins_job"], "pilot_name": v["pilot"]["name"]}
    if v["user_source"] == "upload":
        if not upload:
            raise ValidationError("Choose the file with the user list.")
        stats = parse_uuid_list(upload)
        out.update(user_count=len(stats["uuids"]), duplicates=stats["duplicates"],
                   invalid_count=stats["invalid_count"], invalid_sample=stats["invalid_sample"])
    else:
        sb = StarburstClient(v["env"], form.get("sb_user"), form.get("sb_password"))
        out["user_count"] = sb.count(v["pilot"]["pilot_id"], v["filters"])
        out["enabled_count"] = sb.count(v["pilot"]["pilot_id"], {})
    parts = max(1, -(-out["user_count"] // MAX_ROWS_PER_FILE()))
    folder = today_folder()
    existing = GitHubClient().existing_files(folder)
    script, csvs = file_names(str(v["pilot"]["pilot_id"]), v["event_type"], parts, existing)
    out.update(parts=parts, folder=folder, script_name=script, csv_files=csvs,
               path=f"{SCRIPTS_DIR()}/{folder}/",
               ticket=JiraClient().check(v["ticket"], v["pilot"]))
    return out


def create_run(form: dict, upload: Optional[bytes], user: str) -> Run:
    v = validate_request(form)
    pilot, cfg = v["pilot"], v["cfg"]
    JiraClient().check(v["ticket"], pilot)

    if v["user_source"] == "upload":
        if not upload:
            raise ValidationError("Choose the file with the user list.")
        uuids = parse_uuid_list(upload)["uuids"]
    else:
        uuids = StarburstClient(v["env"], form.get("sb_user"), form.get("sb_password")).fetch(pilot["pilot_id"], v["filters"])
        if not uuids:
            raise ValidationError("Starburst returned no users for this pilot and these filters.")

    parts = split_parts(uuids)
    when = describe_epoch(v["epoch"])
    folder = today_folder()
    gh = GitHubClient()
    script_name, csv_names = file_names(str(pilot["pilot_id"]), v["event_type"],
                                        len(parts), gh.existing_files(folder))
    run_id = f"{datetime.now(timezone.utc):%Y%m%d}-{uuidlib.uuid4().hex[:8]}"

    script = build_script(run_id=run_id, script_env=cfg["script_env"], run_as=cfg["run_as"], pilot=pilot,
                          event_type=v["event_type"], epoch=v["epoch"], skip_subscription_check=v["skip"],
                          csv_files=csv_names, ticket=v["ticket"], created_by=user, user_count=len(uuids))
    files = {script_name: (script, True)}
    for name, part in zip(csv_names, parts):
        files[name] = (render_csv(part), False)

    run = Run(run_id=run_id, created_at=_now(), created_by=user, env=v["env"],
              pilot_id=str(pilot["pilot_id"]), pilot_name=pilot["name"], event_type=v["event_type"],
              send_date=v["send_date"], send_time=v["send_time"], epoch=v["epoch"], ticket=v["ticket"],
              user_source=v["user_source"], filters=v["filters"], skip_subscription_check=v["skip"],
              user_count=len(uuids), folder=folder, script_name=script_name, csv_files=csv_names,
              jenkins_job=cfg["jenkins_job"])

    src = "Starburst" if v["user_source"] == "starburst" else "uploaded list"
    run.note(f"Run created by {user}: {len(uuids):,} users from {src}, {cfg['script_env']}, "
             f"send {when['ist']}, {SCRIPTS_DIR()}/{folder}/{script_name}, "
             f"skip subscription check {'on' if v['skip'] else 'off'}.")
    if not modes()["github"]:
        run.files_zip = f"{run_id}.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for name, (content, exe) in files.items():
                info = zipfile.ZipInfo(f"{SCRIPTS_DIR()}/{folder}/{name}")
                info.external_attr = (0o100755 if exe else 0o100644) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                z.writestr(info, content)
        store.save_blob(run.files_zip, buf.getvalue())
        run.note(f"Files ready: add {SCRIPTS_DIR()}/{folder}/{script_name} and its CSV to Utils in a PR.")
        store.save(run)
        JiraClient().comment(v["ticket"], _ticket_text(run, "Seasonal email run created in OpsHub", []))
        return run

    title = f"{pilot['name']} {SEASON[v['event_type']]} seasonal email – {v['ticket']}"
    body = (f"Created from the OpsHub Seasonal Email tab by {user} (run `{run_id}`).\n\n"
            f"- Pilot: {pilot['name']} ({pilot['pilot_id']}), {cfg['script_env']}\n"
            f"- Event: {v['event_type']}\n- Send at: {when['ist']} (epoch {v['epoch']})\n"
            f"- Users: {len(uuids):,} from {v['user_source']} in {len(parts)} file(s)\n"
            f"- Skip subscription check: {v['skip']}\n- Ticket: {v['ticket']}\n"
            f"- Jenkins: {cfg['jenkins_job']} with FOLDER={folder}, SCRIPT={script_name}\n")
    try:
        run.pr_number, run.pr_url = gh.open_pr(folder, files, title, body,
                                               branch=f"opshub/seasonal-{folder}-{script_name[:-3].lower()}-{run_id[-8:]}")
    except IntegrationError as e:
        run.state, run.error = "ERROR", str(e)
        run.note(f"PR creation failed: {e}")
        _add_to_run_history(run)
        store.save(run)
        raise
    run.note(f"PR opened: {run.pr_url}")
    store.save(run)
    JiraClient().comment(v["ticket"], _ticket_text(run, "Seasonal email run created in OpsHub",
                                                    [f"PR to review and merge: {run.pr_url}"]))
    return run


def _add_to_run_history(run: Run):
    """Adds a finished run to OpsHub's Run History tab, once."""
    if run.logged_to_history or not RUN_HISTORY_WRITER or run.state not in ("SUCCESS", "FAILED", "ERROR"):
        return
    ok = run.state == "SUCCESS"
    try:
        RUN_HISTORY_WRITER({
            "user": run.triggered_by or run.created_by,
            "username": run.triggered_by or run.created_by,
            "type": "Seasonal Email",
            "ticket": run.ticket,
            "env": run.env,
            "queue": f"{run.jenkins_job} · {run.folder}/{run.script_name}",
            "measurement": run.event_type,
            "entries": run.user_count,
            "sent": run.user_count if ok else 0,
            "failed": 0 if ok else run.user_count,
            "status": "OK" if ok else "FAILED",
        })
        run.logged_to_history = True
        run.note("Added to Run History.")
    except Exception as e:
        log.warning("Couldn't add seasonal run %s to Run History: %s", run.run_id, e)
        run.note(f"Couldn’t add to Run History: {e}")


def refresh_run(run: Run) -> Run:
    try:
        if run.state == "PR_OPEN" and run.pr_number is not None:
            st = GitHubClient().pr_state(run.pr_number)
            if st == "merged":
                run.state = "READY"
                run.note("PR merged. Ready to trigger.")
            elif st == "closed":
                run.state = "CANCELLED"
                run.note("PR closed without merging.")
        elif run.state == "QUEUED" and run.jenkins_queue_url:
            q = JenkinsClient().queue_status(run.jenkins_queue_url)
            if q.get("cancelled"):
                run.state, run.error = "FAILED", "Jenkins build was cancelled in the queue."
            elif q.get("executable"):
                run.jenkins_build_number = q["executable"]["number"]
                run.jenkins_build_url = q["executable"]["url"]
                run.state = "RUNNING"
                run.note(f"Jenkins build #{run.jenkins_build_number} started.")
        elif run.state == "RUNNING" and run.jenkins_build_url:
            b = JenkinsClient().build_status(run.jenkins_build_url)
            if not b.get("building"):
                ok = b.get("result") == "SUCCESS"
                run.state, run.finished_at = ("SUCCESS" if ok else "FAILED"), _now()
                if not ok:
                    run.error = f"Jenkins build finished with {b.get('result')}."
                run.note(f"Jenkins build #{run.jenkins_build_number}: {b.get('result')}.")
                JiraClient().comment(run.ticket, _ticket_text(
                    run, "Seasonal emails scheduled" if ok else "Seasonal email run failed",
                    [f"Jenkins build: {run.jenkins_build_url}"]))
                _add_to_run_history(run)
    except (IntegrationError, requests.RequestException) as e:
        run.note(f"Status check failed: {e}")
    store.save(run)
    return run


def mark_pr_merged(run: Run, user: str, pr_url: Optional[str]) -> Run:
    if run.state != "PR_OPEN" or run.pr_number is not None:
        raise ValidationError("This run’s PR is tracked automatically.")
    if pr_url:
        if not re.match(r"^https://github\.com/[\w.-]+/[\w.-]+/pull/\d+/?$", pr_url.strip()):
            raise ValidationError("Paste the PR link, for example https://github.com/bidgely/Utils/pull/17200.")
        run.pr_url = pr_url.strip()
    run.state = "READY"
    run.note(f"PR marked merged by {user}.")
    store.save(run)
    return run


def trigger_run(run: Run, user: str, ip_override: Optional[str] = None) -> Run:
    if run.state != "READY":
        raise ValidationError("Only runs whose PR is merged can be triggered.")
    if run.epoch < int(time.time()) + 60:
        raise ValidationError("The send time has passed. Create a new run with a new time.")
    cfg = env_cfg(run.env)
    loc = DevToolsLocator(run.env)
    ip = loc.resolve(ip_override)
    params = {"FOLDER": run.folder, "SCRIPT": run.script_name, "IP": f"{ip} {cfg['devtools_tag']}",
              "JIRA_REQUEST_ID": SREINFRA_ID()}
    if not SREINFRA_RE.match(params["JIRA_REQUEST_ID"]):
        raise IntegrationError("SEASONAL_SREINFRA_ID must look like SREINFRA-1234.")
    run.target_ip, run.target_tag = ip, cfg["devtools_tag"]
    run.triggered_by, run.triggered_at = user, _now()
    unverified = "" if loc.verified else " (typed in, not checked against AWS)"

    if not modes()["jenkins"]:
        run.manual_params = {"job_url": JenkinsClient().job_url(cfg["jenkins_job"]) + "build",
                             **params, "API_TOKEN": "leave as is (already saved in the job)"}
        run.state = "MANUAL"
        run.note(f"Jenkins parameters prepared for {cfg['jenkins_job']} on {ip}{unverified} by {user}.")
        store.save(run)
        return run

    # Each run-script-*-ps job stores its own API_TOKEN as the parameter's default,
    # so it's only sent when SEASONAL_API_TOKEN_<ENV> is set (to override it).
    if cfg["api_token"]:
        params = {**params, "API_TOKEN": cfg["api_token"]}
    run.jenkins_queue_url = JenkinsClient().trigger(cfg["jenkins_job"], params)
    run.state = "QUEUED"
    run.note(f"Triggered {cfg['jenkins_job']} on {ip}{unverified} by {user}.")
    store.save(run)
    JiraClient().comment(run.ticket, _ticket_text(run, "Seasonal email run triggered", [
        f"Jenkins job: {cfg['jenkins_job']} (FOLDER={run.folder}, SCRIPT={run.script_name})",
        f"DevTools server: {ip}", f"Triggered by: {user}"]))
    return run


def mark_result(run: Run, user: str, result: str, build_url: Optional[str]) -> Run:
    if run.state != "MANUAL":
        raise ValidationError("Only runs run by hand in Jenkins can be marked here.")
    if result not in ("SUCCESS", "FAILED"):
        raise ValidationError("Choose whether the build succeeded or failed.")
    if build_url:
        m = re.match(r"^https?://\S+/job/[\w.-]+/(\d+)/?$", build_url.strip())
        if not m:
            raise ValidationError("Paste the Jenkins build link, e.g. http://jenkins.bidgely.com/job/run-script-na-2-ps/1790/.")
        run.jenkins_build_url, run.jenkins_build_number = build_url.strip(), int(m.group(1))
    run.state, run.finished_at = result, _now()
    if result == "FAILED":
        run.error = "Jenkins build marked failed."
    run.note(f"Marked {result.lower()} by {user}" + (f" (build {run.jenkins_build_url})" if run.jenkins_build_url else "") + ".")
    _add_to_run_history(run)
    store.save(run)
    JiraClient().comment(run.ticket, _ticket_text(
        run, "Seasonal emails scheduled" if result == "SUCCESS" else "Seasonal email run failed",
        [f"Jenkins build: {run.jenkins_build_url}"] if run.jenkins_build_url else []))
    return run


def _ticket_text(run: Run, headline: str, extra: list) -> str:
    when = describe_epoch(run.epoch)
    labels = {"exclude_no_email": "no-email users excluded",
              "exclude_notifications_disabled": "notification-disabled users excluded",
              "exclude_test_users": "test users excluded"}
    f = [labels[k] for k, on in run.filters.items() if on and k in labels]
    return "\n".join([
        f"*{headline}* (OpsHub run {run.run_id})",
        f"Pilot: {run.pilot_name} ({run.pilot_id}), {env_cfg(run.env)['script_env']}",
        f"Event: {run.event_type}",
        f"Send at: {when['ist']} ({when['utc']})",
        f"Users: {run.user_count:,} from {run.user_source}" + (f" ({', '.join(f)})" if f else ""),
        f"Skip subscription check: {'yes' if run.skip_subscription_check else 'no'}",
        f"Files: {SCRIPTS_DIR()}/{run.folder}/{run.script_name}",
        f"Created by: {run.created_by}",
    ] + extra)


# ─── Background poller ──────────────────────────────────────────────────────

_poller_started = False


def start_poller(interval: int = None):
    global _poller_started
    if _poller_started:
        return
    _poller_started = True
    interval = interval or int(_env("SEASONAL_POLL_SECONDS", "60"))

    def loop():
        while True:
            try:
                for r in store.list(limit=200):
                    if r.state in POLLED_STATES:
                        refresh_run(r)
            except Exception:
                log.exception("Seasonal email poller error")
            time.sleep(interval)

    threading.Thread(target=loop, name="seasonal-email-poller", daemon=True).start()


# ─── Routes (registered by server.py with OpsHub's own access decorator) ───

def register_routes(app, guard, get_user: Callable[[], str], ec2_client_factory=None,
                    run_history_writer=None, poll: bool = True):
    """guard = admin_or_ps_required (same access as HER Revocation).
    run_history_writer(entry) adds a finished run to the shared Run History tab."""
    from flask import jsonify, request, send_file
    global EC2_CLIENT_FACTORY, RUN_HISTORY_WRITER
    if ec2_client_factory:
        EC2_CLIENT_FACTORY = ec2_client_factory
    if run_history_writer:
        RUN_HISTORY_WRITER = run_history_writer

    def handle(fn):
        def inner(*a, **kw):
            try:
                return fn(*a, **kw)
            except ValidationError as e:
                return jsonify({"status": "error", "message": str(e)}), 400
            except IntegrationError as e:
                log.warning("Seasonal email: %s", e)
                return jsonify({"status": "error", "message": str(e)}), 502
        inner.__name__ = f"seasonal_{fn.__name__}"
        return guard(inner)

    def run_json(run):
        return jsonify({"status": "ok", "run": asdict(run)})

    def upload_bytes():
        f = request.files.get("user_file")
        return f.read() if f else None

    @handle
    def config():
        pilots = sorted(load_profiles().values(), key=lambda p: p["name"])
        try:
            import starburst_ids
            sb = {c: v["configured"] for c, v in starburst_ids.get_status().items()}
        except Exception:
            sb = {}
        return jsonify({"status": "ok", "environments": {c: env_cfg(c)["script_env"] for c in ENVIRONMENTS},
                        "event_types": EVENT_TYPES, "pilots": pilots, "modes": modes(),
                        "dry_run": _flag("SEASONAL_DRY_RUN"), "scripts_dir": SCRIPTS_DIR(),
                        "starburst_shared_login": sb})

    @handle
    def do_preview():
        return jsonify({"status": "ok", **preview(request.form.to_dict(), upload_bytes())})

    @handle
    def do_count():
        return jsonify({"status": "ok", **count_users(request.form.to_dict())})

    @handle
    def create():
        return run_json(create_run(request.form.to_dict(), upload_bytes(), get_user()))

    @handle
    def list_runs():
        return jsonify({"status": "ok", "runs": [asdict(r) for r in store.list()]})

    @handle
    def get_run(run_id):
        return run_json(store.get(run_id))

    @handle
    def refresh(run_id):
        return run_json(refresh_run(store.get(run_id)))

    @handle
    def target(run_id):
        run = store.get(run_id)
        loc = DevToolsLocator(run.env)
        try:
            ips, err = loc.live_ips(), None
        except IntegrationError as e:
            ips, err = [], f"{e} Enter the IP in the IP box."
        return jsonify({"status": "ok", "ips": ips, "error": err, "tag": loc.cfg["devtools_tag"],
                        "job": loc.cfg["jenkins_job"]})

    @handle
    def trigger(run_id):
        body = request.get_json(silent=True) or {}
        return run_json(trigger_run(store.get(run_id), get_user(), body.get("ip_override")))

    @handle
    def pr_merged(run_id):
        body = request.get_json(silent=True) or {}
        return run_json(mark_pr_merged(store.get(run_id), get_user(), body.get("pr_url")))

    @handle
    def mark(run_id):
        body = request.get_json(silent=True) or {}
        return run_json(mark_result(store.get(run_id), get_user(), body.get("result", ""), body.get("build_url")))

    @handle
    def console(run_id):
        run = store.get(run_id)
        text = JenkinsClient().console(run.jenkins_build_url) if run.jenkins_build_url and modes()["jenkins"] else ""
        return jsonify({"status": "ok", "console": text})

    @handle
    def files(run_id):
        run = store.get(run_id)
        if not run.files_zip:
            raise ValidationError("This run’s files were committed to GitHub directly.")
        return send_file(io.BytesIO(store.get_blob(run.files_zip)), mimetype="application/zip",
                         as_attachment=True, download_name=f"{run.folder}_{run.script_name[:-3]}.zip")

    routes = [
        ("/api/seasonal/config", config, ["GET"]),
        ("/api/seasonal/preview", do_preview, ["POST"]),
        ("/api/seasonal/count", do_count, ["POST"]),
        ("/api/seasonal/runs", create, ["POST"]),
        ("/api/seasonal/runs", list_runs, ["GET"]),
        ("/api/seasonal/runs/<run_id>", get_run, ["GET"]),
        ("/api/seasonal/runs/<run_id>/refresh", refresh, ["POST"]),
        ("/api/seasonal/runs/<run_id>/target", target, ["GET"]),
        ("/api/seasonal/runs/<run_id>/trigger", trigger, ["POST"]),
        ("/api/seasonal/runs/<run_id>/pr-merged", pr_merged, ["POST"]),
        ("/api/seasonal/runs/<run_id>/mark", mark, ["POST"]),
        ("/api/seasonal/runs/<run_id>/console", console, ["GET"]),
        ("/api/seasonal/runs/<run_id>/files", files, ["GET"]),
    ]
    for rule, view, methods in routes:
        app.add_url_rule(rule, endpoint=f"{view.__name__}_{methods[0].lower()}", view_func=view, methods=methods)
    if poll:
        start_poller()
