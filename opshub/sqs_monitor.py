"""
SQS Queues monitor — backs the ops-hub "SQS Queues" tab.

Ported from the standalone OpsRadar dashboard (its queue view only). For the
main pipeline queues in NA, NA2, EU and CA it reports:
  - messages available / in flight          (sqs:GetQueueAttributes)
  - processed/min and incoming/min           (cloudwatch:GetMetricData, AWS/SQS)
  - boxes running for each queue             (autoscaling:DescribeAutoScalingGroups,
                                              ec2:DescribeInstances — see queue_boxes.json)
colour-coded against per-service thresholds, with adhoc queues tracked as free/in use.

AWS identity: server.py installs a client factory (set_client_factory) so this
uses the SAME identities as dispatch — the pod's own role for NA/NA2, and the
<ENV>_DESTINATION_ROLE_ARN cross-account role for EU/CA. Each of those identities
needs the read-only permissions above in its own account. Without a factory (the
CLI below, run on a laptop) it falls back to QM_PROFILE_<NA|EU|CA> /
QM_ROLE_ARN_<NA|EU|CA> or default credentials, same as OpsRadar did.

CLI (run next to this file, with AWS credentials):
    python3 sqs_monitor.py boxes     -> which ASG / EC2 instances each queue matched
"""

import os
import re
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_SECONDS = int(os.getenv("QM_CACHE_SECONDS", "30"))
M = 1_000_000


# --------------------------------------------------------------------------- thresholds
# Colour rule on "Messages available":
#   available >  red            -> red
#   available >= yellow         -> yellow
#   otherwise                   -> green
GROUPS = [
    {"id": "ingestion",    "label": "Ingestion",       "yellow": 1 * M, "red": 2 * M},
    {"id": "aggregation",  "label": "Aggregation",     "yellow": 3 * M, "red": 4 * M},
    {"id": "disagg",       "label": "Disaggregation",  "yellow": 2 * M, "red": 3 * M},
    {"id": "pdf",          "label": "PDF generation",  "yellow": 2 * M, "red": 3 * M},
    {"id": "notification", "label": "Notifications",   "yellow": 2 * M, "red": 3 * M},
    {"id": "email",        "label": "Email",           "yellow": 2 * M, "red": 3 * M},
    # Adhoc queues: shown as their own rows. Thresholds copied from the matching main stage; tune as needed.
    {"id": "adhoc_ingestion",   "label": "Adhoc ingestion",   "yellow": 1 * M, "red": 2 * M, "adhoc": True},
    {"id": "adhoc_aggregation", "label": "Adhoc aggregation", "yellow": 3 * M, "red": 4 * M, "adhoc": True},
    {"id": "adhoc_disagg",      "label": "Adhoc disagg",      "yellow": 2 * M, "red": 3 * M, "adhoc": True},
]

# --------------------------------------------------------------------------- queues

ENVS = {
    "NA":  {"region": "us-east-1",    "account": "857283459404", "cred": "NA"},
    "NA2": {"region": "us-east-1",    "account": "857283459404", "cred": "NA"},
    "EU":  {"region": "eu-central-1", "account": "967871724166", "cred": "EU"},
    "CA":  {"region": "ca-central-1", "account": "076900401824", "cred": "CA"},
}

DISAGG_FULL = ["", "0", "00", "01", "010", "011", "012", "013", "014", "015",
               "02", "03", "04", "05", "06", "07", "08", "09",
               "1", "10", "11", "12", "13", "14", "15"]
DISAGG_SMALL = ["", "0", "00", "01", "1"]


def _series(base, suffixes):
    return [base if s == "" else f"{base}-{s}" for s in suffixes]


QUEUE_SPEC = [  # (env, group, base name, queue names)
    ("NA",  "ingestion", "GBUserDataIngestion-prod-na",   ["GBUserDataIngestion-prod-na"]),
    ("NA2", "ingestion", "GBUserDataIngestion-prod-na-2", ["GBUserDataIngestion-prod-na-2"]),
    ("EU",  "ingestion", "GBUserDataIngestion-prod-eu",   ["GBUserDataIngestion-prod-eu"]),
    ("CA",  "ingestion", "GBUserDataIngestion-prod-ca",   ["GBUserDataIngestion-prod-ca"]),

    ("NA",  "aggregation", "AggregationReadyUsers-prod-na",
        _series("AggregationReadyUsers-prod-na", ["", "01", "02", "03", "04", "07", "08"])),
    ("NA2", "aggregation", "AggregationReadyUsers-prod-na-2",
        _series("AggregationReadyUsers-prod-na-2", ["", "01", "02", "03", "04", "05", "08"])),
    ("EU",  "aggregation", "AggregationReadyUsers-prod-eu", ["AggregationReadyUsers-prod-eu"]),
    ("CA",  "aggregation", "AggregationReadyUsers-prod-ca", ["AggregationReadyUsers-prod-ca"]),

    ("NA",  "disagg", "GbTempDataReadyEventPyAmi-prod-na",
        _series("GbTempDataReadyEventPyAmi-prod-na", DISAGG_FULL)),
    ("NA2", "disagg", "GbTempDataReadyEventPyAmi-prod-na-2",
        _series("GbTempDataReadyEventPyAmi-prod-na-2", DISAGG_FULL)),
    ("EU",  "disagg", "GbTempDataReadyEventPyAmi-prod-eu",
        _series("GbTempDataReadyEventPyAmi-prod-eu", DISAGG_SMALL)),
    ("CA",  "disagg", "GbTempDataReadyEventPyAmi-prod-ca",
        _series("GbTempDataReadyEventPyAmi-prod-ca", DISAGG_SMALL)),

    ("NA",  "pdf",          "PdfGeneration-prod-na",                 ["PdfGeneration-prod-na"]),
    ("NA2", "pdf",          "PdfGeneration-prod-na-2",               ["PdfGeneration-prod-na-2"]),
    ("NA",  "notification", "NotificationsProcessorEvent-prod-na",   ["NotificationsProcessorEvent-prod-na"]),
    ("NA2", "notification", "NotificationsProcessorEvent-prod-na-2", ["NotificationsProcessorEvent-prod-na-2"]),
    ("NA",  "email",        "GenericEventData-prod-na",              ["GenericEventData-prod-na"]),
    ("NA2", "email",        "GenericEventData-prod-na-2",            ["GenericEventData-prod-na-2"]),

    # Adhoc queues (base name is the main queue, so the tile shows the adhoc suffix)
    ("NA",  "adhoc_ingestion",   "GBUserDataIngestion-prod-na",         ["GBUserDataIngestion-prod-na-adhoc"]),
    ("NA2", "adhoc_ingestion",   "GBUserDataIngestion-prod-na-2",       ["GBUserDataIngestion-prod-na-2-adhoc2"]),
    ("EU",  "adhoc_ingestion",   "GBUserDataIngestion-prod-eu",         ["GBUserDataIngestion-prod-eu-adhoceu"]),
    ("CA",  "adhoc_ingestion",   "GBUserDataIngestion-prod-ca",         ["GBUserDataIngestion-prod-ca-adhoc-ca"]),

    ("NA",  "adhoc_aggregation", "AggregationReadyUsers-prod-na",       ["AggregationReadyUsers-prod-na-adhoc"]),
    ("NA2", "adhoc_aggregation", "AggregationReadyUsers-prod-na-2",     ["AggregationReadyUsers-prod-na-2-adhoc2"]),
    ("EU",  "adhoc_aggregation", "AggregationReadyUsers-prod-eu",       ["AggregationReadyUsers-prod-eu-adhoc-prodeu"]),
    ("CA",  "adhoc_aggregation", "AggregationReadyUsers-prod-ca",       ["AggregationReadyUsers-prod-ca-adhoc-ca"]),

    ("NA",  "adhoc_disagg",      "GbTempDataReadyEventPyAmi-prod-na",   ["GbTempDataReadyEventPyAmi-prod-na-adhoc"]),
    ("NA2", "adhoc_disagg",      "GbTempDataReadyEventPyAmi-prod-na-2", ["GbTempDataReadyEventPyAmi-prod-na-2-adhoc2"]),
    ("EU",  "adhoc_disagg",      "GbTempDataReadyEventPyAmi-prod-eu",   ["GbTempDataReadyEventPyAmi-prod-eu-adhoc-prodeu"]),
    ("CA",  "adhoc_disagg",      "GbTempDataReadyEventPyAmi-prod-ca",   ["GbTempDataReadyEventPyAmi-prod-ca-adhoc-ca"]),
]


def _build_queues():
    out = []
    for env, group, base, names in QUEUE_SPEC:
        e = ENVS[env]
        for name in names:
            out.append({
                "env": env, "group": group, "name": name,
                "short": name[len(base) + 1:] if name != base else "main",
                "region": e["region"], "cred": e["cred"],
                "url": f"https://sqs.{e['region']}.amazonaws.com/{e['account']}/{name}",
            })
    return out


QUEUES = _build_queues()

# Maps queue_boxes.json "env" values to (credential key, region).
ENV_TO_AWS = {"prod-na": ("NA", "us-east-1"), "prod-na-2": ("NA", "us-east-1"),
              "prod-eu": ("EU", "eu-central-1"), "prod-ca": ("CA", "ca-central-1")}

# --------------------------------------------------------------------------- AWS

_BOTO_CFG = Config(retries={"max_attempts": 2, "mode": "standard"},
                   connect_timeout=3, read_timeout=5)
_clients, _clients_lock = {}, threading.Lock()
_client_factory = None


def set_client_factory(fn):
    """fn(cred, region, service, config) -> boto3 client. Installed by server.py so
    queue monitoring uses the same identities as dispatch (pod role / cross-account
    roles) instead of AWS CLI profiles, which a pod doesn't have."""
    global _client_factory
    _client_factory = fn


def _client(cred, region, service="sqs"):
    if _client_factory is not None:
        return _client_factory(cred, region, service, _BOTO_CFG)
    key, now = (cred, region, service), time.time()
    with _clients_lock:
        hit = _clients.get(key)
        if hit and hit["expires"] > now:
            return hit["client"]
        profile = os.getenv(f"QM_PROFILE_{cred}")
        base = boto3.Session(profile_name=profile) if profile else boto3.Session()
        role = os.getenv(f"QM_ROLE_ARN_{cred}")
        if role:
            c = base.client("sts").assume_role(RoleArn=role, RoleSessionName="opshub-sqs-monitor",
                                               DurationSeconds=3600)["Credentials"]
            session = boto3.Session(aws_access_key_id=c["AccessKeyId"],
                                    aws_secret_access_key=c["SecretAccessKey"],
                                    aws_session_token=c["SessionToken"])
            expires = now + 45 * 60
        else:
            session, expires = base, now + 24 * 3600
        client = session.client(service, region_name=region, config=_BOTO_CFG)
        _clients[key] = {"client": client, "expires": expires}
        return client

def _base_row(q):
    return {k: q[k] for k in ("env", "group", "name", "short", "url")}


def _fetch_one(q):
    row = _base_row(q)
    try:
        a = _client(q["cred"], q["region"]).get_queue_attributes(
            QueueUrl=q["url"],
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        row.update(available=int(a.get("ApproximateNumberOfMessages", 0)),
                   in_flight=int(a.get("ApproximateNumberOfMessagesNotVisible", 0)), error=None)
    except Exception as exc:  # one bad queue shouldn't break the page
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
        row.update(available=None, in_flight=None, error=str(code)[:60])
    return row


def _fetch_throughput():
    """Per-minute Sum of NumberOfMessagesDeleted (processed) and NumberOfMessagesSent (incoming)
    for every queue, latest complete minute. One GetMetricData call per account/region."""
    from datetime import datetime, timedelta, timezone
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)  # skip the in-progress minute
    start = end - timedelta(minutes=15)  # SQS metrics lag a few minutes
    out = {}
    buckets = {}
    for q in QUEUES:
        buckets.setdefault((q["cred"], q["region"]), []).append(q)

    def run(key):
        cred, region = key
        qs = buckets[key]
        queries, ids = [], {}
        for i, q in enumerate(qs):
            for prefix, metric in (("p", "NumberOfMessagesDeleted"), ("i", "NumberOfMessagesSent")):
                qid = f"{prefix}{i}"
                ids[qid] = (q["url"], prefix)
                queries.append({"Id": qid, "ReturnData": True, "MetricStat": {
                    "Metric": {"Namespace": "AWS/SQS", "MetricName": metric,
                               "Dimensions": [{"Name": "QueueName", "Value": q["name"]}]},
                    "Period": 60, "Stat": "Sum"}})
        res = {}
        try:
            cw = _client(cred, region, "cloudwatch")
            for page in cw.get_paginator("get_metric_data").paginate(
                    MetricDataQueries=queries, StartTime=start, EndTime=end,
                    ScanBy="TimestampDescending"):
                for r in page["MetricDataResults"]:
                    url, prefix = ids[r["Id"]]
                    entry = res.setdefault(url, {})
                    if r["Values"] and prefix not in entry:
                        entry[prefix] = int(r["Values"][0])
                        entry["t"] = max(entry.get("t", 0), int(r["Timestamps"][0].timestamp()))
            for q in qs:  # no datapoints in the window = no activity
                e = res.setdefault(q["url"], {})
                e.setdefault("p", 0); e.setdefault("i", 0)
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
            print(f"[sqs-monitor] CloudWatch {cred}/{region}: {code}", flush=True)
            for q in qs:
                res[q["url"]] = {"err": str(code)[:60]}
        return res

    with ThreadPoolExecutor(max_workers=4) as pool:
        for part in pool.map(run, buckets):
            out.update(part)
    return out


def _merge_throughput(rows, tp):
    for r in rows:
        t = tp.get(r["url"], {})
        r["processed_per_min"] = t.get("p")
        r["incoming_per_min"] = t.get("i")
        r["throughput_at"] = t.get("t")
        r["throughput_error"] = t.get("err")
    return rows


def _demo_one(q):
    row = _base_row(q)
    g = next(g for g in GROUPS if g["id"] == q["group"])
    r = random.random()
    avail = (0 if r < 0.3 else random.randint(1, g["yellow"] - 1) if r < 0.8
             else random.randint(g["yellow"], g["red"]) if r < 0.92
             else random.randint(g["red"] + 1, g["red"] * 2))
    proc = 0 if not avail else random.randint(2_000, 90_000)
    row.update(available=avail, in_flight=0 if not avail else random.randint(0, 60_000), error=None,
               processed_per_min=proc, incoming_per_min=max(0, int(proc * random.uniform(0.6, 1.4))),
               boxes_running=(0 if random.random() < 0.08 else random.randint(1, 8)) if random.random() > 0.1 else None,
               boxes_desired=None, boxes_source=f"{q['name']}-asg (demo)", boxes_error=None,
               throughput_at=int(time.time() // 60 * 60 - 180), throughput_error=None)
    if random.random() < 0.015:
        row.update(available=None, in_flight=None, error="AccessDenied")
    return row


_cache, _cache_lock = {"at": 0.0, "payload": None}, threading.Lock()


def _payload(rows, demo=False, fetch_ms=0):
    return {"fetched_at": int(time.time()), "fetch_ms": fetch_ms, "demo": demo,
            "cache_seconds": CACHE_SECONDS, "groups": GROUPS,
            "envs": list(ENVS), "queues": rows}


# --------------------------------------------------------------------------- Running boxes
# EC2 instances working each queue, counted through Auto Scaling Groups.
# Matching, in order:
#   1. queue_boxes.json (next to this file): {"<queue name>": {"asg": ["asg-name", ...]}}
#                                      or {"<queue name>": {"ec2_name": "Name-tag pattern, * allowed"}}
#   2. An ASG named exactly like the queue (ignoring -asg/-worker/-consumer and similar endings)
#   3. An ASG tagged with the queue name (tag keys: QueueName, Queue, SQSQueue, sqs_queue)
# Run "python3 sqs_monitor.py boxes" to see what matched and get suggestions for the rest.
BOXES_FILE = os.path.join(HERE, "queue_boxes.json")
BOX_CACHE_SECONDS = int(os.getenv("QM_BOX_CACHE_SECONDS", "60"))
ASG_AFFIXES = ("asg", "autoscaling", "workers", "worker", "consumers", "consumer", "ec2", "group")
QUEUE_TAG_KEYS = ("queuename", "queue", "sqsqueue", "sqs_queue", "sqs-queue")
_box_cache, _box_lock = {"at": 0.0, "asgs": None}, threading.Lock()


def _norm(name):
    parts = [x for x in re.split(r"[^a-z0-9]+", name.lower()) if x]
    while parts and parts[-1] in ASG_AFFIXES:
        parts.pop()
    while parts and parts[0] in ASG_AFFIXES:
        parts.pop(0)
    return "-".join(parts)


def _load_box_map():
    if not os.path.exists(BOXES_FILE):
        return {}
    try:
        with open(BOXES_FILE) as fh:
            data = json.load(fh)
        return {k.lower(): v for k, v in data.items() if not k.startswith("_")}
    except ValueError as exc:
        print(f"[sqs-monitor] queue_boxes.json isn't valid JSON: {exc}", flush=True)
        return {}


def _list_asgs():
    """{(cred, region): [{name, running, desired, tags}]}, cached."""
    with _box_lock:
        if _box_cache["asgs"] is not None and time.time() - _box_cache["at"] < BOX_CACHE_SECONDS:
            return _box_cache["asgs"]
        out = {}
        for cred, region in {(q["cred"], q["region"]) for q in QUEUES}:
            try:
                rows = []
                for page in _client(cred, region, "autoscaling").get_paginator("describe_auto_scaling_groups").paginate():
                    for g in page["AutoScalingGroups"]:
                        rows.append({
                            "name": g["AutoScalingGroupName"], "desired": g.get("DesiredCapacity", 0),
                            "running": sum(1 for i in g.get("Instances", []) if i.get("LifecycleState") == "InService"),
                            "tags": {t["Key"].lower(): t["Value"] for t in g.get("Tags", [])},
                        })
                out[(cred, region)] = rows
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
                print(f"[sqs-monitor] Boxes {cred}/{region}: {code}", flush=True)
                out[(cred, region)] = {"error": str(code)[:60]}
        _box_cache.update(at=time.time(), asgs=out)
        return out


_ec2_cache = {}


def _alnum(v):
    return re.sub(r"[^a-z0-9]", "", str(v).lower())


def _list_instances(cred, region):
    """Running instances in one account/region as [{"name", "tags"}], cached for BOX_CACHE_SECONDS."""
    key = (cred, region)
    hit = _ec2_cache.get(key)
    if hit and time.time() - hit[0] < BOX_CACHE_SECONDS:
        return hit[1]
    rows = []
    pages = _client(cred, region, "ec2").get_paginator("describe_instances").paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}])
    for page in pages:
        for r in page["Reservations"]:
            for i in r["Instances"]:
                tags = {t["Key"]: t["Value"] for t in i.get("Tags", [])}
                rows.append({"name": tags.get("Name", ""), "tags": tags})
    _ec2_cache[key] = (time.time(), rows)
    return rows


def _ec2_count(cred, region, names, tags=None, env=None):
    """Running instances whose Name matches (wildcards allowed), with optional tag filters.
    env (e.g. prod-na-2) keeps only instances that carry that environment as any tag value
    (prod-na-2, prodna2 and PRODNA2 all match), which separates NA from NA2 boxes with the same Name."""
    import fnmatch
    names = [names] if isinstance(names, str) else list(names)
    want_env = _alnum(env) if env else None
    n = 0
    for inst in _list_instances(cred, region):
        if not any(fnmatch.fnmatchcase(inst["name"].lower(), pat.lower()) for pat in names):
            continue
        if tags and any(str(inst["tags"].get(k, "")).lower() != str(v).lower() for k, v in tags.items()):
            continue
        if want_env and not any(_alnum(v) == want_env for k, v in inst["tags"].items() if k != "Name"):
            continue
        n += 1
    return n


def _match_asgs(q, asgs, box_map):
    """(list of matched ASGs, how) for one queue."""
    m = box_map.get(q["name"].lower())
    if m and m.get("asg"):
        names = {n.lower() for n in m["asg"]}
        return [a for a in asgs if a["name"].lower() in names], "mapped"
    qn = q["name"].lower()
    by_name = [a for a in asgs if _norm(a["name"]) == _norm(q["name"])]
    if by_name:
        return by_name, "auto"
    by_tag = [a for a in asgs if any(a["tags"].get(k, "").lower() == qn for k in QUEUE_TAG_KEYS)]
    return by_tag, ("auto" if by_tag else None)


def fetch_boxes():
    """{queue url: {"running", "desired", "source", "error"}}"""
    asg_by_acct, box_map, out = _list_asgs(), _load_box_map(), {}
    for q in QUEUES:
        asgs = asg_by_acct.get((q["cred"], q["region"]), [])
        if isinstance(asgs, dict):  # account error
            out[q["url"]] = {"running": None, "desired": None, "source": None, "error": asgs["error"]}
            continue
        m = box_map.get(q["name"].lower()) or {}
        if m.get("ec2_name"):
            cred, region = ENV_TO_AWS.get(m.get("env", ""), (q["cred"], q["region"]))
            try:
                env_tag = m.get("env") if m.get("match_env_tag", True) else None
                n = _ec2_count(cred, region, m["ec2_name"], m.get("tags"), env_tag)
                nm = m["ec2_name"] if isinstance(m["ec2_name"], str) else ", ".join(m["ec2_name"])
                where = f"EC2 instances named {nm}" + (f" tagged {m['env']}" if env_tag else "")
                out[q["url"]] = {"running": n, "desired": None, "source": where, "error": None}
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
                out[q["url"]] = {"running": None, "desired": None, "source": None, "error": str(code)[:60]}
            continue
        matched, how = _match_asgs(q, asgs, box_map)
        if not matched:
            out[q["url"]] = {"running": None, "desired": None, "source": None, "error": None}
            continue
        out[q["url"]] = {"running": sum(a["running"] for a in matched), "desired": sum(a["desired"] for a in matched),
                         "source": ", ".join(a["name"] for a in matched) + ("" if how == "mapped" else " (auto-matched)"),
                         "error": None}
    return out


def _merge_boxes(rows, boxes):
    for r in rows:
        b = boxes.get(r["url"], {})
        r["boxes_running"], r["boxes_desired"] = b.get("running"), b.get("desired")
        r["boxes_source"], r["boxes_error"] = b.get("source"), b.get("error")
    return rows


def boxes_cli():
    """python3 sqs_monitor.py boxes -> shows each queue's matched ASGs and suggests ASGs for unmatched queues."""
    asg_by_acct, box_map = _list_asgs(), _load_box_map()
    unmatched = []
    print(f"\n{'Queue':<48} {'Boxes':>7}  Matched to")
    for q in QUEUES:
        asgs = asg_by_acct.get((q["cred"], q["region"]), [])
        if isinstance(asgs, dict):
            print(f"{q['name']:<48} {'error':>7}  {asgs['error']} (check the {q['cred']} AWS profile)")
            continue
        m = box_map.get(q["name"].lower()) or {}
        if m.get("ec2_name"):
            cred, region = ENV_TO_AWS.get(m.get("env", ""), (q["cred"], q["region"]))
            try:
                env_tag = m.get("env") if m.get("match_env_tag", True) else None
                n = _ec2_count(cred, region, m["ec2_name"], m.get("tags"), env_tag)
                nm = m["ec2_name"] if isinstance(m["ec2_name"], str) else ", ".join(m["ec2_name"])
                print(f"{q['name']:<48} {n:>7}  EC2 {nm}" + (f" [{m['env']}]" if env_tag else ""))
            except Exception as exc:
                print(f"{q['name']:<48} {'error':>7}  EC2 lookup failed: {type(exc).__name__}")
            continue
        matched, how = _match_asgs(q, asgs, box_map)
        if matched:
            run = sum(a["running"] for a in matched)
            print(f"{q['name']:<48} {run:>7}  {', '.join(a['name'] for a in matched)}{'' if how == 'mapped' else '  [auto]'}")
        else:
            print(f"{q['name']:<48} {'-':>7}  not matched")
            unmatched.append((q, asgs))
    if unmatched:
        print("\nSuggestions for unmatched queues (ASGs sharing the most name parts):")
        for q, asgs in unmatched:
            env_words = {"prod", "na", "eu", "ca", "dev", "qa", "stage", "staging", "2"}
            words = lambda n: {w for w in re.split(r"[^a-z0-9]+", re.sub(r"([a-z])([A-Z])", r"\1-\2", n).lower()) if w}
            qw = words(q["name"]) - env_words
            qname, qnum = {w for w in qw if not w.isdigit()}, {w for w in qw if w.isdigit()}
            scored = []
            for a_ in asgs:
                aw = words(a_["name"])
                name_hits = len(qname & aw)
                if name_hits:
                    scored.append((name_hits * 10 + len(qnum & aw) * 3, a_))
            ranked = [a_ for _, a_ in sorted(scored, key=lambda x: -x[0])[:3]]
            sug = [f"{a_['name']} ({a_['running']} running)" for a_ in ranked]
            print(f"  {q['name']}: {', '.join(sug) or 'no similar ASG found'}")
        print('\nAdd the right ones to queue_boxes.json, e.g.  "' + unmatched[0][0]["name"] + '": {"asg": ["<asg name>"]}\n')


def get_metrics(force=False):
    with _cache_lock:
        if not force and _cache["payload"] and time.time() - _cache["at"] < CACHE_SECONDS:
            return _cache["payload"]
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=2) as side:
            tp_future = side.submit(_fetch_throughput)
            box_future = side.submit(fetch_boxes)
            with ThreadPoolExecutor(max_workers=16) as pool:
                rows = list(pool.map(_fetch_one, QUEUES))
            rows = _merge_throughput(rows, tp_future.result())
            rows = _merge_boxes(rows, box_future.result())
        p = _payload(rows, fetch_ms=int((time.time() - t0) * 1000))
        errs = [r for r in rows if r["error"]]
        print(f"[sqs-monitor] fetched {len(rows)} queues in {p['fetch_ms']} ms, "
              f"{len(errs)} failed", flush=True)
        for r in errs[:5]:
            print(f"    {r['env']:<4} {r['name']}: {r['error']}", flush=True)
        _cache.update(at=time.time(), payload=p)
        return p


def demo_metrics():
    """Fake numbers for every queue, no AWS calls — for trying the tab locally."""
    return _payload([_demo_one(q) for q in QUEUES], demo=True)


if __name__ == "__main__":
    import sys
    if sys.argv[1:2] == ["boxes"]:
        boxes_cli()
    else:
        print(__doc__)
