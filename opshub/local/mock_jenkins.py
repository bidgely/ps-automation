"""
Mock Jenkins for local testing of the Seasonal Email flow. No VPN needed.

Mimics run-script-na-ps / run-script-na-2-ps / run-script-ca-ps / run-script-prod-eu-ps:
requires FOLDER, SCRIPT, API_TOKEN, IP and JIRA_REQUEST_ID (SREINFRA-<n>), queues the
build, and reports SUCCESS after a few seconds. If SEASONAL_DRY_RUN_DIR is set it also
runs `bash -n` on the generated script to catch syntax errors.

    python local/mock_jenkins.py          # listens on http://127.0.0.1:8099
    MOCK_JENKINS_FAIL=1 python ...        # every build finishes FAILURE
"""
import os
import re
import subprocess
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, request

app = Flask(__name__)
JOBS = {"run-script-na-ps", "run-script-na-2-ps", "run-script-ca-ps", "run-script-prod-eu-ps"}
REQUIRED = ["FOLDER", "SCRIPT", "IP", "JIRA_REQUEST_ID"]   # API_TOKEN has a saved default in the real jobs
BUILD_SECONDS = int(os.environ.get("MOCK_JENKINS_BUILD_SECONDS", "8"))
queue, builds = {}, {}
counter = {"q": 0, "b": 0}
lock = threading.Lock()


def base():
    return request.host_url.rstrip("/")


@app.post("/job/<job>/buildWithParameters")
def build_with_params(job):
    if job not in JOBS:
        return f"No such job {job}", 404
    p = request.form.to_dict()
    missing = [k for k in REQUIRED if not p.get(k)]
    if missing:
        return f"Missing parameters: {missing}", 400
    if not re.match(r"^SREINFRA-\d+$", p["JIRA_REQUEST_ID"]):
        return "JIRA_REQUEST_ID must be SREINFRA-{TICKET NUMBER}", 400
    if not re.match(r"^\d+\.\d+\.\d+\.\d+ \S+$", p["IP"]):
        return "IP must be '<ip> <tag-name>'", 400
    with lock:
        counter["q"] += 1
        qid = counter["q"]
        queue[qid] = {"job": job, "params": p, "created": time.time(), "build": None}
    root = base()
    return "", 201, {"Location": f"{root}/queue/item/{qid}/"}


@app.get("/queue/item/<int:qid>/api/json")
def queue_item(qid):
    q = queue.get(qid)
    if not q:
        return "not found", 404
    if q["build"] is None and time.time() - q["created"] > 2:
        with lock:
            counter["b"] += 1
            n = counter["b"]
            q["build"] = n
            builds[n] = {"job": q["job"], "params": q["params"], "started": time.time(), "console": _console(q)}
    if q["build"] is None:
        return jsonify({"cancelled": False, "why": "Waiting for next available executor"})
    n = q["build"]
    return jsonify({"cancelled": False, "executable": {"number": n, "url": f"{base()}/job/{q['job']}/{n}/"}})


def _console(q):
    p = q["params"]
    lines = [f"Started by remote API (mock) for {q['job']}",
             f"FOLDER={p['FOLDER']} SCRIPT={p['SCRIPT']} IP={p['IP']} JIRA_REQUEST_ID={p['JIRA_REQUEST_ID']}",
             "API_TOKEN=" + ("**** (sent by OpsHub)" if p.get("API_TOKEN") else "**** (job default)")]
    root = os.environ.get("SEASONAL_DRY_RUN_DIR")
    scripts_dir = os.environ.get("SEASONAL_SCRIPTS_DIR", "prod-one-time-scripts")
    if root:
        script = Path(root) / scripts_dir / p["FOLDER"] / p["SCRIPT"]
        if script.exists():
            r = subprocess.run(["sh", "-n", str(script)], capture_output=True, text=True)
            lines.append(f"sh -n {script}: {'OK' if r.returncode == 0 else r.stderr.strip()}")
            lines.append("----- script -----")
            lines.append(script.read_text())
        else:
            lines.append(f"Script not found at {script}")
    lines.append("(mock) would ssh to the DevTools server and run the script here")
    return "\n".join(lines)


@app.get("/job/<job>/<int:n>/api/json")
def build(job, n):
    b = builds.get(n)
    if not b:
        return "not found", 404
    done = time.time() - b["started"] > BUILD_SECONDS
    result = None
    if done:
        result = "FAILURE" if os.environ.get("MOCK_JENKINS_FAIL") else "SUCCESS"
    return jsonify({"building": not done, "result": result, "number": n, "url": f"{base()}/job/{job}/{n}/"})


@app.get("/job/<job>/<int:n>/consoleText")
def console(job, n):
    b = builds.get(n)
    if not b:
        return "not found", 404
    done = time.time() - b["started"] > BUILD_SECONDS
    tail = "\nFinished: " + ("FAILURE" if os.environ.get("MOCK_JENKINS_FAIL") else "SUCCESS") if done else "\n(running...)"
    return b["console"] + tail, 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("MOCK_JENKINS_PORT", "8099")))
