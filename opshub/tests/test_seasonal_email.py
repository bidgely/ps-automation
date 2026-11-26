"""Run from the opshub folder:  python3 -m pytest tests/test_seasonal_email.py -q"""
import io
import subprocess
import sys
import threading
import time
import zipfile
from functools import wraps
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "local"))


@pytest.fixture(autouse=True)
def se(monkeypatch, tmp_path):
    monkeypatch.setenv("SEASONAL_DRY_RUN", "1")
    monkeypatch.setenv("SEASONAL_DRY_RUN_DIR", str(tmp_path / "repo"))
    monkeypatch.setenv("SEASONAL_LOCAL_STORE", str(tmp_path / "runs"))
    for c in ("NA", "NA2", "CA", "EU"):
        monkeypatch.setenv(f"SEASONAL_API_TOKEN_{c}", "dummy-token")
    import seasonal_email as m
    m.store = m.RunStore()
    m.EC2_CLIENT_FACTORY = None
    m.RUN_HISTORY_WRITER = None
    return m


def form(**kw):
    f = {"pilot_id": "10106", "env": "NA2", "event_type": "WINTER_ALERT", "send_date": "2031-10-26",
         "send_time": "22:30", "ticket": "EEOPS-70", "user_source": "starburst",
         "skip_subscription_check": "true", "skip_ack": "true"}
    f.update(kw)
    return f


def app_for(se, role="admin"):
    from flask import Flask, jsonify, session
    app = Flask(__name__)
    app.secret_key = "t"

    def admin_or_ps_required(fn):          # same shape as server.py's decorator
        @wraps(fn)
        def wrapper(*a, **kw):
            session.setdefault("user", {"email": "atul@bidgely.com", "role": role})
            if session["user"]["role"] not in ("admin", "user"):
                return jsonify({"status": "error", "message": "Access required"}), 403
            return fn(*a, **kw)
        return wrapper

    se.register_routes(app, admin_or_ps_required, get_user=lambda: session["user"]["email"], poll=False)
    return app.test_client()


# ── time, names, inputs ───────────────────────────────────────────────────

def test_ist_epoch_matches_eeops_70(se):
    assert se.to_epoch_ist("2026-10-26", "22:30") == 1793034000
    d = se.describe_epoch(1793034000)
    assert d["utc"] == "26 Oct 2026, 17:00 UTC" and "13:00 EDT" in d["us_east"]


def test_folder_is_creation_date_in_ist(se):
    from datetime import datetime, timezone
    # 20:00 UTC on 26 Oct is already 27 Oct in IST
    assert se.today_folder(datetime(2026, 10, 26, 20, 0, tzinfo=timezone.utc)) == "20261027"
    assert se.today_folder(datetime(2026, 10, 26, 10, 0, tzinfo=timezone.utc)) == "20261026"


def test_file_names_follow_utils_pattern(se):
    assert se.file_names("10106", "WINTER_ALERT", 1, set()) == ("10106_winter.sh", ["10106_winter.csv"])
    assert se.file_names("10106", "WINTER_ALERT", 1, {"10106_winter.sh", "10106_winter.csv"}) == ("10106_winter_2.sh", ["10106_winter_2.csv"])
    assert se.file_names("10106", "WINTER_ALERT", 1, {"10106_winter.csv"})[0] == "10106_winter_2.sh"
    assert se.file_names("10134", "WINTER_ALERT", 1, {"10106_winter.sh"})[0] == "10134_winter.sh"   # other pilots same day
    assert se.file_names("10200", "SUMMER_ALERT", 2, set()) == ("10200_summer.sh", ["10200_summer_part1.csv", "10200_summer_part2.csv"])


def test_uuid_list(se):
    u1, u2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
    out = se.parse_uuid_list(f"uuid\n{u1}\n{u2}\n{u1}\nbad\n\n".encode())
    assert out["uuids"] == [u1, u2] and out["duplicates"] == 1 and out["invalid_count"] == 1
    assert se.render_csv([u1, u2]) == f"{u1}\n{u2}\n"            # UUIDs only, no header
    with pytest.raises(se.ValidationError):
        se.parse_uuid_list(b"nothing\n")


def test_split(se):
    assert [len(p) for p in se.split_parts(list(range(2_500_000)), 1_000_000)] == [1_000_000, 1_000_000, 500_000]


def test_query(se):
    q = se.StarburstClient.build_query("delta_prodna", 10106, {}, False)
    assert q == "SELECT DISTINCT uuid FROM delta_prodna.public.user_meta_data WHERE pilot_id = 10106 AND user_status = 'ENABLED'"
    with pytest.raises(se.ValidationError):
        se.StarburstClient.build_query("x; drop", 1, {}, True)
    with pytest.raises(ValueError):
        se.StarburstClient.build_query("delta_prodna", "1 OR 1=1", {}, True)


@pytest.fixture
def fake_starburst_ids(monkeypatch):
    """Stand-in for OpsHub's starburst_ids.py (HER Revocation's Starburst module)."""
    import types
    calls = {}

    class Cur:
        def __init__(self): self.rows = []
        def execute(self, sql):
            calls["sql"] = sql
            self.rows = [(42,)] if "count(" in sql else [("u-1",), ("u-2",)]
        def fetchone(self): return self.rows[0]
        def fetchmany(self, n):
            r, self.rows = self.rows, []
            return r

    class Conn:
        def cursor(self): return Cur()

    mod = types.ModuleType("starburst_ids")
    mod.STARBURST_ENV_CONFIG = {"NA2": 0, "NA": 0, "CA": 0, "EU": 0}
    mod.shared = {"NA2": True, "NA": True, "CA": False, "EU": True}

    def _connect(env, user=None, password=None):
        calls["login"] = (env, user, password)
        if not user and not mod.shared[env]:
            raise ValueError("No Starburst credentials")
        return Conn()
    mod._connect = _connect
    mod._env_settings = lambda env: {"catalog": {"NA2": "delta_prodna", "NA": "delta_prodna", "CA": "delta_prodca", "EU": "delta_prodeu"}[env]}
    mod.get_status = lambda: {e: {"configured": mod.shared[e]} for e in mod.shared}
    monkeypatch.setitem(sys.modules, "starburst_ids", mod)
    monkeypatch.setenv("SEASONAL_DRY_RUN", "0")
    return calls


def test_starburst_uses_her_module(se, fake_starburst_ids):
    calls = fake_starburst_ids
    sb = se.StarburstClient("NA2")                                   # shared account
    assert sb.count(10106, {"exclude_no_email": True}) == 42
    assert calls["login"] == ("NA2", None, None)
    assert "FROM delta_prodna.public.user_meta_data WHERE pilot_id = 10106" in calls["sql"]
    assert se.StarburstClient("EU").fetch(10200, {}) == ["u-1", "u-2"]
    assert "delta_prodeu" in calls["sql"]

    personal = se.StarburstClient("CA", " atul@bidgely.com ", "pw")   # personal login
    personal.count(10300, {})
    assert calls["login"] == ("CA", "atul@bidgely.com", "pw") and "delta_prodca" in calls["sql"]

    with pytest.raises(se.ValidationError, match="enter your own Starburst login"):
        se.StarburstClient("CA").count(10300, {})                    # no shared account for CA
    with pytest.raises(se.ValidationError, match="both"):
        se.StarburstClient("NA2", "atul@bidgely.com", "")


def test_personal_login_reaches_starburst_and_isnt_stored(se, fake_starburst_ids, tmp_path):
    c = app_for(se)
    cfg = c.get("/api/seasonal/config").get_json()
    assert cfg["starburst_shared_login"]["CA"] is False
    pv = c.post("/api/seasonal/preview", data=form(env="CA", pilot_id="10300", sb_user="atul@bidgely.com", sb_password="s3cret"))
    # GitHub/Jira are off without dry run; preview still works
    assert pv.status_code == 200, pv.get_json()
    assert pv.get_json()["user_count"] == 42
    assert fake_starburst_ids["login"] == ("CA", "atul@bidgely.com", "s3cret")
    run = c.post("/api/seasonal/runs", data=form(env="CA", pilot_id="10300", sb_user="atul@bidgely.com",
                                                 sb_password="s3cret")).get_json()["run"]
    assert run["user_count"] == 2
    stored = "".join(p.read_text() for p in (tmp_path / "runs").glob("*.json"))
    assert "s3cret" not in stored and "sb_user" not in stored


@pytest.mark.parametrize("bad, msg", [
    ({"pilot_id": "abc"}, "as a number"),
    ({"env": ""}, "Choose the environment"),
    ({"env": "MARS"}, "Choose the environment"),
    ({"event_type": "SPRING"}, "season"),
    ({"send_date": "2020-01-01"}, "minutes from now"),
    ({"send_time": ""}, "send date and time"),
    ({"ticket": "nope"}, "PROJECT-NUMBER"),
    ({"skip_ack": "false"}, "subscription check"),
])
def test_validation(se, bad, msg):
    with pytest.raises(se.ValidationError, match=msg):
        se.validate_request(form(**bad))


def test_any_pilot_id(se):
    v = se.validate_request(form(pilot_id="10200", env="CA"))
    assert v["pilot"]["name"] == "Pilot 10200" and v["cfg"]["jenkins_job"] == "run-script-ca-ps"


def test_env_codes(se):
    assert {c: se.env_cfg(c)["jenkins_job"] for c in se.ENVIRONMENTS} == {
        "NA2": "run-script-na-2-ps", "NA": "run-script-na-ps", "CA": "run-script-ca-ps", "EU": "run-script-prod-eu-ps"}
    assert se.env_cfg("na2")["script_env"] == "prod-na-2"


def test_script_matches_manual_and_runs_under_sh(se, tmp_path):
    s = se.build_script(run_id="r", script_env="prod-na-2", run_as="bprod", pilot={"name": "PPL", "pilot_id": 10106},
                        event_type="WINTER_ALERT", epoch=1793034000, skip_subscription_check=True,
                        csv_files=["ppl_winter_part1.csv", "ppl_winter_part2.csv"], ticket="EEOPS-70",
                        created_by="atul", user_count=1513275)
    for part in ["-scheduledTime 1793034000", "-eventType WINTER_ALERT", "-skipUserSubscriptionsCheck true",
                 "-emailProcessorName SeasonalAlertUserProcessor", '-filePath "$DIR/ppl_winter_part2.csv"',
                 "ENV=prod-na-2", 'USER="bprod"']:
        assert part in s
    p = tmp_path / "t.sh"
    p.write_text(s)
    assert subprocess.run(["sh", "-n", str(p)]).returncode == 0     # plain sh (dash)
    assert subprocess.run(["bash", "-n", str(p)]).returncode == 0


def test_script_behaviour_with_fake_tool(se, tmp_path):
    """Runs the generated script under sh with a fake tool and no sudo, to check
    the health check passes when the tool keeps running and fails when it errors."""
    import re as _re
    for tool_body, expect in (("sleep 60", 0), ("echo boom; exit 3", 1)):
        d = tmp_path / f"t{expect}"
        d.mkdir()
        tool = d / "tool.sh"
        tool.write_text(f"#!/bin/sh\n{tool_body}\n")
        tool.chmod(0o755)
        (d / "u.csv").write_text("11111111-1111-1111-1111-111111111111\n")
        s = se.build_script(run_id="r", script_env="prod-na-2", run_as="x", pilot={"name": "P", "pilot_id": 1},
                            event_type="WINTER_ALERT", epoch=1, skip_subscription_check=False,
                            csv_files=["u.csv"], ticket="T-1", created_by="a", user_count=1)
        s = s.replace(f"TOOL={se.DEVTOOL_SCRIPT}", f"TOOL={tool}").replace('sudo -u "$USER" ', "").replace("sleep 30", "sleep 1")
        (d / "run.sh").write_text(s)
        r = subprocess.run(["sh", str(d / "run.sh")], capture_output=True, text=True, timeout=20)
        assert r.returncode == expect, r.stdout + r.stderr
        subprocess.run(["pkill", "-f", str(tool)])


def test_ip_resolution(se, monkeypatch):
    loc = se.DevToolsLocator("NA2")
    monkeypatch.setattr(loc, "live_ips", lambda: ["10.0.0.5"])
    assert loc.resolve() == "10.0.0.5" and loc.resolve("10.0.0.5") == "10.0.0.5"
    with pytest.raises(se.ValidationError, match="isn’t a running"):
        loc.resolve("10.0.0.9")
    monkeypatch.setattr(loc, "live_ips", lambda: ["10.0.0.5", "10.0.0.6"])
    with pytest.raises(se.ValidationError, match="More than one"):
        loc.resolve()

    def no_aws():
        raise se.IntegrationError("no access")
    monkeypatch.setattr(loc, "live_ips", no_aws)
    assert loc.resolve("10.2.3.6") == "10.2.3.6" and loc.verified is False
    with pytest.raises(se.ValidationError, match="IP box"):
        loc.resolve()


def test_ec2_factory_used(se, monkeypatch):
    monkeypatch.setenv("SEASONAL_DRY_RUN", "0")
    calls = []

    class FakeEC2:
        def describe_instances(self, Filters):
            calls.append(Filters)
            return {"Reservations": [{"Instances": [{"PrivateIpAddress": "10.1.1.1"}]}]}
    se.EC2_CLIENT_FACTORY = lambda code, region: FakeEC2() if (code, region) == ("CA", "ca-central-1") else None
    assert se.DevToolsLocator("CA").live_ips() == ["10.1.1.1"]
    assert calls[0][0] == {"Name": "tag:Name", "Values": ["devtoolservices-od"]}


# ── routes ────────────────────────────────────────────────────────────────

def test_delivery_role_blocked(se):
    c = app_for(se, role="delivery")
    assert c.get("/api/seasonal/config").status_code == 403


@pytest.fixture
def mock_jenkins(monkeypatch):
    monkeypatch.setenv("MOCK_JENKINS_BUILD_SECONDS", "1")
    import importlib
    import mock_jenkins as mj
    importlib.reload(mj)
    from werkzeug.serving import make_server
    srv = make_server("127.0.0.1", 0, mj.app)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv("SEASONAL_JENKINS_URL", f"http://127.0.0.1:{srv.server_port}")
    yield
    srv.shutdown()


def test_end_to_end_automatic(se, mock_jenkins):
    c = app_for(se)
    cfg = c.get("/api/seasonal/config").get_json()
    assert cfg["modes"] == {"github": True, "jenkins": True, "jira": True}
    pv = c.post("/api/seasonal/preview", data=form()).get_json()
    today = se.today_folder()
    assert pv["script_name"] == "10106_winter.sh" and pv["path"] == f"prod-one-time-scripts/{today}/"
    bad = c.post("/api/seasonal/preview", data=form(env=""))
    assert bad.status_code == 400 and bad.get_json()["status"] == "error"

    run = c.post("/api/seasonal/runs", data=form()).get_json()["run"]
    assert run["folder"] == today and run["script_name"] == "10106_winter.sh" and run["created_by"] == "atul@bidgely.com"
    # a second run the same day gets _2
    run2 = c.post("/api/seasonal/runs", data=form()).get_json()["run"]
    assert run2["script_name"] == "10106_winter_2.sh" and run2["csv_files"] == ["10106_winter_2.csv"]

    rid = run["run_id"]
    assert c.post(f"/api/seasonal/runs/{rid}/trigger", json={}).status_code == 400   # not merged yet
    assert c.post(f"/api/seasonal/runs/{rid}/refresh").get_json()["run"]["state"] == "READY"
    run = c.post(f"/api/seasonal/runs/{rid}/trigger", json={}).get_json()["run"]
    assert run["state"] == "QUEUED"
    for _ in range(20):
        run = c.post(f"/api/seasonal/runs/{rid}/refresh").get_json()["run"]
        if run["state"] in ("SUCCESS", "FAILED"):
            break
        time.sleep(0.4)
    assert run["state"] == "SUCCESS"
    con = c.get(f"/api/seasonal/runs/{rid}/console").get_json()["console"]
    assert f"FOLDER={today} SCRIPT=10106_winter.sh" in con and "SREINFRA-1234" in con
    assert "sh -n" in con and ": OK" in con and "dummy-token" not in con
    assert len(c.get("/api/seasonal/runs").get_json()["runs"]) == 2


def test_manual_mode(se, monkeypatch):
    monkeypatch.setenv("SEASONAL_DRY_RUN", "0")
    for k in ("SEASONAL_GITHUB_TOKEN", "SEASONAL_JENKINS_TOKEN", "SEASONAL_JIRA_TOKEN"):
        monkeypatch.delenv(k, raising=False)

    def no_aws(self):
        raise se.IntegrationError("No AWS access.")
    monkeypatch.setattr(se.DevToolsLocator, "live_ips", no_aws)
    c = app_for(se)
    u = b"11111111-1111-1111-1111-111111111111\n"
    data = form(user_source="upload", pilot_id="10200", env="EU")
    data["user_file"] = (io.BytesIO(u), "u.csv")
    run = c.post("/api/seasonal/runs", data=data, content_type="multipart/form-data").get_json()["run"]
    assert run["files_zip"] and run["script_name"] == "10200_winter.sh"
    rid = run["run_id"]
    z = zipfile.ZipFile(io.BytesIO(c.get(f"/api/seasonal/runs/{rid}/files").data))
    today = se.today_folder()
    assert set(z.namelist()) == {f"prod-one-time-scripts/{today}/10200_winter.sh",
                                 f"prod-one-time-scripts/{today}/10200_winter.csv"}
    assert z.read(f"prod-one-time-scripts/{today}/10200_winter.csv") == u
    assert c.post(f"/api/seasonal/runs/{rid}/pr-merged", json={}).get_json()["run"]["state"] == "READY"
    assert c.post(f"/api/seasonal/runs/{rid}/trigger", json={}).status_code == 400   # no IP
    run = c.post(f"/api/seasonal/runs/{rid}/trigger", json={"ip_override": "10.2.3.6"}).get_json()["run"]
    mp = run["manual_params"]
    assert run["state"] == "MANUAL" and mp["IP"] == "10.2.3.6 devtoolservices-od" and mp["FOLDER"] == today
    assert mp["SCRIPT"] == "10200_winter.sh" and mp["JIRA_REQUEST_ID"] == "SREINFRA-1234"
    assert "run-script-prod-eu-ps" in mp["job_url"] and "dummy-token" not in str(mp)
    run = c.post(f"/api/seasonal/runs/{rid}/mark", json={
        "result": "SUCCESS", "build_url": "http://jenkins.bidgely.com/job/run-script-prod-eu-ps/55/"}).get_json()["run"]
    assert run["state"] == "SUCCESS" and run["jenkins_build_number"] == 55


def test_installer_on_copy_of_opshub(tmp_path):
    """Installer against a minimal index.html / server.py shaped like OpsHub's."""
    import shutil
    d = tmp_path / "opshub"
    shutil.copytree(ROOT / "seasonal_install", d / "seasonal_install")
    shutil.copy(ROOT / "seasonal_email.py", d / "seasonal_email.py")
    (d / "index.html").write_text('''<body>
  <div class="nav-tab" data-tab="herrevoke" onclick="switchTab('herrevoke')">
    <span class="tab-icon">✕</span> HER Revocation
  </div>
  <!-- ═══ HER REVOCATION TAB ═══ -->
  <div class="page" id="page-herrevoke"></div>
<script>
const ROLE_CONFIG = {
  admin:    { tabs: ['rerun','herrevoke','admin'], queueFilter: null },
  user:     { tabs: ['rerun','herrevoke','admin'], queueFilter: null },
  delivery: { tabs: ['psdash','rerun'], queueFilter: 'priority' },
};
function switchTab(tab) {
  if (tab === 'herrevoke') { herLoad(); }
}
</script>
</body>''')
    (d / "server.py").write_text('''import boto3
app = None
def admin_or_ps_required(f): return f
def _destination_role_arn(env): return None
def _get_cross_account_client(service, role_arn, region, env, config=None): pass

from seasonal_email import seasonal_bp, start_poller
app.register_blueprint(seasonal_bp)
start_poller()

if __name__ == '__main__':
    pass
''')
    r = subprocess.run([sys.executable, "seasonal_install/install_seasonal.py"], cwd=d, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    html, py = (d / "index.html").read_text(), (d / "server.py").read_text()
    assert 'data-tab="seasonal"' in html and 'id="page-seasonal"' in html and "seLoad()" in html
    assert "'herrevoke','seasonal'" in html.split("delivery")[0] and "'seasonal'" not in html.split("delivery:")[1].split("\n")[0]
    assert "seasonal_bp" not in py and "seasonal_email.register_routes(" in py
    assert py.index("register_routes") < py.index("if __name__")
    r2 = subprocess.run([sys.executable, "seasonal_install/install_seasonal.py"], cwd=d, capture_output=True, text=True)
    assert "updated to the latest version" in r2.stdout and (d / "index.html").read_text() == html
    assert html.count('id="page-seasonal"') == 1 and html.count("data-tab=\"seasonal\"") == 1

    # upgrading an older install replaces the page and script, keeps one copy of everything
    (d / "seasonal_install" / "seasonal_page.html").write_text(
        (d / "seasonal_install" / "seasonal_page.html").read_text().replace("Seasonal Email</div>", "Seasonal Email v2</div>"))
    r3 = subprocess.run([sys.executable, "seasonal_install/install_seasonal.py"], cwd=d, capture_output=True, text=True)
    up = (d / "index.html").read_text()
    assert r3.returncode == 0 and "Seasonal Email v2" in up and up.count('id="page-seasonal"') == 1
    assert up.count("/* Seasonal Email tab") == 1 and up.count("let seConfig") == 1
    assert up.count("if (tab === 'seasonal')") == 1 and up.count("'herrevoke','seasonal'") == 2
    subprocess.run([sys.executable, "seasonal_install/install_seasonal.py", "--undo"], cwd=d, capture_output=True, text=True)
    assert "seasonal_bp" in (d / "server.py").read_text() and 'page-seasonal' not in (d / "index.html").read_text()


def test_live_count_needs_only_pilot_and_env(se):
    c = app_for(se)
    r = c.post("/api/seasonal/count", data={"pilot_id": "10106", "env": "NA2"}).get_json()
    assert r["status"] == "ok" and r["user_count"] == 1200 and r["enabled_count"] == 1200 and r["pilot_name"] == "PPL"
    assert c.post("/api/seasonal/count", data={"pilot_id": "10106"}).status_code == 400          # no env
    assert c.post("/api/seasonal/count", data={"pilot_id": "x", "env": "NA2"}).status_code == 400


def test_live_count_with_filters_and_personal_login(se, fake_starburst_ids):
    c = app_for(se)
    r = c.post("/api/seasonal/count", data={"pilot_id": "10300", "env": "CA", "exclude_no_email": "true",
                                            "sb_user": "atul@bidgely.com", "sb_password": "pw"}).get_json()
    assert r["user_count"] == 42 and r["catalog"] == "delta_prodca"
    assert fake_starburst_ids["login"] == ("CA", "atul@bidgely.com", "pw")
    r = c.post("/api/seasonal/count", data={"pilot_id": "10300", "env": "CA"})
    assert r.status_code == 400 and "own Starburst login" in r.get_json()["message"]



def test_run_history_and_audit_trail(se, monkeypatch):
    written = []
    se.RUN_HISTORY_WRITER = written.append
    monkeypatch.setenv("SEASONAL_DRY_RUN", "0")
    for k in ("SEASONAL_GITHUB_TOKEN", "SEASONAL_JENKINS_TOKEN", "SEASONAL_JIRA_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(se.DevToolsLocator, "live_ips", lambda self: ["10.2.3.6"])
    c = app_for(se)
    data = form(user_source="upload", env="NA")
    data["user_file"] = (io.BytesIO(b"11111111-1111-1111-1111-111111111111\n"), "u.csv")
    rid = c.post("/api/seasonal/runs", data=data, content_type="multipart/form-data").get_json()["run"]["run_id"]
    c.post(f"/api/seasonal/runs/{rid}/pr-merged", json={})
    c.post(f"/api/seasonal/runs/{rid}/trigger", json={})
    assert written == []                                   # nothing until it finishes
    run = c.post(f"/api/seasonal/runs/{rid}/mark", json={
        "result": "SUCCESS", "build_url": "http://jenkins.bidgely.com/job/run-script-na-ps/9/"}).get_json()["run"]
    assert len(written) == 1 and run["logged_to_history"]
    e = written[0]
    assert e["type"] == "Seasonal Email" and e["status"] == "OK" and e["entries"] == 1 and e["sent"] == 1
    assert e["ticket"] == "EEOPS-70" and e["env"] == "NA" and e["measurement"] == "WINTER_ALERT"
    assert e["username"] == "atul@bidgely.com" and "run-script-na-ps" in e["queue"]
    msgs = [h["msg"] for h in run["history"]]
    assert msgs[0].startswith("Run created by atul@bidgely.com: 1 users from uploaded list")
    assert any("PR marked merged by atul@bidgely.com" in m for m in msgs)
    assert any("10.2.3.6" in m and "by atul@bidgely.com" in m for m in msgs)
    assert any(m.startswith("Marked success by atul@bidgely.com (build http") for m in msgs)
    assert msgs[-1] == "Added to Run History."


def test_run_history_failure_is_noted_not_fatal(se, monkeypatch):
    def broken(entry):
        raise RuntimeError("S3 unavailable")
    se.RUN_HISTORY_WRITER = broken
    run = se.Run(run_id="r1", created_at="2026-11-26T00:00:00Z", created_by="a", env="NA2", pilot_id="1",
                 pilot_name="P", event_type="WINTER_ALERT", send_date="2031-01-01", send_time="10:00",
                 epoch=1924992000, ticket="T-1", user_source="upload", filters={}, skip_subscription_check=False,
                 user_count=1, folder="20261126", script_name="1_winter.sh", csv_files=["1_winter.csv"], state="FAILED")
    se._add_to_run_history(run)
    assert not run.logged_to_history and "S3 unavailable" in run.history[-1]["msg"]


def test_installer_upgrades_existing_block_and_adds_run_history(tmp_path):
    import shutil
    d = tmp_path / "opshub"
    shutil.copytree(ROOT / "seasonal_install", d / "seasonal_install")
    shutil.copy(ROOT / "seasonal_email.py", d / "seasonal_email.py")
    (d / "index.html").write_text("""<body>
  <div class="nav-tab" data-tab="herrevoke" onclick="switchTab('herrevoke')">
    <span class="tab-icon">✕</span> HER Revocation
  </div>
          <select class="form-select" id="adm-filter-type" onchange="renderAdminTable()">
            <option value="">All types</option>
            <option value="Rate Comparison">Rate Comparison</option>
          </select>
  <!-- ═══ HER REVOCATION TAB ═══ -->
  <div class="page" id="page-herrevoke"></div>
<script>
const ROLE_CONFIG = {
  admin:    { tabs: ['rerun','herrevoke','admin'], queueFilter: null },
  user:     { tabs: ['rerun','herrevoke','admin'], queueFilter: null },
  delivery: { tabs: ['psdash','rerun'], queueFilter: 'priority' },
};
function switchTab(tab) {
  if (tab === 'herrevoke') { herLoad(); }
}
</script>
</body>""")
    # server.py as it is on Atul's machine today: run log + the previous Seasonal block
    (d / "server.py").write_text("""import boto3
RUN_LOG_S3_KEY = 'opshub/run_log.json'
_run_log_lock = None
def _save_run_log(entries): pass
def _prune_run_log(entries): return entries
app = None
def admin_or_ps_required(f): return f
def _destination_role_arn(env): return None
def _get_cross_account_client(service, role_arn, region, env, config=None): pass


# ─── SEASONAL EMAIL TAB ──────────────────────────────────────────
# Lives in seasonal_email.py (next to this file), like HER Revocation.
import seasonal_email


def _seasonal_ec2_client(env_code, region):
    return boto3.client('ec2', region_name=region)


seasonal_email.register_routes(
    app, admin_or_ps_required,
    get_user=lambda: session['user']['email'],
    ec2_client_factory=_seasonal_ec2_client,
)


if __name__ == '__main__':
    pass
""")
    r = subprocess.run([sys.executable, "seasonal_install/install_seasonal.py"], cwd=d, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    py, html = (d / "server.py").read_text(), (d / "index.html").read_text()
    assert "updated to the latest version" in r.stdout
    assert py.count("seasonal_email.register_routes(") == 1 and py.count("# ─── SEASONAL EMAIL TAB") == 1
    assert "run_history_writer=_seasonal_run_history" in py and "def _seasonal_run_history" in py
    assert py.index("register_routes") < py.index("if __name__")
    compile(py, "server.py", "exec")
    assert '<option value="Seasonal Email">Seasonal Email</option>' in html
    # running again changes nothing
    subprocess.run([sys.executable, "seasonal_install/install_seasonal.py"], cwd=d, capture_output=True, text=True)
    assert (d / "server.py").read_text() == py and (d / "index.html").read_text() == html



def test_api_token_optional_uses_job_default(se, monkeypatch):
    sent = {}

    class FakeJenkins:
        def trigger(self, job, params):
            sent.update(params, job=job)
            return "http://jenkins/queue/item/1/"
        def job_url(self, job): return f"http://jenkins/job/{job}/"
    monkeypatch.setattr(se, "JenkinsClient", FakeJenkins)
    monkeypatch.delenv("SEASONAL_API_TOKEN_CA", raising=False)
    c = app_for(se)
    rid = c.post("/api/seasonal/runs", data=form(env="CA", pilot_id="10300")).get_json()["run"]["run_id"]
    c.post(f"/api/seasonal/runs/{rid}/refresh")
    run = c.post(f"/api/seasonal/runs/{rid}/trigger", json={}).get_json()["run"]
    assert run["state"] == "QUEUED" and sent["job"] == "run-script-ca-ps"
    assert "API_TOKEN" not in sent and set(sent) == {"job", "FOLDER", "SCRIPT", "IP", "JIRA_REQUEST_ID"}
    monkeypatch.setenv("SEASONAL_API_TOKEN_CA", "override")
    rid = c.post("/api/seasonal/runs", data=form(env="CA", pilot_id="10301")).get_json()["run"]["run_id"]
    c.post(f"/api/seasonal/runs/{rid}/refresh")
    c.post(f"/api/seasonal/runs/{rid}/trigger", json={})
    assert sent["API_TOKEN"] == "override"
