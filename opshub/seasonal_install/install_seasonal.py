"""
Adds the Seasonal Email tab to OpsHub. Run from the opshub folder:

    python3 seasonal_install/install_seasonal.py          # apply
    python3 seasonal_install/install_seasonal.py --undo   # restore the backups

Changes (backups saved as index.html.bak-seasonal and server.py.bak-seasonal):
  index.html  - "Seasonal Email" nav tab after HER Revocation
              - 'seasonal' added to ROLE_CONFIG for admin and user (same as herrevoke)
              - switchTab() loads the tab when opened
              - the tab's page, before the HER Revocation page
              - the tab's script, before </body>
              - "Seasonal Email" added to the Run History tab's Type filter
  server.py   - removes the earlier test lines (seasonal_bp / start_poller), if present
              - registers the Seasonal Email routes with admin_or_ps_required and
                connects finished runs to Run History, just before
                `if __name__ == '__main__':` (or updates the block if it's there)
Safe to run again: it updates the tab's page and script to the latest version
and skips anything else that's already there.
"""
import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
INDEX, SERVER = ROOT / "index.html", ROOT / "server.py"
BAK = ".bak-seasonal"

NAV_TAB = '''  <div class="nav-tab" data-tab="seasonal" onclick="switchTab('seasonal')" title="Schedule summer / winter seasonal emails">
    <span class="tab-icon">❄</span> Seasonal Email
  </div>
'''

CSS = '''<style>
  /* Seasonal Email tab: primary button in the accent colour used across OpsHub */
  .se-btn-primary { background: var(--accent, var(--accent2)); border: 1px solid var(--accent, var(--accent2)); color: #fff; }
  .se-btn-primary:disabled { opacity: .45; cursor: not-allowed; }
</style>
'''

SERVER_BLOCK = '''
# ─── SEASONAL EMAIL TAB ──────────────────────────────────────────
# Lives in seasonal_email.py (next to this file), like HER Revocation. Same
# access as HER Revocation (admin + PS). The DevTools IP lookup uses the same
# cross-account roles as SQS (<ENV>_DESTINATION_ROLE_ARN) when one is set, and
# finished runs are added to the shared Run History tab.
import seasonal_email


def _seasonal_ec2_client(env_code, region):
    try:
        role_arn = _destination_role_arn(env_code)
    except Exception:
        role_arn = None
    if role_arn:
        return _get_cross_account_client('ec2', role_arn, region, env_code)
    return boto3.client('ec2', region_name=region)
__RUN_HISTORY_FN__


seasonal_email.register_routes(
    app, admin_or_ps_required,
    get_user=lambda: session['user']['email'],
    ec2_client_factory=_seasonal_ec2_client,__RUN_HISTORY_ARG__
)
# ─── END SEASONAL EMAIL TAB ──────────────────────────────────────

'''

RUN_HISTORY_FN = '''

def _seasonal_run_history(entry):
    # Same file the other tabs write to (RUN_LOG_S3_KEY). Unlike _load_run_log,
    # a failed S3 read is NOT treated as an empty log here, so a hiccup can
    # never overwrite the existing Run History with just this one entry.
    entry = {'id': uuid.uuid4().hex, 'timestamp': datetime.now(timezone.utc).isoformat(), **entry}
    with _run_log_lock:
        try:
            obj = get_s3_client().get_object(Bucket=RUN_LOG_S3_BUCKET, Key=RUN_LOG_S3_KEY)
            entries = json.loads(obj['Body'].read())
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') not in ('NoSuchKey', '404'):
                raise
            entries = []
        entries.insert(0, entry)
        _save_run_log(_prune_run_log(entries))'''



def fail(msg):
    print(f"\n✗ {msg}\nNothing was changed. Paste this message to Claude.")
    sys.exit(1)


HER_PAGE_RE = re.compile(r"^[ \t]*<!--[^\n]*HER REVOCATION TAB[^\n]*-->", re.M)


def strip_old_ui(html: str) -> tuple:
    """Removes a previously installed Seasonal Email page and script so they can
    be replaced with the current version. Nav tab, ROLE_CONFIG and switchTab stay."""
    removed = False
    m = re.search(r"^[ \t]*<!--[^\n]*SEASONAL EMAIL TAB[^\n]*-->", html, re.M)
    if m:
        her = HER_PAGE_RE.search(html, m.end()) or re.search(r'^[ \t]*<div class="page" id="page-herrevoke"', html[m.end():], re.M)
        if not her:
            fail("Found the old Seasonal Email page but not the HER Revocation page after it.")
        end = her.start() if her.re is HER_PAGE_RE else m.end() + her.start()
        html = html[:m.start()] + html[end:]
        removed = True
    s = html.find("<style>\n  /* Seasonal Email tab")
    if s >= 0:
        e = html.find("</script>\n", s)
        if e < 0:
            fail("Found the old Seasonal Email styles but not the end of its script.")
        html = html[:s] + html[e + len("</script>\n"):]
        removed = True
    return html, removed


def patch_index(html: str) -> str:
    html, upgraded = strip_old_ui(html)

    # 1. nav tab right after the HER Revocation tab
    if 'data-tab="seasonal"' not in html:
        m = re.search(r'<div class="nav-tab"[^>]*data-tab="herrevoke"[^>]*>.*?</div>\s*\n', html, re.S)
        if not m:
            fail("Couldn't find the HER Revocation nav tab (data-tab=\"herrevoke\") in index.html.")
        html = html[:m.end()] + NAV_TAB + html[m.end():]

    # 2. ROLE_CONFIG: same roles as herrevoke
    def add_role(match):
        line = match.group(0)
        return line if "'seasonal'" in line else line.replace("'herrevoke',", "'herrevoke','seasonal',", 1)
    new, n = re.subn(r"^\s*(admin|user)\s*:\s*\{\s*tabs:\s*\[[^\]]*'herrevoke',[^\]]*\].*$", add_role, html, flags=re.M)
    if n < 1:
        fail("Couldn't find 'herrevoke' in ROLE_CONFIG for admin/user in index.html.")
    html = new

    # 3. switchTab hook
    if "if (tab === 'seasonal')" not in html:
        m = re.search(r"^(\s*)if \(tab === 'herrevoke'\).*$", html, re.M)
        if not m:
            fail("Couldn't find the herrevoke line in switchTab() in index.html.")
        html = html[:m.end()] + f"\n{m.group(1)}if (tab === 'seasonal') {{ seLoad(); }}" + html[m.end():]

    # 4. page section before the HER Revocation page
    page = (HERE / "seasonal_page.html").read_text()
    m = HER_PAGE_RE.search(html) or re.search(r'^[ \t]*<div class="page" id="page-herrevoke"', html, re.M)
    if not m:
        fail("Couldn't find the HER Revocation page (id=\"page-herrevoke\") in index.html.")
    html = html[:m.start()] + page + html[m.start():]

    # 5. Run History tab: "Seasonal Email" in its Type filter
    if 'value="Seasonal Email"' not in html:
        m = re.search(r'(<select[^>]*id="adm-filter-type"[^>]*>.*?)(\n?[ \t]*</select>)', html, re.S)
        if m:
            indent = re.search(r'\n([ \t]*)<option', m.group(1))
            pad = indent.group(1) if indent else "            "
            html = html[:m.end(1)] + f'\n{pad}<option value="Seasonal Email">Seasonal Email</option>' + html[m.end(1):]
        else:
            print("• Run History type filter not found — skipped adding Seasonal Email to it")

    # 6. styles + script before </body>
    js = (HERE / "seasonal_tab.js").read_text()
    i = html.lower().rfind("</body>")
    if i < 0:
        fail("Couldn't find </body> in index.html.")
    html = html[:i] + CSS + "<script>\n" + js + "\n</script>\n" + html[i:]
    print("✓ index.html: Seasonal Email tab " + ("updated to the latest version" if upgraded else
          "added (tab, role access, switchTab hook, page and script)"))
    return html


def server_block(py: str) -> str:
    has_history = all(x in py for x in ("def _save_run_log", "def _prune_run_log", "_run_log_lock", "RUN_LOG_S3_KEY"))
    block = SERVER_BLOCK.replace("__RUN_HISTORY_FN__", RUN_HISTORY_FN if has_history else "")
    block = block.replace("__RUN_HISTORY_ARG__", "\n    run_history_writer=_seasonal_run_history," if has_history else "")
    if not has_history:
        print("• server.py has no shared Run History (run_log) here — Seasonal runs won't be added to it")
    return block


def patch_server(py: str) -> str:
    # remove the earlier standalone-page test lines
    old = re.compile(r"\n?from seasonal_email import seasonal_bp, start_poller\napp\.register_blueprint\(seasonal_bp\)\nstart_poller\(\)\n")
    py, n = old.subn("\n", py)
    if n:
        print("✓ server.py: removed the earlier test lines (seasonal_bp)")
    for needed in ("def admin_or_ps_required", "def _get_cross_account_client", "def _destination_role_arn"):
        if needed not in py:
            fail(f"server.py has no `{needed}`, which the Seasonal Email routes use.")
    block = server_block(py)

    # replace an existing Seasonal Email block (any earlier version)
    m = re.search(r"^# ─── SEASONAL EMAIL TAB ─+\n.*?^seasonal_email\.register_routes\(\n.*?^\)\n(# ─── END SEASONAL EMAIL TAB ─+\n)?",
                  py, re.S | re.M)
    if m:
        py = py[:m.start()] + block.strip("\n") + "\n\n\n" + py[m.end():].lstrip("\n")
        print("✓ server.py: Seasonal Email routes updated to the latest version")
        return py

    if not re.search(r"^import boto3|^from boto3|^\s*import boto3", py, re.M):
        py = "import boto3\n" + py
    m = re.search(r"^if __name__ == ['\"]__main__['\"]\s*:", py, re.M)
    if m:
        py = py[:m.start()] + block.lstrip("\n") + "\n" + py[m.start():]
    else:
        py = py.rstrip("\n") + "\n" + block
    print("✓ server.py: Seasonal Email routes registered (admin_or_ps_required)")
    return py


def main():
    for f in (INDEX, SERVER):
        if not f.exists():
            fail(f"{f.name} not found. Run this from the opshub folder: python3 seasonal_install/install_seasonal.py")
    if "--undo" in sys.argv:
        for f in (INDEX, SERVER):
            b = f.with_name(f.name + BAK)
            if b.exists():
                shutil.copy2(b, f)
                print(f"✓ restored {f.name} from {b.name}")
            else:
                print(f"• no backup for {f.name}")
        return
    if not (ROOT / "seasonal_email.py").exists():
        fail("seasonal_email.py isn't in the opshub folder yet. Copy it there first.")
    html, py = INDEX.read_text(), SERVER.read_text()
    new_html, new_py = patch_index(html), patch_server(py)   # both checked before writing anything
    for f, old, new in ((INDEX, html, new_html), (SERVER, py, new_py)):
        if new != old:
            b = f.with_name(f.name + BAK)
            if not b.exists():
                shutil.copy2(f, b)
            f.write_text(new)
    print("\nDone. Restart OpsHub and open the Seasonal Email tab.")


if __name__ == "__main__":
    main()
