#!/usr/bin/env python3
"""
Adds "BII – Blocker" and "BII – Critical" to the PS Daily Operations Dashboard.
Run from the opshub folder:

    cd ~/ps-automation/opshub
    python3 add_bii_priority_sections.py

What it changes (safe to re-run; stops without changing anything if a file
doesn't look as expected):
  ps_dashboard.py - a filter can now be a JQL query ('jql') as well as a saved
                    filter ('id'); each result carries the Jira link to open it.
                    Two new entries after "BII – Open": BII – Blocker, BII – Critical.
  index.html      - tiles, counts and "open in Jira" links use that link, so
                    JQL-based sections open the right search.
"""
import re
import sys
from pathlib import Path

PY, HTML = Path("ps_dashboard.py"), Path("index.html")
for f in (PY, HTML):
    if not f.exists():
        sys.exit(f"{f} not found. Run this from the opshub folder.")

py, html = PY.read_text(), HTML.read_text()
orig_py, orig_html = py, html


def need(text, anchor, where):
    if anchor not in text:
        sys.exit(f"Couldn't find this in {where}, so nothing was changed:\n  {anchor.strip()[:120]}\n"
                 "Paste this message to Claude.")


# ── ps_dashboard.py ────────────────────────────────────────────────────────
if "'bii-blocker'" not in py:
    need(py, "import requests\n", "ps_dashboard.py")
    py = py.replace("import requests\n", "import requests\nfrom urllib.parse import quote\n", 1)

    need(py, "FILTERS = [\n", "ps_dashboard.py")
    py = py.replace("FILTERS = [\n", '''# Open BII incidents (project 19255), excluding ones pending on CSM/utility and the
# automated BRE email-monitoring tickets. Same query as the "BII – Open" view.
BII_OPEN_JQL = (
    'project = 19255 AND status not in ("Pending Utility Information", "Pending with CSM", '
    'Declined, Closed, Cancelled, Resolved) AND NOT (labels = Revamped_BRE_Email_Monitoring '
    'AND reporter = 712020:c61697c5-092e-451f-b392-f3f947c61023 AND status != "Under investigation")'
)

# Each entry is a saved Jira filter ('id') or a JQL query ('key' + 'jql').
FILTERS = [
''', 1)

    m = re.search(r"^(\s*)\{'id': 46990, 'name': 'BII – Open',[^\n]*\n", py, re.M)
    if not m:
        sys.exit("Couldn't find the 'BII – Open' (46990) entry in ps_dashboard.py, so nothing was changed. "
                 "Paste this message to Claude.")
    pad = m.group(1)
    py = py[:m.end()] + (
        f"{pad}{{'key': 'bii-blocker',  'name': 'BII – Blocker',  'section': 'action', "
        f"'jql': BII_OPEN_JQL + ' AND priority = Blocker'}},\n"
        f"{pad}{{'key': 'bii-critical', 'name': 'BII – Critical', 'section': 'action', "
        f"'jql': BII_OPEN_JQL + ' AND priority = Critical'}},\n"
    ) + py[m.end():]

    old_head = '''    jql = 'filter = %d' % flt['id']
    out = {'id': flt['id'], 'name': flt['name'], 'section': flt['section'],'''
    need(py, old_head, "ps_dashboard.py")
    py = py.replace(old_head, '''    if flt.get('jql'):
        jql = flt['jql']
        url = '%s/issues/?jql=%s' % (JIRA_SITE, quote(jql))
    else:
        jql = 'filter = %d' % flt['id']
        url = '%s/issues/?filter=%d' % (JIRA_SITE, flt['id'])
    out = {'id': flt.get('id') or flt['key'], 'url': url, 'name': flt['name'], 'section': flt['section'],''', 1)

    old_log = "log.warning('Jira count failed for filter %s', flt['id'])"
    if old_log in py:
        py = py.replace(old_log, "log.warning('Jira count failed for filter %s', out['id'])")
    old_log2 = "log.warning('PS dashboard: filter %s failed: %s', flt['id'], e)"
    if old_log2 in py:
        py = py.replace(old_log2, "log.warning('PS dashboard: filter %s failed: %s', out['id'], e)")
else:
    print("• ps_dashboard.py already has the BII Blocker/Critical sections")

# ── index.html ─────────────────────────────────────────────────────────────
html, n = re.subn(r"(?<!f\.url \|\| )psdFilterUrl\(site, f\.id\)", "(f.url || psdFilterUrl(site, f.id))", html)
if not n and "(f.url || psdFilterUrl(site, f.id))" not in html:
    sys.exit("Couldn't find psdFilterUrl(site, f.id) in index.html, so nothing was changed. Paste this message to Claude.")

# sample data (?psdemo=1) gets the two new sections too
demo_anchor = "    F(46990, 'BII – Open', 'action', mk(2, 'Ravi', 'Medium', 3)),\n"
if demo_anchor in html and "'bii-blocker'" not in html:
    html = html.replace(demo_anchor, demo_anchor +
        "    F('bii-blocker', 'BII – Blocker', 'action', mk(1, 'Ravi', 'Blocker', 2)),\n"
        "    F('bii-critical', 'BII – Critical', 'action', mk(1, 'Asha', 'Critical', 4)),\n", 1)

compile(py, "ps_dashboard.py", "exec")
if py != orig_py:
    PY.write_text(py)
    print("✓ ps_dashboard.py: BII – Blocker and BII – Critical added after BII – Open")
if html != orig_html:
    HTML.write_text(html)
    print(f"✓ index.html: links use each section's own Jira search ({n} place(s))")
print("Done. Restart OpsHub locally to check, then commit ps_dashboard.py and index.html.")
