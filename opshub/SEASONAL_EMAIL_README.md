# Seasonal Email tab for OpsHub

Adds a **Seasonal Email** tab next to HER Revocation. Same access as HER Revocation (admin + PS).

## What PS does on the tab

1. Ticket (e.g. EEOPS-70), environment (NA2 / NA / CA / EU), pilot ID, season, send date and time in IST.
2. User list: all enabled users from Starburst (`user_meta_data`, `user_status = 'ENABLED'`), or upload a list (one UUID per line). Starburst goes through HER Revocation's `starburst_ids.py`, with the same optional personal login (used once, never saved).
3. **Check Details** shows the user count, epoch, folder, script name and Jenkins job.
4. **Create PR** puts `prod-one-time-scripts/<today in IST, YYYYMMDD>/<pilotId>_<season>.sh` and `<pilotId>_<season>.csv` (e.g. `10106_winter.sh`) in a Utils PR (`_2`, `_3` if that name is already used that day).
5. After merge, **Trigger Jenkins** runs `run-script-<env>-ps` with FOLDER, SCRIPT, IP (live `devtoolservices-od` server) and JIRA_REQUEST_ID = SREINFRA-1234 (API_TOKEN uses the job's saved value).
6. The run shows PR → Merged → Triggered → Running → Scheduled, with the Jenkins console.
7. **Audit** on each run shows who created, merged and triggered it, the DevTools IP, Jenkins build and result, with timestamps.
8. Finished runs (Scheduled or Failed) are added to OpsHub's **Run History** tab as type "Seasonal Email".

Without a GitHub / Jenkins / Jira token, that step is manual instead (download the folder and raise the PR; copy the five Jenkins values; no ticket comments). Adding the token later makes it automatic.

## Files

| File | Put in `ps-automation/opshub/` |
|---|---|
| `seasonal_email.py` | next to `server.py` |
| `config/seasonal_pilots.json` | `config/` (optional defaults per pilot) |
| `seasonal_install/` (3 files) | `seasonal_install/` |
| `tests/test_seasonal_email.py` | `tests/` |
| `local/mock_jenkins.py` | `local/` (local testing only) |

## Install

```bash
cd ~/ps-automation/opshub
python3 seasonal_install/install_seasonal.py        # patches index.html + server.py, keeps .bak-seasonal backups
python3 -m pytest tests/test_seasonal_email.py -q   # 23 passed
```

Run the installer again after updating the files: it replaces the tab's page and script with the new version.
Undo: `python3 seasonal_install/install_seasonal.py --undo`

## Settings (environment variables)

| Variable | Needed for |
|---|---|
| `SEASONAL_JENKINS_USER`, `SEASONAL_JENKINS_TOKEN` | OpsHub triggering Jenkins |
| `SEASONAL_API_TOKEN_NA2`, `_NA`, `_CA`, `_EU` | optional: each job already has its API_TOKEN saved as the default; set only to override it |
| `SEASONAL_GITHUB_TOKEN` | OpsHub opening the Utils PR |
| Same as HER Revocation (`starburst_ids.py`): a personal login typed on the tab, or `STARBURST_USER` / `STARBURST_PASSWORD` (or `DB_NA_*`, `DB_CA_*`, `DB_EU_*`) | "All active users" |
| `SEASONAL_JIRA_EMAIL`, `SEASONAL_JIRA_TOKEN` | ticket checks and comments |
| `SEASONAL_SREINFRA_ID` | default `SREINFRA-1234` |
| `SEASONAL_LOCAL_STORE` | local testing: keep runs on disk instead of S3 |
| `SEASONAL_DRY_RUN=1` | local testing: stub everything |

The IP lookup uses OpsHub's `_get_cross_account_client` with `<ENV>_DESTINATION_ROLE_ARN` when set. That role needs `ec2:DescribeInstances`; if it can't, PS types the IP.

## Local test

```bash
# terminal 1
python3 local/mock_jenkins.py
# terminal 2
export OPSHUB_LOCAL_NO_AUTH=1 SEASONAL_DRY_RUN=1 SEASONAL_LOCAL_STORE=/tmp/opshub_seasonal_runs \
       SEASONAL_JENKINS_URL=http://127.0.0.1:8099 SEASONAL_API_TOKEN_NA2=dummy
python3 server.py      # open http://localhost:8080 → Seasonal Email
```
