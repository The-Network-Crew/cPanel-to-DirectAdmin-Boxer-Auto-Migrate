# BAM — Boxer Auto-Migration (cPanel to DirectAdmin)

Automated, account-by-account cPanel to DirectAdmin migration tooling.

Runs on the **DirectAdmin target host** and handles each user end-to-end: remote `pkgacct` on the cPanel source, `rsync` transfer, DirectAdmin restore + validation, post-restore cleanup, and artifact removal.

## How It Works

Boxer processes accounts **one at a time**, fully completing each before moving to the next:

1. SSH to the source cPanel host and run `pkgacct` for the user.
2. `rsync` the resulting `cpmove` archive to the DA target.
3. Trigger a DirectAdmin restore via the task queue and run validation.
4. Run post-restore ownership and `public_html` ini cleanup (unless `--skip-finalize`).
5. Remove source and target backup artifacts on success.

Failures are isolated — if a user fails, Boxer logs the error and continues with the next user. This avoids per-domain `public_html` rsync complexity by restoring from cPanel packages directly.

## Quick Start

Run as `root` on the DirectAdmin target host, inside `screen` or `tmux`.

**1. Clone the repository:**

```bash
git clone https://github.com/The-Network-Crew/cP-to-DA-Boxer-Auto-Migration-BAM
cd cP-to-DA-Boxer-Auto-Migration-BAM
```

**2. Set up SSH key auth to the source cPanel host:**

```bash
bash boxer.sh --source-host <SOURCE_IP> --add-key
```

**3. Run migrations (auto-discovers all cPanel users by owner):**

```bash
bash boxer.sh --source-host <SOURCE_IP>
```

Or specify users explicitly:

```bash
bash boxer.sh --source-host <SOURCE_IP> --users user1,user2,user3
```

## Boxer Usage

```text
Usage:
  bash boxer.sh [options]

Required:
  --source-host <host>       Source cPanel host/IP reachable by SSH from DA target

SSH key setup:
  --add-key                  Generate an SSH key (if needed), copy it to the source
                             host, verify connectivity, then exit. Re-run boxer
                             afterwards to start the migration.

User selection (optional, auto-discovery if omitted):
  --users <csv>              Comma-separated users (example: user1,user2)
  --users-file <path>        Text file with one username per line

Common options:
  --owner <name>             DA reseller owner (default: admin)
  --source-owner <name>      Match cPanel users by OWNER= value on source
                             (default: same as --owner)
  --target-backup-dir <path> DA-side directory to store cpmove archives
                             (default: /home/<owner>/user_backups)
  --source-user <name>       SSH user on cPanel source (default: root)
  --source-port <port>       SSH port on source (default: 22)
  --source-key <path>        SSH private key for source access
  --source-backup-dir <path> Source directory for pkgacct output
                             (default: /root/boxer_backups)
  --ip-choice <mode>         Restore ip_choice for DA (default: select)
  --ip <addr>                IP when --ip-choice=select
  --log-dir <path>           Per-user boxer logs directory (default: /root/boxer-logs)
  --run-report-base <path>   Restore report base dir (default: /root/boxer-runs/<timestamp>)
  --state-file <path>        Persistent status CSV (default: /root/boxer-runs/boxer-state.csv)
  --strict-validate          Fail user if validation detects any FAIL (default: off)
  --skip-finalize            Skip ownership + public_html ini cleanup phase
  --no-cleanup-source        Keep source cpmove archive after successful migration
  --no-cleanup-target        Keep target cpmove archive after successful migration
  --dry-run                  Print actions only (no remote/restore changes)
  -h, --help                 Show help

Outputs:
  - per user: <log-dir>/boxer-USER.log
  - per user: <log-dir>/boxer-USER.err
  - run state: <state-file>
```

## Script Inventory

| Script | Purpose |
|--------|---------|
| `boxer.sh` | Main orchestrator — remote pkgacct, rsync, restore, validate, finalize, cleanup, per-user split logs. |
| `da-restore-wrap.sh` | Batch restore queue runner + validation + tracker CSV. |
| `da-restore-from-pkgacct.sh` | Builds DA restore task payload from cpmove archives. |
| `da-restore-validate.sh` | Artifact-based restore validation (`PASS`/`WARN`/`FAIL`). |
| `da-restore-ownership.sh` | Post-restore ownership reconciliation + cPanel `.user.ini`/`php.ini` cleanup. |
| `post-boxer.sh` | Optional post-migration maintenance (ini cleanup, error_reporting, ownership fix). |
| `usr/.../cpanel_to_da.sh` | DA-side cpmove-to-DA format converter (called by DA during restore). |

## Standalone Restore Wrapper

If you already have `cpmove` archives on the DA target (not using Boxer's remote flow), use the restore wrapper directly:

```bash
bash da-restore-wrap.sh --owner admin --backup-dir /home/admin/user_backups
```

Strict validation (non-zero exit on any `FAIL`):

```bash
bash da-restore-wrap.sh --owner admin --backup-dir /home/admin/user_backups --strict-validate
```

Retry failed users only:

```bash
bash da-restore-wrap.sh --retry-failures-only --strict-validate
```

View tracker summary:

```bash
bash da-restore-wrap.sh --show-tracker
```

## Post-Restore Cleanup

Ownership reconciliation and cPanel ini cleanup:

```bash
bash da-restore-ownership.sh --owner admin --dry-run
bash da-restore-ownership.sh --owner admin
```

Optional post-migration maintenance tasks (can be run independently at any time):

```bash
bash post-boxer.sh --clean-ini        # Remove .user.ini/php.ini from public_html trees
bash post-boxer.sh --clean-error      # Neutralise PHP error output in WP/Joomla configs
bash post-boxer.sh --fix-ownership    # Ensure users are in reseller users.list
```

## Validation: WARN vs FAIL

- **FAIL**: Core restore artifacts are missing (e.g. missing DA user data, missing home directory, empty domains list).
- **WARN**: Account exists but suspicious deltas detected (e.g. backup had web/db content but restored artifacts look incomplete).

## Prerequisites

- DirectAdmin target server, run as `root`.
- SSH connectivity from target to source cPanel host (use `--add-key` to set up).
- `rsync`, `ssh`, `bash`, `awk`, `sed`, `grep`, `tee`, `mktemp` on the target.
- Source host must support `/scripts/pkgacct`.
- Run inside `screen` or `tmux` (Boxer enforces this).

## Troubleshooting

1. Check `boxer-USER.err` first for actionable failures.
2. If transfer fails, verify SSH key, source host, and source port.
3. If restore fails, inspect wrapper reports and DA queue log output in the user `.log` file.
4. Re-run only failed users by creating a new users file and launching Boxer again.

## Tips

- Test with `--dry-run` and small user lists before large migration waves.
- A failed user does not stop the remaining queue.
- Source cPanel accounts are never removed — only temporary archives are cleaned up.
- `ip_choice=select` is resolved at runtime via auto-detected primary IPv4.
- Run smaller first batches to confirm timing and throughput before full waves.

## License

See [LICENSE](LICENSE).
