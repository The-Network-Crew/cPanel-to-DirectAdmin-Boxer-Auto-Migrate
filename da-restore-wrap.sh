#!/bin/bash
# Wrap DA restore execution, capture logs, then run artifact-based validation.

set -euo pipefail
IFS=$'\n\t'

OWNER="admin"
BACKUP_DIR=""
IP_CHOICE="select"
IP=""
DRY_RUN=0
STRICT_VALIDATE=0
REPORT_DIR=""
LOG_FILE=""
TRACK_FILE=""
BATCH_SIZE=10
USERS_CSV=""
SELECTED_USERS_CSV=""
SHOW_TRACKER=0
RETRY_FAILURES_ONLY=0
RESTORE_SCRIPT="./da-restore-from-pkgacct.sh"
VALIDATE_SCRIPT="./da-restore-validate.sh"

log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*"; }
err()  { echo "[ERROR] $*"; exit 1; }

usage() {
    cat <<'EOF'
Usage:
  bash da-restore-wrap.sh [options]

Options:
  --owner <name>        Restore owner/reseller (default: admin)
  --backup-dir <path>   cpmove source directory (default: /home/<owner>/user_backups)
  --ip-choice <value>   DA restore ip_choice (default: select)
  --ip <addr>           IP used when --ip-choice=select
    --users <csv>         Restore only these users (bypass auto-batch selection)
    --batch-size <n>      Restore next N users per run (default: 10)
    --track-file <path>   Persistent CSV tracker (default: /root/da-restore-reports/restore-batch-tracker.csv)
    --show-tracker        Print tracker summary and exit
    --retry-failures-only Select only users whose latest tracked status is FAIL
  --report-dir <path>   Validation output directory
  --strict-validate     Make validation fail if any FAIL is detected
  --dry-run             Build/print restore payload only
  -h, --help            Show help

Flow:
1) build restore payload
2) enqueue restore task
3) run dataskq and tee output to log
4) validate restored users/artifacts
EOF
}

detect_primary_ipv4() {
    ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i=="src") {print $(i+1); exit}}'
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --owner)
                shift; [ $# -gt 0 ] || err "Missing value for --owner"
                OWNER="$1"
                ;;
            --backup-dir)
                shift; [ $# -gt 0 ] || err "Missing value for --backup-dir"
                BACKUP_DIR="$1"
                ;;
            --ip-choice)
                shift; [ $# -gt 0 ] || err "Missing value for --ip-choice"
                IP_CHOICE="$1"
                ;;
            --ip)
                shift; [ $# -gt 0 ] || err "Missing value for --ip"
                IP="$1"
                ;;
            --users)
                shift; [ $# -gt 0 ] || err "Missing value for --users"
                USERS_CSV="$1"
                ;;
            --batch-size)
                shift; [ $# -gt 0 ] || err "Missing value for --batch-size"
                BATCH_SIZE="$1"
                ;;
            --track-file)
                shift; [ $# -gt 0 ] || err "Missing value for --track-file"
                TRACK_FILE="$1"
                ;;
            --show-tracker)
                SHOW_TRACKER=1
                ;;
            --retry-failures-only)
                RETRY_FAILURES_ONLY=1
                ;;
            --report-dir)
                shift; [ $# -gt 0 ] || err "Missing value for --report-dir"
                REPORT_DIR="$1"
                ;;
            --strict-validate)
                STRICT_VALIDATE=1
                ;;
            --dry-run)
                DRY_RUN=1
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                err "Unknown argument: $1"
                ;;
        esac
        shift
    done

    [ -n "$BACKUP_DIR" ] || BACKUP_DIR="/home/${OWNER}/user_backups"
    [ -d "$BACKUP_DIR" ] || err "Backup directory not found: $BACKUP_DIR"

    if [ "$IP_CHOICE" = "select" ] && [ -z "$IP" ]; then
        IP="$(detect_primary_ipv4 || true)"
        [ -n "$IP" ] || err "Could not auto-detect IPv4 for --ip-choice select. Pass --ip explicitly."
        log "Auto-detected restore IP for select mode: $IP"
    fi

    [ -f "$VALIDATE_SCRIPT" ] || err "Validator script not found: $VALIDATE_SCRIPT"
    [ -f "$RESTORE_SCRIPT" ] || err "Restore payload script not found: $RESTORE_SCRIPT"

    if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]]; then
        err "--batch-size must be a positive integer"
    fi

    if [ "$BATCH_SIZE" -lt 1 ]; then
        err "--batch-size must be >= 1"
    fi

    [ -n "$TRACK_FILE" ] || TRACK_FILE="/root/da-restore-reports/restore-batch-tracker.csv"
    mkdir -p "$(dirname "$TRACK_FILE")"

    if [ -z "$REPORT_DIR" ]; then
        REPORT_DIR="/root/da-restore-reports/$(date +%F-%H%M%S)"
    fi
    mkdir -p "$REPORT_DIR"

    LOG_FILE="${REPORT_DIR}/dataskq-restore.log"
}

ensure_tracker_file() {
    if [ ! -f "$TRACK_FILE" ]; then
        echo "timestamp,batch_id,user,status,notes,report_dir" > "$TRACK_FILE"
    fi
}

print_tracker_summary() {
    [ -f "$TRACK_FILE" ] || {
        log "Tracker file does not exist yet: $TRACK_FILE"
        return
    }

    echo "Tracker: $TRACK_FILE"
    echo "Total entries: $(awk 'END{print NR-1}' "$TRACK_FILE")"
    echo "Status counts:"
    awk -F',' 'NR>1 {counts[$4]++} END {for (k in counts) printf "  %s: %d\n", k, counts[k]}' "$TRACK_FILE" | sort
    echo "Latest 10 entries:"
    tail -n 10 "$TRACK_FILE"
}

extract_all_users() {
    local users=()
    while IFS= read -r file; do
        local base user
        base=$(basename "$file")
        user=$(echo "$base" | sed -E 's/^cpmove-([A-Za-z0-9_]+).*\.tar\.gz$/\1/')
        [ -n "$user" ] && users+=("$user")
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'cpmove-*.tar.gz' | sort)

    printf '%s\n' "${users[@]}" | awk 'NF && !seen[$0]++'
}

is_completed_user() {
    local user="$1"
    [ -f "$TRACK_FILE" ] || return 1
    awk -F',' -v u="$user" 'NR>1 && $3==u && ($4=="PASS" || $4=="WARN") {found=1} END{exit found?0:1}' "$TRACK_FILE"
}

latest_status_for_user() {
    local user="$1"
    [ -f "$TRACK_FILE" ] || return 0
    awk -F',' -v u="$user" 'NR>1 && $3==u {status=$4} END {if (status != "") print status}' "$TRACK_FILE"
}

join_by_comma() {
    local out=""
    local item
    for item in "$@"; do
        if [ -z "$out" ]; then
            out="$item"
        else
            out="${out},${item}"
        fi
    done
    echo "$out"
}

select_users_for_run() {
    if [ -n "$USERS_CSV" ]; then
        SELECTED_USERS_CSV="$USERS_CSV"
        return
    fi

    local limit="$BATCH_SIZE"
    [ "$limit" -eq 0 ] && limit=999999

    local selected=()
    local user
    while IFS= read -r user; do
        [ -n "$user" ] || continue

        if [ "$RETRY_FAILURES_ONLY" -eq 1 ]; then
            if [ "$(latest_status_for_user "$user")" != "FAIL" ]; then
                continue
            fi
        fi

        if is_completed_user "$user"; then
            continue
        fi
        selected+=("$user")
        if [ "${#selected[@]}" -ge "$limit" ]; then
            break
        fi
    done < <(extract_all_users)

    if [ "${#selected[@]}" -eq 0 ]; then
        if [ "$RETRY_FAILURES_ONLY" -eq 1 ]; then
            log "No retry candidates found where latest status is FAIL."
        else
            log "No pending users found in backups (all tracked as PASS/WARN)."
        fi
        exit 0
    fi

    SELECTED_USERS_CSV=$(join_by_comma "${selected[@]}")
    log "Selected users for this run: ${SELECTED_USERS_CSV}"
}

build_restore_payload() {
    # Run restore payload script with overridable environment values.
    OWNER="$OWNER" LOCAL_PATH="$BACKUP_DIR" IP_CHOICE="$IP_CHOICE" IP="$IP" USERS_CSV="$SELECTED_USERS_CSV" bash "$RESTORE_SCRIPT"
}

enqueue_restore() {
    local payload="$1"
    [ -n "$payload" ] || err "Empty restore payload"
    echo "$payload" | grep -q '^action=restore&' || err "Restore payload does not look valid"

    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] Would enqueue payload:"
        echo "$payload"
        return
    fi

    echo "$payload" >> /usr/local/directadmin/data/task.queue
    log "Restore task queued."
}

run_restore_queue() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] Would execute: /usr/local/directadmin/dataskq d"
        return
    fi

    log "Running dataskq restore queue..."
    /usr/local/directadmin/dataskq d 2>&1 | tee "$LOG_FILE"
    log "Restore queue output logged to: $LOG_FILE"
}

run_validation() {
    local strict_arg=()
    [ "$STRICT_VALIDATE" -eq 1 ] && strict_arg=(--strict)

    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] Would validate restores from $BACKUP_DIR"
        return
    fi

    bash "$VALIDATE_SCRIPT" \
        --owner "$OWNER" \
        --backup-dir "$BACKUP_DIR" \
        --users "$SELECTED_USERS_CSV" \
        --log-file "$LOG_FILE" \
        --report-dir "$REPORT_DIR" \
        "${strict_arg[@]}"
}

track_validation_results() {
    [ "$DRY_RUN" -eq 1 ] && return

    local csv="${REPORT_DIR}/restore-validation-summary.csv"
    [ -f "$csv" ] || {
        warn "Validation CSV not found for tracking: $csv"
        return
    }

    local batch_id
    batch_id=$(date +%F-%H%M%S)

    awk -F',' 'NR>1 {user=$1;status=$2;$1="";$2="";sub(/^,,/,""); print user "|" status "|" $0}' "$csv" | while IFS='|' read -r user status notes; do
        [ -n "$user" ] || continue
        printf '%s,%s,%s,%s,"%s",%s\n' "$(date -u +'%F %T')" "$batch_id" "$user" "$status" "$notes" "$REPORT_DIR" >> "$TRACK_FILE"
    done

    log "Tracker updated: $TRACK_FILE"
}

main() {
    parse_args "$@"
    ensure_tracker_file

    if [ "$SHOW_TRACKER" -eq 1 ]; then
        print_tracker_summary
        exit 0
    fi

    select_users_for_run

    local payload
    payload=$(build_restore_payload)

    enqueue_restore "$payload"
    run_restore_queue
    local validate_rc=0
    if run_validation; then
        validate_rc=0
    else
        validate_rc=$?
    fi

    track_validation_results

    log "Done. Report directory: $REPORT_DIR"

    if [ "$validate_rc" -ne 0 ]; then
        if [ "$STRICT_VALIDATE" -eq 1 ]; then
            err "Validation returned non-zero exit (${validate_rc})."
        else
            warn "Validation returned non-zero exit (${validate_rc}) — review reports in $REPORT_DIR"
        fi
    fi
}

main "$@"
