#!/bin/bash
# Validate DirectAdmin restore outcomes using artifact checks, not only log text.

set -euo pipefail
IFS=$'\n\t'

OWNER="admin"
BACKUP_DIR=""
REPORT_DIR=""
LOG_FILE=""
STRICT=0
USERS_CSV=""

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

REPORT_TXT=""
REPORT_CSV=""

log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*"; }
err()  { echo "[ERROR] $*"; exit 1; }

usage() {
    cat <<'EOF'
Usage:
  bash da-restore-validate.sh [options]

Options:
  --owner <name>        Reseller owner (default: admin)
  --backup-dir <path>   Directory containing cpmove-*.tar.gz backups
                        (default: /home/<owner>/user_backups)
  --users <csv>         Validate only comma-separated users (e.g. user1,user2)
  --log-file <path>     Restore log file to scan for critical markers
  --report-dir <path>   Output directory for reports
  --strict              Exit non-zero if any FAIL is detected
  -h, --help            Show help

Output:
- Human summary: restore-validation-summary.txt
- CSV summary: restore-validation-summary.csv
EOF
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
            --users)
                shift; [ $# -gt 0 ] || err "Missing value for --users"
                USERS_CSV="$1"
                ;;
            --log-file)
                shift; [ $# -gt 0 ] || err "Missing value for --log-file"
                LOG_FILE="$1"
                ;;
            --report-dir)
                shift; [ $# -gt 0 ] || err "Missing value for --report-dir"
                REPORT_DIR="$1"
                ;;
            --strict)
                STRICT=1
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

    if [ -z "$REPORT_DIR" ]; then
        REPORT_DIR="/root/da-restore-reports/$(date +%F-%H%M%S)"
    fi
    mkdir -p "$REPORT_DIR"

    REPORT_TXT="${REPORT_DIR}/restore-validation-summary.txt"
    REPORT_CSV="${REPORT_DIR}/restore-validation-summary.csv"
}

backup_for_user() {
    local user="$1"
    local match
    # Try original cpmove archive first
    match=$(find "$BACKUP_DIR" -maxdepth 1 -type f -name "cpmove-${user}*.tar.gz" | sort | tail -n1)
    [ -n "$match" ] && { echo "$match"; return 0; }
    # Try DA-converted archive (cpanel_to_da renames to user.OWNER.USER.tar.zst)
    match=$(find "$BACKUP_DIR" -maxdepth 1 -type f -name "user.*.${user}.tar.zst" | sort | tail -n1)
    [ -n "$match" ] && { echo "$match"; return 0; }
    return 0
}

extract_users_from_backups() {
    local users=()
    # Original cpmove archives
    while IFS= read -r file; do
        local base user
        base=$(basename "$file")
        user=$(echo "$base" | sed -E 's/^cpmove-([A-Za-z0-9_]+).*\.tar\.gz$/\1/')
        [ -n "$user" ] && users+=("$user")
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'cpmove-*.tar.gz' | sort)
    # DA-converted archives (user.OWNER.USER.tar.zst)
    while IFS= read -r file; do
        local base user
        base=$(basename "$file")
        user=$(echo "$base" | sed -E 's/^user\.[^.]+\.([A-Za-z0-9_]+)\.tar\.zst$/\1/')
        [ -n "$user" ] && users+=("$user")
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'user.*.tar.zst' | sort)

    if [ "${#users[@]}" -eq 0 ]; then
        err "No cpmove-*.tar.gz or user.*.tar.zst backups found in $BACKUP_DIR"
    fi

    printf '%s\n' "${users[@]}" | awk '!seen[$0]++'
}

users_to_validate() {
    if [ -n "$USERS_CSV" ]; then
        echo "$USERS_CSV" | tr ',' '\n' | sed '/^[[:space:]]*$/d'
    else
        extract_users_from_backups
    fi
}

backup_contains_path() {
    local archive="$1"
    local regex="$2"
    case "$archive" in
        *.tar.zst) zstd -dcf "$archive" 2>/dev/null | tar -t 2>/dev/null | grep -Eq "$regex" ;;
        *)         tar -tzf "$archive" 2>/dev/null | grep -Eq "$regex" ;;
    esac
}

default_domain_from_backup() {
    local archive="$1"
    local user="$2"
    case "$archive" in
        *.tar.zst) zstd -dcf "$archive" 2>/dev/null | tar -xO "cp/${user}" 2>/dev/null | awk -F= '$1=="DNS"{print $2; exit}' | tr -d '[:space:]' ;;
        *)         tar -xOzf "$archive" "cp/${user}" 2>/dev/null | awk -F= '$1=="DNS"{print $2; exit}' | tr -d '[:space:]' ;;
    esac
}

user_domains_count() {
    local user="$1"
    local list="/usr/local/directadmin/data/users/${user}/domains.list"
    [ -f "$list" ] || { echo 0; return; }
    grep -Ev '^[[:space:]]*$' "$list" | wc -l | awk '{print $1}'
}

count_public_html_for_user() {
    local user="$1"
    local domains_dir="/home/${user}/domains"
    [ -d "$domains_dir" ] || { echo 0; return; }
    find "$domains_dir" -type d -name public_html | wc -l | awk '{print $1}'
}

count_mysql_dirs_for_user() {
    local user="$1"
    if [ ! -d /var/lib/mysql ]; then
        echo 0
        return
    fi
    find /var/lib/mysql -maxdepth 1 -mindepth 1 -type d -name "${user}_*" | wc -l | awk '{print $1}'
}

append_txt() {
    echo "$1" >> "$REPORT_TXT"
}

append_csv() {
    echo "$1" >> "$REPORT_CSV"
}

evaluate_user() {
    local user="$1"
    local archive="$2"

    local status="PASS"
    local notes=()

    [ -d "/usr/local/directadmin/data/users/${user}" ] || {
        status="FAIL"
        notes+=("missing_da_user_data")
    }

    [ -d "/home/${user}" ] || {
        status="FAIL"
        notes+=("missing_home_dir")
    }

    local domain_count
    domain_count=$(user_domains_count "$user")
    if [ "$domain_count" -eq 0 ]; then
        status="FAIL"
        notes+=("no_domains_list_entries")
    fi

    local public_html_count
    public_html_count=$(count_public_html_for_user "$user")
    if backup_contains_path "$archive" '^homedir/public_html/'; then
        if [ "$public_html_count" -eq 0 ]; then
            [ "$status" = "PASS" ] && status="WARN"
            notes+=("backup_has_public_html_but_none_restored")
        fi
    fi

    local expected_domain
    expected_domain=$(default_domain_from_backup "$archive" "$user")
    if [ -n "$expected_domain" ] && [ -f "/usr/local/directadmin/data/users/${user}/domains.list" ]; then
        if ! grep -qxF "$expected_domain" "/usr/local/directadmin/data/users/${user}/domains.list"; then
            [ "$status" = "PASS" ] && status="WARN"
            notes+=("default_domain_mismatch:${expected_domain}")
        fi
    fi

    if backup_contains_path "$archive" '^mysql\.sql|^mysql/'; then
        local mysql_count
        mysql_count=$(count_mysql_dirs_for_user "$user")
        if [ "$mysql_count" -eq 0 ]; then
            [ "$status" = "PASS" ] && status="WARN"
            notes+=("backup_has_mysql_but_no_user_db_dirs")
        fi
    fi

    local notes_joined
    if [ "${#notes[@]}" -eq 0 ]; then
        notes_joined="ok"
    else
        notes_joined=$(IFS=';'; echo "${notes[*]}")
    fi

    case "$status" in
        PASS) PASS_COUNT=$((PASS_COUNT + 1)) ;;
        WARN) WARN_COUNT=$((WARN_COUNT + 1)) ;;
        FAIL) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    esac

    append_txt "${user}: ${status} (${notes_joined})"
    append_csv "${user},${status},\"${notes_joined}\""
}

scan_restore_log() {
    [ -n "$LOG_FILE" ] || return
    [ -f "$LOG_FILE" ] || { warn "Log file not found: $LOG_FILE"; return; }

    append_txt ""
    append_txt "Log scan: ${LOG_FILE}"

    local suspect
    suspect=$(grep -Ein 'fatal|failed|error|cannot|can.t|not found|permission denied|sqlstate|rollback|exit status|exit code|timed.?out|timeout|refused|abort|segfault|killed|out of memory|oom' "$LOG_FILE" || true)

    if [ -z "$suspect" ]; then
        append_txt "- No critical markers matched in log scan"
        return
    fi

    append_txt "- Critical markers detected (review context):"
    echo "$suspect" | head -n 50 | sed 's/^/  /' >> "$REPORT_TXT"
}

main() {
    parse_args "$@"

    : > "$REPORT_TXT"
    : > "$REPORT_CSV"
    append_csv "user,status,notes"

    append_txt "Restore Validation Report"
    append_txt "Timestamp: $(date +'%F %T %Z')"
    append_txt "Owner: ${OWNER}"
    append_txt "Backup dir: ${BACKUP_DIR}"

    local users=()
    mapfile -t users < <(users_to_validate)

    append_txt "Users to validate: ${#users[@]}"
    append_txt ""

    local user archive
    for user in "${users[@]}"; do
        archive=$(backup_for_user "$user")
        if [ -z "$archive" ]; then
            FAIL_COUNT=$((FAIL_COUNT + 1))
            append_txt "${user}: FAIL (missing_backup_archive)"
            append_csv "${user},FAIL,\"missing_backup_archive\""
            continue
        fi

        evaluate_user "$user" "$archive"
    done

    scan_restore_log

    append_txt ""
    append_txt "Summary: PASS=${PASS_COUNT}, WARN=${WARN_COUNT}, FAIL=${FAIL_COUNT}"

    log "Validation complete: PASS=${PASS_COUNT}, WARN=${WARN_COUNT}, FAIL=${FAIL_COUNT}"
    log "Report: ${REPORT_TXT}"
    log "CSV:    ${REPORT_CSV}"

    if [ "$STRICT" -eq 1 ] && [ "$FAIL_COUNT" -gt 0 ]; then
        err "Strict mode enabled and FAIL count > 0"
    fi
}

main "$@"
