#!/bin/bash
# Post-restore cleanup for DirectAdmin migrations:
# - fixes reseller ownership for users in users.list
# - removes cPanel-era .user.ini/php.ini files from DA public_html trees

set -euo pipefail
IFS=$'\n\t'

OWNER="admin"
USERS_LIST=""
DRY_RUN=0
SKIP_OWNERSHIP=0
SKIP_PHP_INI_CLEANUP=0

log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*"; }
err()  { echo "[ERROR] $*"; exit 1; }

usage() {
    cat <<'EOF'
Usage:
    bash da-done-cleaning.sh [options]

Options:
  --owner <name>            Reseller owner (default: admin)
  --users-list <path>       Path to users.list (default: /usr/local/directadmin/data/users/<owner>/users.list)
  --dry-run                 Show actions without making changes
  --skip-ownership          Skip move_user_to_reseller.sh ownership loop
  --skip-php-ini-cleanup    Skip .user.ini/php.ini cleanup
  -h, --help                Show help

Notes:
- Cleanup targets only /home/<user>/domains/**/public_html trees, including subdomains.
- This avoids touching global/system php.ini files outside website document roots.
EOF
}

require_root() {
    [ "$(id -u)" -eq 0 ] || err "Run as root."
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --owner)
                shift
                [ $# -gt 0 ] || err "Missing value for --owner"
                OWNER="$1"
                ;;
            --users-list)
                shift
                [ $# -gt 0 ] || err "Missing value for --users-list"
                USERS_LIST="$1"
                ;;
            --dry-run)
                DRY_RUN=1
                ;;
            --skip-ownership)
                SKIP_OWNERSHIP=1
                ;;
            --skip-php-ini-cleanup)
                SKIP_PHP_INI_CLEANUP=1
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

    if [ -z "$USERS_LIST" ]; then
        USERS_LIST="/usr/local/directadmin/data/users/${OWNER}/users.list"
    fi
}

load_users() {
    [ -f "$USERS_LIST" ] || err "users.list not found: $USERS_LIST"

    mapfile -t USERS < <(grep -Ev '^[[:space:]]*(#|$)' "$USERS_LIST" | awk '{print $1}')
    [ "${#USERS[@]}" -gt 0 ] || err "No users found in $USERS_LIST"

    log "Loaded ${#USERS[@]} user(s) from $USERS_LIST"
}

ensure_users_in_reseller_list() {
    local da_users_list="/usr/local/directadmin/data/users/${OWNER}/users.list"

    if [ ! -f "$da_users_list" ]; then
        warn "Reseller users.list not found at ${da_users_list} — creating it"
        mkdir -p "$(dirname "$da_users_list")"
        touch "$da_users_list"
    fi

    local added=0
    for user in "${USERS[@]}"; do
        if ! grep -qxF "$user" "$da_users_list"; then
            if [ "$DRY_RUN" -eq 1 ]; then
                log "[DRY-RUN] Would append '${user}' to ${da_users_list}"
            else
                echo "$user" >> "$da_users_list"
                log "Appended '${user}' to ${da_users_list}"
            fi
            added=$((added + 1))
        fi
    done

    if [ "$added" -eq 0 ]; then
        log "All ${#USERS[@]} user(s) already present in ${da_users_list}"
    else
        log "Added ${added} user(s) to ${da_users_list}"
    fi
}

fix_ownership_for_users() {
    local mover="/usr/local/directadmin/scripts/move_user_to_reseller.sh"
    [ -x "$mover" ] || err "Missing executable: $mover"

    local ok=0
    local failed=0
    local skipped=0

    log "Starting ownership reconciliation to reseller '${OWNER}'..."

    for user in "${USERS[@]}"; do
        if [ ! -d "/usr/local/directadmin/data/users/${user}" ]; then
            warn "User '${user}' does not exist in DA data dir; skipping"
            skipped=$((skipped + 1))
            continue
        fi

        if [ "$DRY_RUN" -eq 1 ]; then
            log "[DRY-RUN] ${mover} ${user} ${OWNER} ${OWNER}"
            ok=$((ok + 1))
            continue
        fi

        if "$mover" "$user" "$OWNER" "$OWNER"; then
            ok=$((ok + 1))
        else
            warn "Ownership fix failed for user '${user}'"
            failed=$((failed + 1))
        fi
    done

    log "Ownership summary: ok=${ok}, failed=${failed}, skipped=${skipped}"
    [ "$failed" -eq 0 ] || warn "Some ownership operations failed; review logs above"
}

remove_cp_php_ini_from_public_html() {
    local removed=0
    local found=0

    log "Removing cPanel-era .user.ini/php.ini from public_html trees..."

    for user in "${USERS[@]}"; do
        local domains_dir="/home/${user}/domains"
        if [ ! -d "$domains_dir" ]; then
            warn "No domains dir for '${user}' at ${domains_dir}; skipping"
            continue
        fi

        while IFS= read -r -d '' webroot; do
            while IFS= read -r -d '' file; do
                found=$((found + 1))
                if [ "$DRY_RUN" -eq 1 ]; then
                    log "[DRY-RUN] rm -f ${file}"
                else
                    rm -f "$file"
                    removed=$((removed + 1))
                    log "Removed ${file}"
                fi
            done < <(find "$webroot" -type f \( -name '.user.ini' -o -name 'php.ini' \) -print0)
        done < <(find "$domains_dir" -type d -name public_html -print0)
    done

    if [ "$DRY_RUN" -eq 1 ]; then
        log "PHP ini cleanup summary (dry-run): candidate files=${found}"
    else
        log "PHP ini cleanup summary: removed=${removed}, discovered=${found}"
    fi
}

main() {
    parse_args "$@"
    require_root
    load_users

    ensure_users_in_reseller_list
    [ "$SKIP_OWNERSHIP" -eq 1 ] || fix_ownership_for_users
    [ "$SKIP_PHP_INI_CLEANUP" -eq 1 ] || remove_cp_php_ini_from_public_html

    log "Done."
}

main "$@"
