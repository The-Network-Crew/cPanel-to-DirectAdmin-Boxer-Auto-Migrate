#!/bin/bash
# Post-migration maintenance tasks for DirectAdmin servers after Boxer runs.
# Can be run independently of boxer.sh at any time.

set -euo pipefail
IFS=$'\n\t'

OWNER="admin"
USERS_CSV=""
USERS_FILE=""
DRY_RUN=0
DO_CLEAN_INI=0
DO_CLEAN_ERROR=0
DO_FIX_OWNERSHIP=0
DO_ALL=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OWNERSHIP_SCRIPT="${SCRIPT_DIR}/da-restore-ownership.sh"

declare -a USERS=()

ts() {
	date +'%F %T %Z'
}

log() {
	echo "[$(ts)] [INFO]  $*"
}

warn() {
	echo "[$(ts)] [WARN]  $*"
}

err() {
	echo "[$(ts)] [ERROR] $*" >&2
	exit 1
}

usage() {
	cat <<'EOF'
Usage:
  bash post-boxer.sh [options]

Tasks (at least one required):
  --clean-ini          Remove every .user.ini and php.ini from
                       /home/*/domains/*/public_html (and subdirs).
  --clean-error        Neutralise PHP error output in WordPress wp-config.php
                       and Joomla configuration.php across all sites.
  --fix-ownership      Ensure migrated users are in the reseller's users.list
                       then run move_user_to_reseller.sh for each.
  --all                Run all of the above tasks.

Common options:
  --owner <name>       DA reseller owner (default: admin)
  --users <csv>        Target specific users (comma-separated)
  --users-file <path>  Text file with one username per line
  --dry-run            Show actions without making changes
  -h, --help           Show help
EOF
}

require_root() {
	[ "$(id -u)" -eq 0 ] || err "Run as root on the DirectAdmin target host."
}

validate_username() {
	local name="$1"
	if [[ ! "$name" =~ ^[a-z][a-z0-9_]{0,15}$ ]]; then
		err "Invalid username '${name}': must be lowercase alphanumeric+underscore, 1-16 chars, starting with a letter"
	fi
}

parse_users_csv() {
	local csv="$1"
	local item
	IFS=',' read -r -a __tmp_users <<< "$csv"
	for item in "${__tmp_users[@]}"; do
		item="$(echo "$item" | xargs)"
		[ -n "$item" ] || continue
		validate_username "$item"
		USERS+=("$item")
	done
}

parse_users_file() {
	local file="$1"
	[ -f "$file" ] || err "Users file not found: $file"

	local line
	while IFS= read -r line; do
		line="$(echo "$line" | sed -E 's/[[:space:]]+#.*$//' | xargs || true)"
		[ -n "$line" ] || continue
		validate_username "$line"
		USERS+=("$line")
	done < "$file"
}

dedupe_users() {
	local deduped
	deduped=$(printf '%s\n' "${USERS[@]}" | awk 'NF && !seen[$0]++')
	mapfile -t USERS <<< "$deduped"
}

discover_all_da_users() {
	local data_dir="/usr/local/directadmin/data/users"
	[ -d "$data_dir" ] || err "DA users data dir not found: $data_dir"

	local -a found=()
	local d user
	for d in "${data_dir}"/*/; do
		[ -d "$d" ] || continue
		user="$(basename "$d")"
		# Skip system/reseller accounts
		[ "$user" = "admin" ] && continue
		[ "$user" = "$OWNER" ] && continue
		[ -d "/home/${user}" ] || continue
		found+=("$user")
	done

	if [ "${#found[@]}" -eq 0 ]; then
		err "No DA users found under ${data_dir}"
	fi

	printf '%s\n' "${found[@]}" | sort -u
}

resolve_users() {
	if [ -n "$USERS_CSV" ] && [ -n "$USERS_FILE" ]; then
		err "Use either --users or --users-file, not both"
	fi

	if [ -n "$USERS_CSV" ]; then
		parse_users_csv "$USERS_CSV"
	elif [ -n "$USERS_FILE" ]; then
		parse_users_file "$USERS_FILE"
	else
		log "No user list specified — discovering all DA users..."
		mapfile -t USERS < <(discover_all_da_users)
	fi

	dedupe_users
	[ "${#USERS[@]}" -gt 0 ] || err "No users resolved"
	log "Target users: ${#USERS[@]}"
}

parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
			--owner)
				shift; [ $# -gt 0 ] || err "Missing value for --owner"
				OWNER="$1"
				;;
			--users)
				shift; [ $# -gt 0 ] || err "Missing value for --users"
				USERS_CSV="$1"
				;;
			--users-file)
				shift; [ $# -gt 0 ] || err "Missing value for --users-file"
				USERS_FILE="$1"
				;;
			--clean-ini)
				DO_CLEAN_INI=1
				;;
			--clean-error)
				DO_CLEAN_ERROR=1
				;;
			--fix-ownership)
				DO_FIX_OWNERSHIP=1
				;;
			--all)
				DO_ALL=1
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

	if [ "$DO_ALL" -eq 1 ]; then
		DO_CLEAN_INI=1
		DO_CLEAN_ERROR=1
		DO_FIX_OWNERSHIP=1
	fi

	if [ "$DO_CLEAN_INI" -eq 0 ] && [ "$DO_CLEAN_ERROR" -eq 0 ] && [ "$DO_FIX_OWNERSHIP" -eq 0 ]; then
		err "Specify at least one task: --clean-ini, --clean-error, --fix-ownership, or --all"
	fi
}

# ──────────────────────────────────────────────
#  --clean-ini
# ──────────────────────────────────────────────
task_clean_ini() {
	log "=== Task: clean-ini ==="

	local -a files=()
	local user
	for user in "${USERS[@]}"; do
		local domains_dir="/home/${user}/domains"
		[ -d "$domains_dir" ] || continue
		while IFS= read -r f; do
			files+=("$f")
		done < <(find "$domains_dir" -type f \( -name '.user.ini' -o -name 'php.ini' \) 2>/dev/null || true)
	done

	if [ "${#files[@]}" -eq 0 ]; then
		log "No .user.ini or php.ini files found. Nothing to do."
		return 0
	fi

	log "Found ${#files[@]} ini file(s) to remove:"
	local f
	for f in "${files[@]}"; do
		echo "  $f"
	done

	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would remove ${#files[@]} file(s)."
		return 0
	fi

	for f in "${files[@]}"; do
		rm -f "$f"
	done

	log "Removed ${#files[@]} ini file(s)."
}

# ──────────────────────────────────────────────
#  --clean-error
# ──────────────────────────────────────────────

clean_wp_config() {
	local file="$1"

	# Patterns to remove from wp-config.php:
	#   - error_reporting()          PHP function
	#   - @ini_set('display_errors'  PHP ini_set
	#   - @ini_set('display_startup_errors'
	#   - @ini_set('log_errors'
	#   - @ini_set('error_reporting'
	#   - define('WP_DEBUG'          WP constants
	#   - define('WP_DEBUG_DISPLAY'
	#   - define('WP_DEBUG_LOG'
	local pattern='^\s*(@\s*ini_set|ini_set)\s*\(\s*['"'"'\"](display_errors|display_startup_errors|log_errors|error_reporting)['"'"'\"]'
	pattern="${pattern}|"
	pattern="${pattern}"'^\s*error_reporting\s*\('
	pattern="${pattern}|"
	pattern="${pattern}"'^\s*define\s*\(\s*['"'"'\"](WP_DEBUG|WP_DEBUG_DISPLAY|WP_DEBUG_LOG)['"'"'\"]'

	local matches
	matches=$(grep -cE "$pattern" "$file" 2>/dev/null || echo 0)
	[ "$matches" -gt 0 ] || return 0

	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would remove ${matches} error-reporting line(s) from ${file}"
		grep -nE "$pattern" "$file" 2>/dev/null | sed 's/^/  /'
		return 0
	fi

	local tmp
	tmp=$(mktemp)
	grep -vE "$pattern" "$file" > "$tmp"
	# Preserve original ownership and permissions
	chmod --reference="$file" "$tmp" 2>/dev/null || true
	chown --reference="$file" "$tmp" 2>/dev/null || true
	mv "$tmp" "$file"

	log "Removed ${matches} error-reporting line(s) from ${file}"
}

clean_joomla_config() {
	local file="$1"

	# Match: public $error_reporting = 'anything';
	local pattern='^\s*public\s\+\$error_reporting\s*='

	if ! grep -qE '^\s*public\s+\$error_reporting\s*=' "$file" 2>/dev/null; then
		return 0
	fi

	# Check if already set to 'none'
	if grep -qE '^\s*public\s+\$error_reporting\s*=\s*['"'"'\"]none['"'"'\"]\s*;' "$file" 2>/dev/null; then
		return 0
	fi

	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would set \$error_reporting = 'none' in ${file}"
		grep -nE '^\s*public\s+\$error_reporting\s*=' "$file" | sed 's/^/  /'
		return 0
	fi

	local tmp
	tmp=$(mktemp)
	sed -E 's/^([[:space:]]*public[[:space:]]+\$error_reporting[[:space:]]*=[[:space:]]*).*/\1'"'"'none'"'"';/' "$file" > "$tmp"
	chmod --reference="$file" "$tmp" 2>/dev/null || true
	chown --reference="$file" "$tmp" 2>/dev/null || true
	mv "$tmp" "$file"

	log 'Set $error_reporting = '"'"'none'"'"' in '"${file}"
}

task_clean_error() {
	log "=== Task: clean-error ==="

	local wp_count=0
	local joomla_count=0
	local user

	for user in "${USERS[@]}"; do
		local domains_dir="/home/${user}/domains"
		[ -d "$domains_dir" ] || continue

		# WordPress: wp-config.php
		while IFS= read -r f; do
			clean_wp_config "$f"
			wp_count=$((wp_count + 1))
		done < <(find "$domains_dir" -maxdepth 3 -type f -name 'wp-config.php' 2>/dev/null || true)

		# Joomla: configuration.php (contains JConfig class)
		while IFS= read -r f; do
			# Verify it's actually a Joomla config (contains JConfig or error_reporting property)
			if grep -qE '^\s*(class\s+JConfig|public\s+\$error_reporting)' "$f" 2>/dev/null; then
				clean_joomla_config "$f"
				joomla_count=$((joomla_count + 1))
			fi
		done < <(find "$domains_dir" -maxdepth 3 -type f -name 'configuration.php' 2>/dev/null || true)
	done

	log "Processed ${wp_count} wp-config.php file(s), ${joomla_count} Joomla configuration.php file(s)."
}

# ──────────────────────────────────────────────
#  --fix-ownership
# ──────────────────────────────────────────────
task_fix_ownership() {
	log "=== Task: fix-ownership ==="

	[ -f "$OWNERSHIP_SCRIPT" ] || err "Ownership script not found: $OWNERSHIP_SCRIPT"

	local users_list
	users_list=$(mktemp)
	printf '%s\n' "${USERS[@]}" > "$users_list"

	local -a cmd=(bash "$OWNERSHIP_SCRIPT" --owner "$OWNER" --users-list "$users_list" --skip-php-ini-cleanup)
	if [ "$DRY_RUN" -eq 1 ]; then
		cmd+=(--dry-run)
	fi

	set +e
	"${cmd[@]}"
	local rc=$?
	set -e

	rm -f "$users_list"

	if [ "$rc" -ne 0 ]; then
		warn "Ownership script exited with code ${rc}"
	fi
}

# ──────────────────────────────────────────────
#  main
# ──────────────────────────────────────────────
main() {
	parse_args "$@"
	require_root
	resolve_users

	log "=== post-boxer.sh — $(date +'%F %T %Z') ==="

	if [ "$DO_FIX_OWNERSHIP" -eq 1 ]; then
		task_fix_ownership
	fi

	if [ "$DO_CLEAN_INI" -eq 1 ]; then
		task_clean_ini
	fi

	if [ "$DO_CLEAN_ERROR" -eq 1 ]; then
		task_clean_error
	fi

	log "=== post-boxer.sh complete ==="
}

main "$@"
