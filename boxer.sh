#!/bin/bash
# Boxer: target-side cPanel -> DirectAdmin per-user migration orchestrator.

set -euo pipefail
IFS=$'\n\t'

OWNER="admin"
SOURCE_OWNER_MATCH=""
TARGET_BACKUP_DIR=""
SOURCE_USER="root"
SOURCE_HOST=""
SOURCE_PORT="22"
SOURCE_KEY=""
SOURCE_BACKUP_DIR="/root/boxer_backups"
USERS_CSV=""
USERS_FILE=""
IP_CHOICE="select"
IP=""
STRICT_VALIDATE=0
SKIP_FINALIZE=0
CLEANUP_SOURCE=1
CLEANUP_TARGET=1
DRY_RUN=0
ADD_KEY=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESTORE_WRAP_SCRIPT="${SCRIPT_DIR}/da-restore-wrap.sh"
OWNERSHIP_SCRIPT="${SCRIPT_DIR}/da-restore-ownership.sh"

RUN_ID="$(date +%F-%H%M%S)"
LOG_DIR="/root/boxer-logs"
RUN_REPORT_BASE="/root/boxer-runs/${RUN_ID}"
STATE_FILE="/root/boxer-runs/boxer-state.csv"

CURRENT_USER=""
CURRENT_LOG=""
CURRENT_ERR=""

declare -a USERS=()
declare -a FAILED_USERS=()

validate_username() {
	local name="$1"
	if [[ ! "$name" =~ ^[a-z][a-z0-9_]{0,15}$ ]]; then
		err "Invalid cPanel username '${name}': must be lowercase alphanumeric+underscore, 1-16 chars, starting with a letter"
	fi
}

validate_owner() {
	local name="$1"
	if [[ ! "$name" =~ ^[a-zA-Z][a-zA-Z0-9_]{0,15}$ ]]; then
		err "Invalid owner '${name}': must be alphanumeric+underscore, 1-16 chars, starting with a letter"
	fi
}

validate_path_safe() {
	local p="$1"
	if [[ "$p" =~ [\"\$\`\\] ]] || [[ "$p" == *"'"* ]]; then
		err "Path contains unsafe shell characters: $p"
	fi
}

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

fmt_bytes() {
	local bytes="$1"
	if [ "$bytes" -ge 1073741824 ] 2>/dev/null; then
		awk "BEGIN {printf \"%.2f GiB\", ${bytes}/1073741824}"
	elif [ "$bytes" -ge 1048576 ] 2>/dev/null; then
		awk "BEGIN {printf \"%.2f MiB\", ${bytes}/1048576}"
	elif [ "$bytes" -ge 1024 ] 2>/dev/null; then
		awk "BEGIN {printf \"%.1f KiB\", ${bytes}/1024}"
	else
		echo "${bytes} B"
	fi
}

fmt_elapsed() {
	local secs="$1"
	if [ "$secs" -ge 3600 ] 2>/dev/null; then
		printf '%dh %dm %ds' $((secs/3600)) $(((secs%3600)/60)) $((secs%60))
	elif [ "$secs" -ge 60 ] 2>/dev/null; then
		printf '%dm %ds' $((secs/60)) $((secs%60))
	else
		printf '%ds' "$secs"
	fi
}

user_log() {
	local message="$*"
	echo "[$(ts)] [INFO]  ${message}" | tee -a "$CURRENT_LOG"
}

user_warn() {
	local message="$*"
	echo "[$(ts)] [WARN]  ${message}" | tee -a "$CURRENT_LOG"
	echo "[$(ts)] [WARN]  ${message}" >> "$CURRENT_ERR"
}

user_error() {
	local step="$1"
	local message="$2"
	local banner="######## BOXER-ERROR [${CURRENT_USER}] [${step}] ${message} ########"
	echo "$banner" | tee -a "$CURRENT_LOG" >&2
	echo "[$(ts)] [ERROR] ${banner}" >> "$CURRENT_ERR"
}

append_state() {
	local user="$1"
	local status="$2"
	local step="$3"
	local notes="$4"
	printf '%s,%s,%s,%s,"%s"\n' "$(date +'%F %T %Z')" "$user" "$status" "$step" "$notes" >> "$STATE_FILE"
}

usage() {
	cat <<'EOF'
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
EOF
}

require_root() {
	[ "$(id -u)" -eq 0 ] || err "Run as root on the DirectAdmin target host."
}

require_multiplexer() {
	if [ -n "${STY:-}" ]; then
		return 0  # screen
	fi
	if [ -n "${TMUX:-}" ]; then
		return 0  # tmux
	fi
	err "Boxer must be run inside screen or tmux — a disconnected SSH session will kill the migration mid-run."
}

setup_ssh_key() {
	local key_path="${SOURCE_KEY:-/root/.ssh/id_ed25519}"
	local pub_path="${key_path}.pub"

	log "=== SSH key setup for ${SOURCE_USER}@${SOURCE_HOST}:${SOURCE_PORT} ==="

	if [ -f "$key_path" ] && [ -f "$pub_path" ]; then
		log "SSH key already exists: ${key_path}"
	else
		log "Generating ed25519 SSH key at ${key_path} ..."
		mkdir -p "$(dirname "$key_path")"
		chmod 700 "$(dirname "$key_path")"
		ssh-keygen -t ed25519 -N "" -f "$key_path" -C "boxer@$(hostname -f 2>/dev/null || hostname)" \
			|| err "ssh-keygen failed"
		log "Key generated: ${key_path}"
	fi

	log "Copying public key to ${SOURCE_USER}@${SOURCE_HOST}:${SOURCE_PORT} ..."
	log "You will be prompted for the remote password."
	ssh-copy-id -i "$pub_path" -p "$SOURCE_PORT" "${SOURCE_USER}@${SOURCE_HOST}" \
		|| err "ssh-copy-id failed — check credentials and that the remote host accepts password auth"

	log "Verifying key-based SSH connectivity ..."
	local test_out
	test_out=$(ssh -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new \
		-p "$SOURCE_PORT" -i "$key_path" "${SOURCE_USER}@${SOURCE_HOST}" "echo boxer-ok" 2>&1) \
		|| err "SSH connectivity test failed: ${test_out}"

	if [ "$test_out" = "boxer-ok" ]; then
		log "SSH key auth verified — connection to ${SOURCE_USER}@${SOURCE_HOST} is working."
	else
		err "Unexpected SSH test output: ${test_out}"
	fi

	echo ""
	log "All set! Re-run boxer without --add-key to start the migration."
	if [ -n "$SOURCE_KEY" ]; then
		log "  (your --source-key ${SOURCE_KEY} will be used automatically)"
	else
		log "  (default key ${key_path} will be used automatically)"
	fi
	exit 0
}

ensure_dependencies() {
	local -a installable=(rsync)
	local -a missing_fatal=()
	local bin

	for bin in ssh rsync bash awk sed grep tee mktemp; do
		if ! command -v "$bin" >/dev/null 2>&1; then
			local found=0
			for pkg in "${installable[@]}"; do
				if [ "$bin" = "$pkg" ]; then found=1; break; fi
			done
			if [ "$found" -eq 1 ]; then
				log "Missing '$bin' — installing via yum..."
				yum install -y "$bin" >/dev/null 2>&1 \
					|| err "Failed to yum install $bin"
				command -v "$bin" >/dev/null 2>&1 \
					|| err "$bin still not available after install"
				log "Installed $bin successfully."
			else
				warn "Missing required command: $bin"
				missing_fatal+=("$bin")
			fi
		fi
	done
	[ "${#missing_fatal[@]}" -eq 0 ] || err "Missing dependencies: ${missing_fatal[*]}"
}

discover_source_users_by_owner() {
	local owner_match="$1"
	local -a opts
	mapfile -t opts < <(ssh_opts)

	local remote_cmd
	remote_cmd=$(cat <<EOF
set -euo pipefail
shopt -s nullglob
for f in /var/cpanel/users/*; do
  [ -f "\$f" ] || continue
  u=\$(basename "\$f")
  o=\$(awk -F= '/^OWNER=/{print \$2; exit}' "\$f")
  if [ "\$o" = '${owner_match}' ]; then echo "\$u"; fi
done | sort -u
EOF
)

	ssh "${opts[@]}" "${SOURCE_USER}@${SOURCE_HOST}" "$remote_cmd"
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

parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
			--source-host)
				shift; [ $# -gt 0 ] || err "Missing value for --source-host"
				SOURCE_HOST="$1"
				;;
			--users)
				shift; [ $# -gt 0 ] || err "Missing value for --users"
				USERS_CSV="$1"
				;;
			--users-file)
				shift; [ $# -gt 0 ] || err "Missing value for --users-file"
				USERS_FILE="$1"
				;;
			--owner)
				shift; [ $# -gt 0 ] || err "Missing value for --owner"
				OWNER="$1"
				;;
			--source-owner)
				shift; [ $# -gt 0 ] || err "Missing value for --source-owner"
				SOURCE_OWNER_MATCH="$1"
				;;
			--target-backup-dir)
				shift; [ $# -gt 0 ] || err "Missing value for --target-backup-dir"
				TARGET_BACKUP_DIR="$1"
				;;
			--source-user)
				shift; [ $# -gt 0 ] || err "Missing value for --source-user"
				SOURCE_USER="$1"
				;;
			--source-port)
				shift; [ $# -gt 0 ] || err "Missing value for --source-port"
				SOURCE_PORT="$1"
				;;
			--source-key)
				shift; [ $# -gt 0 ] || err "Missing value for --source-key"
				SOURCE_KEY="$1"
				;;
			--source-backup-dir)
				shift; [ $# -gt 0 ] || err "Missing value for --source-backup-dir"
				SOURCE_BACKUP_DIR="$1"
				;;
			--ip-choice)
				shift; [ $# -gt 0 ] || err "Missing value for --ip-choice"
				IP_CHOICE="$1"
				;;
			--ip)
				shift; [ $# -gt 0 ] || err "Missing value for --ip"
				IP="$1"
				;;
			--log-dir)
				shift; [ $# -gt 0 ] || err "Missing value for --log-dir"
				LOG_DIR="$1"
				;;
			--run-report-base)
				shift; [ $# -gt 0 ] || err "Missing value for --run-report-base"
				RUN_REPORT_BASE="$1"
				;;
			--state-file)
				shift; [ $# -gt 0 ] || err "Missing value for --state-file"
				STATE_FILE="$1"
				;;
			--strict-validate)
				STRICT_VALIDATE=1
				;;
			--no-strict-validate)
				STRICT_VALIDATE=0
				;;
			--skip-finalize)
				SKIP_FINALIZE=1
				;;
			--no-cleanup-source)
				CLEANUP_SOURCE=0
				;;
			--no-cleanup-target)
				CLEANUP_TARGET=0
				;;
			--add-key)
				ADD_KEY=1
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

	[ -n "$SOURCE_HOST" ] || err "--source-host is required"

	validate_owner "$OWNER"
	if [ -n "$SOURCE_OWNER_MATCH" ]; then
		validate_owner "$SOURCE_OWNER_MATCH"
	fi
	validate_path_safe "$SOURCE_BACKUP_DIR"
	validate_path_safe "$TARGET_BACKUP_DIR"

	if [ -n "$USERS_CSV" ] && [ -n "$USERS_FILE" ]; then
		err "Use either --users or --users-file, not both"
	fi

	if [ -z "$TARGET_BACKUP_DIR" ]; then
		TARGET_BACKUP_DIR="/home/${OWNER}/user_backups"
	fi

	if [ -z "$SOURCE_OWNER_MATCH" ]; then
		SOURCE_OWNER_MATCH="$OWNER"
	fi

	[ -f "$RESTORE_WRAP_SCRIPT" ] || err "Missing restore wrapper: $RESTORE_WRAP_SCRIPT"
	[ -f "$OWNERSHIP_SCRIPT" ] || err "Missing ownership script: $OWNERSHIP_SCRIPT"

	if [ -n "$SOURCE_KEY" ]; then
		[ -f "$SOURCE_KEY" ] || err "SSH key not found: $SOURCE_KEY"
	fi

	if [ -n "$USERS_CSV" ] || [ -n "$USERS_FILE" ]; then
		if [ -n "$USERS_CSV" ]; then
			parse_users_csv "$USERS_CSV"
		else
			parse_users_file "$USERS_FILE"
		fi

		dedupe_users
		[ "${#USERS[@]}" -gt 0 ] || err "No valid users parsed"
	fi
}

resolve_users_if_needed() {
	if [ "${#USERS[@]}" -gt 0 ]; then
		return
	fi

	log "No explicit user list provided; discovering source users where OWNER=${SOURCE_OWNER_MATCH}..."
	local discovered
	set +e
	discovered=$(discover_source_users_by_owner "$SOURCE_OWNER_MATCH" 2>&1)
	local rc=$?
	set -e

	if [ "$rc" -ne 0 ]; then
		err "Failed to discover source users by owner '${SOURCE_OWNER_MATCH}': ${discovered}"
	fi

	mapfile -t USERS < <(printf '%s\n' "$discovered" | sed '/^[[:space:]]*$/d')
	local u
	for u in "${USERS[@]}"; do
		validate_username "$u"
	done
	dedupe_users
	[ "${#USERS[@]}" -gt 0 ] || err "No source users found for owner '${SOURCE_OWNER_MATCH}'"
	log "Discovered ${#USERS[@]} source user(s) for owner '${SOURCE_OWNER_MATCH}'."
}

ensure_paths() {
	mkdir -p "$TARGET_BACKUP_DIR"
	mkdir -p "$LOG_DIR"
	mkdir -p "$RUN_REPORT_BASE"
	mkdir -p "$(dirname "$STATE_FILE")"

	if [ ! -f "$STATE_FILE" ]; then
		echo "timestamp,user,status,step,notes" > "$STATE_FILE"
	fi
}

ssh_opts() {
	local opts=(-o BatchMode=yes -o ConnectTimeout=30 -o StrictHostKeyChecking=accept-new -p "$SOURCE_PORT")
	if [ -n "$SOURCE_KEY" ]; then
		opts+=(-i "$SOURCE_KEY")
	fi
	printf '%s\n' "${opts[@]}"
}

run_and_capture() {
	local description="$1"
	shift

	local tmp
	tmp=$(mktemp)
	user_log "Running: ${description}"

	set +e
	"$@" >"$tmp" 2>&1
	local rc=$?
	set -e

	if [ -s "$tmp" ]; then
		cat "$tmp" | tee -a "$CURRENT_LOG"
		grep -Ein '(^|[^a-z])(error|failed|fatal|cannot|not found|permission denied|sqlstate|rollback|exit status|exit code|timed.?out|timeout|refused|abort|segfault|killed|out of memory|oom)($|[^a-z])|has not been restored' "$tmp" >> "$CURRENT_ERR" || true
	fi
	rm -f "$tmp"

	return "$rc"
}

remote_pkgacct() {
	local user="$1"
	local -a opts
	mapfile -t opts < <(ssh_opts)

	local remote_cmd
	remote_cmd=$(cat <<EOF
set -euo pipefail
mkdir -p '${SOURCE_BACKUP_DIR}'
/scripts/pkgacct '${user}' '${SOURCE_BACKUP_DIR}' >&2
ls -1t '${SOURCE_BACKUP_DIR}'/cpmove-'${user}'*.tar.gz | head -n1
EOF
)

	ssh "${opts[@]}" "${SOURCE_USER}@${SOURCE_HOST}" "$remote_cmd"
}

restore_wrap_for_user() {
	local user="$1"
	local report_dir="$2"

	(
		cd "$SCRIPT_DIR"
		local -a cmd=(bash "./da-restore-wrap.sh" --owner "$OWNER" --backup-dir "$TARGET_BACKUP_DIR" --users "$user" --report-dir "$report_dir")

		if [ "$STRICT_VALIDATE" -eq 1 ]; then
			cmd+=(--strict-validate)
		fi
		if [ -n "$IP_CHOICE" ]; then
			cmd+=(--ip-choice "$IP_CHOICE")
		fi
		if [ -n "$IP" ]; then
			cmd+=(--ip "$IP")
		fi

		"${cmd[@]}"
	)
}

transfer_archive_to_target() {
	local remote_archive="$1"
	local -a opts
	mapfile -t opts < <(ssh_opts)

	local ssh_cmd="ssh"
	local opt
	for opt in "${opts[@]}"; do
		ssh_cmd+=" $(printf '%q' "$opt")"
	done

	rsync -av --partial --append-verify -e "$ssh_cmd" \
		"${SOURCE_USER}@${SOURCE_HOST}:${remote_archive}" \
		"${TARGET_BACKUP_DIR}/"
}

cleanup_source_archive() {
	local remote_archive="$1"
	local -a opts
	mapfile -t opts < <(ssh_opts)
	local escaped_path
	escaped_path=$(printf '%q' "$remote_archive")
	ssh "${opts[@]}" "${SOURCE_USER}@${SOURCE_HOST}" "rm -f ${escaped_path}"
}

run_restore_for_user() {
	local user="$1"
	local report_dir="$2"

	run_and_capture "restore+validate (${user})" restore_wrap_for_user "$user" "$report_dir"
}

run_finalize_for_user() {
	local user="$1"
	local users_list
	users_list=$(mktemp)
	echo "$user" > "$users_list"

	set +e
	run_and_capture "finalize ownership+ini cleanup (${user})" bash "$OWNERSHIP_SCRIPT" --owner "$OWNER" --users-list "$users_list"
	local rc=$?
	set -e

	rm -f "$users_list"
	return "$rc"
}

cleanup_target_archive() {
	local local_archive="$1"
	rm -f "$local_archive"
}

cleanup_target_working_artifacts() {
	local user="$1"
	local dirs=(
		"${TARGET_BACKUP_DIR}/${user}"
		"${TARGET_BACKUP_DIR}/${user}_cpanel_to_convert"
	)

	local d
	for d in "${dirs[@]}"; do
		[ -e "$d" ] && rm -rf "$d"
	done

	find "$TARGET_BACKUP_DIR" -maxdepth 1 -type f \
		\( -name "user.*.${user}.tar*" -o -name "reseller.*.${user}.tar*" \) -exec rm -f {} +
}

cleanup_after_user() {
	local user="$1"
	local local_archive="$2"
	local remote_archive="$3"

	if [ "$CLEANUP_TARGET" -eq 1 ]; then
		if [ -n "$local_archive" ] && [ -f "$local_archive" ]; then
			run_and_capture "cleanup target archive (${user})" cleanup_target_archive "$local_archive" || true
		fi
		run_and_capture "cleanup target working artifacts (${user})" cleanup_target_working_artifacts "$user" || true
	fi

	if [ "$CLEANUP_SOURCE" -eq 1 ] && [ -n "$remote_archive" ]; then
		run_and_capture "cleanup source archive (${user})" cleanup_source_archive "$remote_archive" || true
	fi
}

remote_archive_size() {
	local archive="$1"
	local -a opts
	mapfile -t opts < <(ssh_opts)
	ssh "${opts[@]}" "${SOURCE_USER}@${SOURCE_HOST}" "stat -c '%s' '${archive}' 2>/dev/null || stat -f '%z' '${archive}' 2>/dev/null || echo 0"
}

process_user() {
	local user="$1"
	CURRENT_USER="$user"
	CURRENT_LOG="${LOG_DIR}/boxer-${user}.log"
	CURRENT_ERR="${LOG_DIR}/boxer-${user}.err"

	: > "$CURRENT_LOG"
	: > "$CURRENT_ERR"

	local user_start_ts=$SECONDS
	user_log "=== Boxer start for user: ${user} ==="
	append_state "$user" "START" "init" "processing started"

	local remote_archive=""
	local local_archive=""
	local report_dir="${RUN_REPORT_BASE}/${user}"
	mkdir -p "$report_dir"

	if [ "$DRY_RUN" -eq 1 ]; then
		user_log "[DRY-RUN] Would execute remote pkgacct on ${SOURCE_HOST}"
		user_log "[DRY-RUN] Would rsync archive to ${TARGET_BACKUP_DIR}"
		user_log "[DRY-RUN] Would run restore wrapper for ${user}"
		[ "$SKIP_FINALIZE" -eq 1 ] || user_log "[DRY-RUN] Would run finalize ownership cleanup"
		append_state "$user" "DRY-RUN" "all" "no changes performed"
		user_log "=== Boxer complete for ${user} (dry-run) ==="
		return 0
	fi

	# ---- pkgacct ----
	local step_start=$SECONDS
	set +e
	remote_archive=$(remote_pkgacct "$user" 2>&1)
	local pkgacct_rc=$?
	set -e
	local step_elapsed=$(( SECONDS - step_start ))

	echo "$remote_archive" >> "$CURRENT_LOG"
	remote_archive=$(echo "$remote_archive" | tail -n1)

	if [ "$pkgacct_rc" -ne 0 ] || [ -z "$remote_archive" ]; then
		user_error "pkgacct" "pkgacct failed or archive not detected"
		FAILED_USERS+=("${user}:pkgacct")
		append_state "$user" "FAIL" "pkgacct" "pkgacct failed or no archive"
		return 1
	fi

	user_log "Source archive: ${remote_archive}"

	local remote_size
	remote_size=$(remote_archive_size "$remote_archive" 2>/dev/null || echo 0)
	user_log "[STATS] pkgacct complete — archive size: $(fmt_bytes "$remote_size") (${remote_size} bytes) — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- transfer ----
	step_start=$SECONDS
	if ! run_and_capture "rsync archive (${user})" transfer_archive_to_target "$remote_archive"; then
		user_error "transfer" "rsync transfer failed"
		FAILED_USERS+=("${user}:transfer")
		cleanup_after_user "$user" "" ""
		append_state "$user" "FAIL" "transfer" "rsync failed"
		return 1
	fi
	step_elapsed=$(( SECONDS - step_start ))

	local_archive="${TARGET_BACKUP_DIR}/$(basename "$remote_archive")"
	if [ ! -f "$local_archive" ]; then
		user_error "transfer" "archive missing on target after rsync: ${local_archive}"
		FAILED_USERS+=("${user}:transfer")
		cleanup_after_user "$user" "$local_archive" "$remote_archive"
		append_state "$user" "FAIL" "transfer" "archive missing on target"
		return 1
	fi

	local local_size
	local_size=$(stat -c '%s' "$local_archive" 2>/dev/null || stat -f '%z' "$local_archive" 2>/dev/null || echo 0)
	user_log "[STATS] transfer complete — local archive: $(fmt_bytes "$local_size") (${local_size} bytes) — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- restore ----
	step_start=$SECONDS
	if ! run_restore_for_user "$user" "$report_dir"; then
		user_error "restore" "restore/validate failed"
		FAILED_USERS+=("${user}:restore")
		cleanup_after_user "$user" "$local_archive" "$remote_archive"
		append_state "$user" "FAIL" "restore" "restore wrapper failed"
		return 1
	fi
	step_elapsed=$(( SECONDS - step_start ))

	# ---- output-based restore result detection ----
	if grep -qi 'has not been restored' "$CURRENT_LOG"; then
		user_error "restore" "restore output indicates account was not restored"
		FAILED_USERS+=("${user}:restore-not-restored")
		cleanup_after_user "$user" "$local_archive" "$remote_archive"
		append_state "$user" "FAIL" "restore" "output: has not been restored"
		return 1
	fi
	if grep -qi 'has been restored from' "$CURRENT_LOG"; then
		user_log "Restore confirmed: account '${user}' has been restored from backup"
	else
		user_warn "Restore output did not contain expected confirmation 'has been restored from' — verify manually"
	fi

	user_log "[STATS] restore+validate complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- cleanup ----
	step_start=$SECONDS
	cleanup_after_user "$user" "$local_archive" "$remote_archive"
	step_elapsed=$(( SECONDS - step_start ))
	user_log "[STATS] cleanup complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	local user_elapsed=$(( SECONDS - user_start_ts ))
	append_state "$user" "PASS" "complete" "migration completed in $(fmt_elapsed "$user_elapsed")"
	user_log "=== Boxer complete for ${user} — total elapsed: $(fmt_elapsed "$user_elapsed") ==="
	return 0
}

print_summary() {
	local total="${#USERS[@]}"
	local pass_count fail_count

	pass_count=$(awk -F',' 'NR>1 && $3=="PASS" {c++} END {print c+0}' "$STATE_FILE")
	fail_count=$(awk -F',' 'NR>1 && $3=="FAIL" {c++} END {print c+0}' "$STATE_FILE")

	log "Boxer run complete. Users in this invocation: ${total}"
	log "State file: ${STATE_FILE}"
	log "Per-user logs: ${LOG_DIR}/boxer-<user>.log"
	log "Per-user problems: ${LOG_DIR}/boxer-<user>.err"
	log "Current PASS rows in state file: ${pass_count}"
	log "Current FAIL rows in state file: ${fail_count}"

	if [ "${#FAILED_USERS[@]}" -gt 0 ]; then
		echo "######## BOXER FAILED USERS ########" >&2
		local item
		for item in "${FAILED_USERS[@]}"; do
			echo "- ${item}" >&2
		done
		echo "####################################" >&2
	fi
}

require_owner_exists() {
	local owner_home
	if [ "$OWNER" = "root" ]; then
		owner_home="/root"
	else
		owner_home="/home/${OWNER}"
	fi
	if [ ! -d "$owner_home" ]; then
		err "DA reseller home '${owner_home}' does not exist. Create the '${OWNER}' reseller in DirectAdmin before running boxer."
	fi
}

main() {
	parse_args "$@"
	require_root
	require_multiplexer
	ensure_dependencies
	require_owner_exists

	if [ "$ADD_KEY" -eq 1 ]; then
		setup_ssh_key
	fi
	ensure_paths
	resolve_users_if_needed

	log "Starting Boxer run ${RUN_ID}"
	log "Source: ${SOURCE_USER}@${SOURCE_HOST}:${SOURCE_BACKUP_DIR}"
	log "Target backup dir: ${TARGET_BACKUP_DIR}"
	log "Users queued: ${#USERS[@]}"

	local failed=0
	local user
	local -a passed_users=()
	for user in "${USERS[@]}"; do
		if process_user "$user"; then
			passed_users+=("$user")
		else
			failed=$((failed + 1))
		fi
	done

	# ---- finalize ownership for all passed users at once ----
	if [ "$SKIP_FINALIZE" -eq 0 ] && [ "${#passed_users[@]}" -gt 0 ] && [ "$DRY_RUN" -eq 0 ]; then
		log "Running ownership finalization for ${#passed_users[@]} passed user(s)..."
		local finalize_start=$SECONDS
		local users_list
		users_list=$(mktemp)
		printf '%s\n' "${passed_users[@]}" > "$users_list"

		set +e
		bash "$OWNERSHIP_SCRIPT" --owner "$OWNER" --users-list "$users_list" 2>&1 | tee -a "${LOG_DIR}/boxer-finalize.log"
		local finalize_rc=${PIPESTATUS[0]}
		set -e
		rm -f "$users_list"

		local finalize_elapsed=$(( SECONDS - finalize_start ))
		if [ "$finalize_rc" -ne 0 ]; then
			warn "Ownership finalization exited with code ${finalize_rc} — review ${LOG_DIR}/boxer-finalize.log"
		else
			log "[STATS] ownership finalization complete — elapsed: $(fmt_elapsed "$finalize_elapsed")"
		fi
	fi

	print_summary

	if [ "$failed" -gt 0 ]; then
		err "${failed} user(s) failed. Review .err files and state CSV."
	fi
}

main "$@"
