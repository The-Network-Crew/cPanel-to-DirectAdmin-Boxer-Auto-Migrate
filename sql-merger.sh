#!/bin/bash
# sql-merger: merge MySQL databases from a cPanel SQL server into existing
# DirectAdmin accounts. Matches source cPanel users to DA users by primary
# domain — not by username — so mismatched usernames are no problem.
#
# Designed to run AFTER boxer.sh has restored web content. Does NOT touch
# web files — only creates databases via the DA API, imports data.
#
# Interactive: presents each database for naming before any changes.
# Generates credentials and writes them to a summary file at the end.

SQL_MERGER_VERSION="2026-03-30.7"

trap '__sm_rc=$?; echo "[FATAL] sql-merger died at line ${LINENO} (exit code ${__sm_rc})" >&2; echo "[FATAL] sql-merger died at line ${LINENO} (exit code ${__sm_rc})"; exit ${__sm_rc}' ERR

set -Eeuo pipefail
IFS=$'\n\t'

##############################
# defaults
##############################

SQL_HOST=""
SQL_USER="root"
SQL_PORT="22"
SQL_KEY=""
SQL_DUMP_DIR="/root/sql_merger_dumps"
LOCAL_DUMP_DIR="/root/sql-merger-dumps"

DA_ADMIN="admin"
DA_PORT="2222"
DA_PROTO="https"
DA_PASS=""

DOMAINS_CSV=""
DOMAINS_FILE=""
SOURCE_OWNER_MATCH=""

CLEANUP_SOURCE=1
CLEANUP_LOCAL=1
DRY_RUN=0
ADD_KEY=0

LOG_DIR="/root/sql-merger-logs"
RUN_ID="$(date +%F-%H%M%S)"
LOG_FILE=""
CREDS_FILE=""

##############################
# plan arrays — one entry per database to migrate
##############################

declare -a P_DOMAIN=()
declare -a P_CP_USER=()
declare -a P_DA_USER=()
declare -a P_SRC_DB=()
declare -a P_SRC_DBUSERS=()
declare -a P_DST_DB_SUFFIX=()
declare -a P_DST_DBUSER_SUFFIX=()
declare -a P_DST_DB=()
declare -a P_DST_DBUSER=()
declare -a P_PASSWORD=()
declare -a P_STATUS=()
declare -a P_NOTES=()

##############################
# utilities
##############################

ts() { date +'%F %T %Z'; }

log() {
	local msg="[$(ts)] [INFO]  $*"
	echo "$msg"
	if [ -n "${LOG_FILE:-}" ]; then echo "$msg" >> "$LOG_FILE"; fi
}

warn() {
	local msg="[$(ts)] [WARN]  $*"
	echo "$msg"
	if [ -n "${LOG_FILE:-}" ]; then echo "$msg" >> "$LOG_FILE"; fi
}

err() {
	local msg="[$(ts)] [ERROR] $*"
	echo "$msg" >&2
	echo "$msg"
	if [ -n "${LOG_FILE:-}" ]; then echo "$msg" >> "$LOG_FILE"; fi
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

validate_suffix() {
	local s="$1"
	[[ "$s" =~ ^[a-zA-Z0-9_]{1,32}$ ]]
}

generate_password() {
	LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24
}

##############################
# SSH helpers
##############################

sql_ssh_opts() {
	local opts=(-o BatchMode=yes -o ConnectTimeout=30 -o StrictHostKeyChecking=accept-new -p "$SQL_PORT")
	if [ -n "$SQL_KEY" ]; then opts+=(-i "$SQL_KEY"); fi
	printf '%s\n' "${opts[@]}"
}

sql_ssh() {
	local -a opts
	mapfile -t opts < <(sql_ssh_opts)
	ssh "${opts[@]}" "${SQL_USER}@${SQL_HOST}" "$@"
}

sql_rsync() {
	local src="$1" dst="$2"
	local -a opts
	mapfile -t opts < <(sql_ssh_opts)

	local ssh_cmd="ssh"
	local opt
	for opt in "${opts[@]}"; do
		ssh_cmd+=" $(printf '%q' "$opt")"
	done

	rsync -av --partial --append-verify -e "$ssh_cmd" \
		"${SQL_USER}@${SQL_HOST}:${src}" "$dst"
}

##############################
# DA API helpers
##############################

da_api_as_user() {
	local da_user="$1"
	local endpoint="$2"
	shift 2
	curl -sk -u "${DA_ADMIN}|${da_user}:${DA_PASS}" "$@" \
		"${DA_PROTO}://127.0.0.1:${DA_PORT}/${endpoint}"
}

verify_da_api() {
	local http_code
	http_code=$(curl -sk -o /dev/null -w '%{http_code}' \
		-u "${DA_ADMIN}:${DA_PASS}" \
		"${DA_PROTO}://127.0.0.1:${DA_PORT}/CMD_API_SHOW_ALL_USERS" 2>/dev/null)
	[ "$http_code" = "200" ]
}

create_da_database() {
	local da_user="$1" db_suffix="$2" dbuser_suffix="$3" password="$4"
	da_api_as_user "$da_user" "CMD_API_DATABASES" \
		-d "action=create&name=${db_suffix}&user=${dbuser_suffix}&passwd=${password}&passwd2=${password}"
}

##############################
# domain matching
##############################

lookup_da_user_for_domain() {
	local domain="$1"
	if [ -f /etc/virtual/domainowners ]; then
		awk -F': ' -v d="$domain" 'tolower($1)==tolower(d) {print $2; exit}' /etc/virtual/domainowners
	fi
}

##############################
# discovery
##############################

discover_source_data() {
	# Returns tab-separated lines: domain \t cpuser \t dbname \t db_users_csv
	# One line per database.
	local owner_filter="$SOURCE_OWNER_MATCH"

	local remote_script
	remote_script=$(cat <<'REMOTESCRIPT'
set -euo pipefail

OWNER_FILTER="__OWNER_FILTER__"

# Build user→domain map from /etc/trueuserdomains
declare -A user_domain
while IFS= read -r line; do
    domain=$(echo "$line" | awk -F: '{gsub(/^[ \t]+|[ \t]+$/,"",$1); print $1}')
    user=$(echo "$line" | awk -F: '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
    [ -n "$user" ] && [ -n "$domain" ] && user_domain["$user"]="$domain"
done < /etc/trueuserdomains

for user in "${!user_domain[@]}"; do
    # Owner filter if specified
    if [ -n "$OWNER_FILTER" ]; then
        if [ -f "/var/cpanel/users/${user}" ]; then
            file_owner=$(awk -F= '/^OWNER=/{print $2; exit}' "/var/cpanel/users/${user}")
            [ "$file_owner" = "$OWNER_FILTER" ] || continue
        else
            continue
        fi
    fi

    domain="${user_domain[$user]}"

    # Find databases owned by this user (cPanel convention: user_*)
    while IFS= read -r db; do
        [ -n "$db" ] || continue
        db_users=$(mysql -N -e "SELECT DISTINCT User FROM mysql.db WHERE Db='${db}'" 2>/dev/null | paste -sd, - || echo "")
        printf '%s\t%s\t%s\t%s\n' "$domain" "$user" "$db" "$db_users"
    done < <(mysql -N -e "SHOW DATABASES" 2>/dev/null | grep "^${user}_")
done
REMOTESCRIPT
)

	# Inject owner filter value (already validated as alphanumeric)
	remote_script="${remote_script/__OWNER_FILTER__/$owner_filter}"

	sql_ssh "$remote_script"
}

##############################
# SSH key setup
##############################

setup_ssh_key() {
	local key_path="${SQL_KEY:-/root/.ssh/id_ed25519}"
	local pub_path="${key_path}.pub"

	log "=== SSH key setup for ${SQL_USER}@${SQL_HOST}:${SQL_PORT} ==="

	if [ -f "$key_path" ] && [ -f "$pub_path" ]; then
		log "SSH key already exists: ${key_path}"
	else
		log "Generating ed25519 SSH key at ${key_path} ..."
		mkdir -p "$(dirname "$key_path")"
		chmod 700 "$(dirname "$key_path")"
		ssh-keygen -t ed25519 -N "" -f "$key_path" -C "sql-merger@$(hostname -f 2>/dev/null || hostname)" \
			|| err "ssh-keygen failed"
		log "Key generated: ${key_path}"
	fi

	log "Copying public key to ${SQL_USER}@${SQL_HOST}:${SQL_PORT} ..."
	log "You will be prompted for the remote password."
	ssh-copy-id -i "$pub_path" -p "$SQL_PORT" "${SQL_USER}@${SQL_HOST}" \
		|| err "ssh-copy-id failed — check credentials and that the remote host accepts password auth"

	log "Verifying SSH connectivity ..."
	local test_out
	test_out=$(ssh -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new \
		-p "$SQL_PORT" -i "$key_path" "${SQL_USER}@${SQL_HOST}" "echo sql-merger-ok" 2>&1) \
		|| err "SSH connectivity test failed: ${test_out}"
	[ "$test_out" = "sql-merger-ok" ] || err "Unexpected SSH test output: ${test_out}"

	log "Verifying MySQL access on source ..."
	local mysql_test
	mysql_test=$(ssh -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new \
		-p "$SQL_PORT" -i "$key_path" "${SQL_USER}@${SQL_HOST}" "mysql -N -e 'SELECT 1'" 2>&1) \
		|| err "MySQL access test failed: ${mysql_test}"
	[ "$(echo "$mysql_test" | tr -d '[:space:]')" = "1" ] || err "MySQL returned unexpected output: ${mysql_test}"
	log "MySQL access verified on ${SQL_HOST}."

	echo ""
	log "All set! Re-run sql-merger without --add-key to start the migration."
	exit 0
}

##############################
# preflight checks
##############################

require_root() {
	[ "$(id -u)" -eq 0 ] || err "Run as root on the DirectAdmin target host."
}

require_multiplexer() {
	[ -n "${STY:-}" ] || [ -n "${TMUX:-}" ] || \
		err "sql-merger must be run inside screen or tmux — a disconnected SSH session will kill it mid-run."
}

ensure_basic_dependencies() {
	local -a missing=()
	local bin
	for bin in ssh ssh-keygen ssh-copy-id; do
		command -v "$bin" >/dev/null 2>&1 || missing+=("$bin")
	done
	[ "${#missing[@]}" -eq 0 ] || err "Missing required commands: ${missing[*]}"
}

ensure_dependencies() {
	local -a missing=()
	local bin
	for bin in ssh rsync mysql mysqldump curl awk sed grep gzip gunzip; do
		command -v "$bin" >/dev/null 2>&1 || missing+=("$bin")
	done
	[ "${#missing[@]}" -eq 0 ] || err "Missing required commands: ${missing[*]}"
}

ensure_paths() {
	mkdir -p "$LOG_DIR" "$LOCAL_DUMP_DIR"
	LOG_FILE="${LOG_DIR}/sql-merger-${RUN_ID}.log"
	CREDS_FILE="${LOG_DIR}/credentials-${RUN_ID}.txt"
	: > "$LOG_FILE"
}

##############################
# usage
##############################

usage() {
	cat <<'EOF'
Usage:
  bash sql-merger.sh [options]

Required:
  --sql-host <host>          Source cPanel SQL server reachable by SSH

SSH key setup:
  --add-key                  Generate/copy SSH key to SQL server, verify
                             MySQL access, then exit

SSH options:
  --sql-user <name>          SSH user on SQL source (default: root)
  --sql-port <port>          SSH port (default: 22)
  --sql-key <path>           SSH private key path

DirectAdmin API:
  --da-admin <user>          DA admin username (default: admin)
  --da-port <port>           DA API port (default: 2222)
  --da-proto <http|https>    DA API protocol (default: https)

Filtering (optional — all domains by default):
  --domains <csv>            Only process these primary domains
  --domains-file <path>      File with one domain per line
  --source-owner <name>      Only match cPanel users with this OWNER= on source

Other:
  --dump-dir <path>          Source-side dump dir (default: /root/sql_merger_dumps)
  --local-dump-dir <path>    Target-side dump dir (default: /root/sql-merger-dumps)
  --no-cleanup-source        Keep dump files on source after import
  --no-cleanup-local         Keep dump files on target after import
  --log-dir <path>           Log directory (default: /root/sql-merger-logs)
  --dry-run                  Show plan without making any changes
  -h, --help                 Show this help

Outputs:
  - run log:      <log-dir>/sql-merger-<timestamp>.log
  - credentials:  <log-dir>/credentials-<timestamp>.txt
EOF
}

##############################
# arg parsing
##############################

declare -a DOMAIN_FILTERS=()

parse_domains_csv() {
	local csv="$1" item
	IFS=',' read -r -a __tmp <<< "$csv"
	for item in "${__tmp[@]}"; do
		item="$(echo "$item" | xargs)"
		if [ -n "$item" ]; then DOMAIN_FILTERS+=("$item"); fi
	done
}

parse_domains_file() {
	local file="$1"
	[ -f "$file" ] || err "Domains file not found: $file"
	local line
	while IFS= read -r line; do
		line="$(echo "$line" | sed -E 's/[[:space:]]+#.*$//' | xargs || true)"
		if [ -n "$line" ]; then DOMAIN_FILTERS+=("$line"); fi
	done < "$file"
}

domain_in_filter() {
	local d="$1"
	if [ "${#DOMAIN_FILTERS[@]}" -eq 0 ]; then return 0; fi
	local f
	for f in "${DOMAIN_FILTERS[@]}"; do
		if [[ "${d,,}" == "${f,,}" ]]; then return 0; fi
	done
	return 1
}

parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
			--sql-host)          shift; [ $# -gt 0 ] || err "Missing value for --sql-host"; SQL_HOST="$1" ;;
			--sql-user)          shift; [ $# -gt 0 ] || err "Missing value for --sql-user"; SQL_USER="$1" ;;
			--sql-port)          shift; [ $# -gt 0 ] || err "Missing value for --sql-port"; SQL_PORT="$1" ;;
			--sql-key)           shift; [ $# -gt 0 ] || err "Missing value for --sql-key"; SQL_KEY="$1" ;;
			--da-admin)          shift; [ $# -gt 0 ] || err "Missing value for --da-admin"; DA_ADMIN="$1" ;;
			--da-port)           shift; [ $# -gt 0 ] || err "Missing value for --da-port"; DA_PORT="$1" ;;
			--da-proto)          shift; [ $# -gt 0 ] || err "Missing value for --da-proto"; DA_PROTO="$1" ;;
			--domains)           shift; [ $# -gt 0 ] || err "Missing value for --domains"; DOMAINS_CSV="$1" ;;
			--domains-file)      shift; [ $# -gt 0 ] || err "Missing value for --domains-file"; DOMAINS_FILE="$1" ;;
			--source-owner)      shift; [ $# -gt 0 ] || err "Missing value for --source-owner"; SOURCE_OWNER_MATCH="$1" ;;
			--dump-dir)          shift; [ $# -gt 0 ] || err "Missing value for --dump-dir"; SQL_DUMP_DIR="$1" ;;
			--local-dump-dir)    shift; [ $# -gt 0 ] || err "Missing value for --local-dump-dir"; LOCAL_DUMP_DIR="$1" ;;
			--no-cleanup-source) CLEANUP_SOURCE=0 ;;
			--no-cleanup-local)  CLEANUP_LOCAL=0 ;;
			--log-dir)           shift; [ $# -gt 0 ] || err "Missing value for --log-dir"; LOG_DIR="$1" ;;
			--add-key)           ADD_KEY=1 ;;
			--dry-run)           DRY_RUN=1 ;;
			-h|--help)           usage; exit 0 ;;
			*)                   err "Unknown argument: $1" ;;
		esac
		shift
	done

	[ -n "$SQL_HOST" ] || err "--sql-host is required"

	if [ -n "$SOURCE_OWNER_MATCH" ]; then
		validate_owner "$SOURCE_OWNER_MATCH"
	fi
	validate_path_safe "$SQL_DUMP_DIR"
	validate_path_safe "$LOCAL_DUMP_DIR"

	if [ -n "$SQL_KEY" ]; then
		[ -f "$SQL_KEY" ] || err "SSH key not found: $SQL_KEY"
	fi

	if [ -n "$DOMAINS_CSV" ] && [ -n "$DOMAINS_FILE" ]; then
		err "Use --domains or --domains-file, not both"
	fi
	if [ -n "$DOMAINS_CSV" ]; then parse_domains_csv "$DOMAINS_CSV"; fi
	if [ -n "$DOMAINS_FILE" ]; then parse_domains_file "$DOMAINS_FILE"; fi
}

##############################
# interactive naming phase
##############################

prompt_db_plan() {
	local domain="$1" cp_user="$2" da_user="$3" src_db="$4" src_dbusers="$5"

	# Derive default suffix by stripping cPanel username prefix
	local default_db_suffix=""
	if [[ "$src_db" == "${cp_user}_"* ]]; then
		default_db_suffix="${src_db#${cp_user}_}"
	else
		default_db_suffix="$src_db"
	fi
	# Sanitise: strip anything not [a-zA-Z0-9_], truncate to 32
	default_db_suffix="$(echo "$default_db_suffix" | sed 's/[^a-zA-Z0-9_]/_/g' | head -c 32)"

	# Default DB user suffix — use first source DB user, strip prefix
	local default_dbuser_suffix="$default_db_suffix"
	if [ -n "$src_dbusers" ]; then
		local first_dbuser="${src_dbusers%%,*}"
		if [[ "$first_dbuser" == "${cp_user}_"* ]]; then
			default_dbuser_suffix="${first_dbuser#${cp_user}_}"
		else
			default_dbuser_suffix="$first_dbuser"
		fi
	fi
	default_dbuser_suffix="$(echo "$default_dbuser_suffix" | sed 's/[^a-zA-Z0-9_]/_/g' | head -c 32)"

	echo ""
	echo "───────────────────────────────────────────────────────────────"
	echo " Domain:         ${domain}"
	echo " Source cP user: ${cp_user}"
	echo " DA target user: ${da_user}"
	echo " Source DB:      ${src_db}"
	echo " Source DB user: ${src_dbusers:-<none found>}"
	echo "───────────────────────────────────────────────────────────────"

	local db_suffix=""
	local dbuser_suffix=""

	if [ "$DRY_RUN" -eq 1 ]; then
		# Auto-accept defaults in dry-run
		db_suffix="$default_db_suffix"
		dbuser_suffix="$default_dbuser_suffix"
	else
		echo " Enter 'skip' to skip this database."
		echo ""

		while true; do
			read -rp " New DB suffix (creates: ${da_user}_<suffix>) [${default_db_suffix}]: " db_suffix
			db_suffix="${db_suffix:-$default_db_suffix}"
			if [ "$db_suffix" = "skip" ]; then
				echo " Skipping ${src_db}."
				return 1
			fi
			if validate_suffix "$db_suffix"; then
				break
			fi
			echo " Invalid suffix — alphanumeric and underscore only, 1-32 chars."
		done

		while true; do
			read -rp " New DB user suffix (creates: ${da_user}_<suffix>) [${default_dbuser_suffix}]: " dbuser_suffix
			dbuser_suffix="${dbuser_suffix:-$default_dbuser_suffix}"
			if [ "$dbuser_suffix" = "skip" ]; then
				echo " Skipping ${src_db}."
				return 1
			fi
			if validate_suffix "$dbuser_suffix"; then
				break
			fi
			echo " Invalid suffix — alphanumeric and underscore only, 1-32 chars."
		done
	fi

	local password
	password=$(generate_password)

	local full_db="${da_user}_${db_suffix}"
	local full_dbuser="${da_user}_${dbuser_suffix}"

	echo ""
	echo " -> Will create:  DB = ${full_db}   User = ${full_dbuser}"
	echo ""

	P_DOMAIN+=("$domain")
	P_CP_USER+=("$cp_user")
	P_DA_USER+=("$da_user")
	P_SRC_DB+=("$src_db")
	P_SRC_DBUSERS+=("$src_dbusers")
	P_DST_DB_SUFFIX+=("$db_suffix")
	P_DST_DBUSER_SUFFIX+=("$dbuser_suffix")
	P_DST_DB+=("$full_db")
	P_DST_DBUSER+=("$full_dbuser")
	P_PASSWORD+=("$password")
	P_STATUS+=("PENDING")
	P_NOTES+=("")

	return 0
}

##############################
# show plan & confirm
##############################

show_plan() {
	local count="${#P_DOMAIN[@]}"
	echo ""
	echo "═══════════════════════════════════════════════════════════════════════"
	echo " PLANNED MIGRATIONS: ${count} database(s)"
	echo "═══════════════════════════════════════════════════════════════════════"
	local i
	for (( i=0; i<count; i++ )); do
		printf ' %2d. %-30s -> DA user: %s\n' "$((i+1))" "${P_DOMAIN[$i]}" "${P_DA_USER[$i]}"
		printf '     %-30s -> %s (user: %s)\n' "${P_SRC_DB[$i]}" "${P_DST_DB[$i]}" "${P_DST_DBUSER[$i]}"
	done
	echo "═══════════════════════════════════════════════════════════════════════"
}

confirm_plan() {
	echo ""
	read -rp " Proceed with migration? [y/N]: " answer
	case "$answer" in
		y|Y|yes|YES) return 0 ;;
		*) return 1 ;;
	esac
}

##############################
# execution — per database
##############################

execute_single_db() {
	local idx="$1"
	local da_user="${P_DA_USER[$idx]}"
	local src_db="${P_SRC_DB[$idx]}"
	local dst_db="${P_DST_DB[$idx]}"
	local dst_dbuser="${P_DST_DBUSER[$idx]}"
	local db_suffix="${P_DST_DB_SUFFIX[$idx]}"
	local dbuser_suffix="${P_DST_DBUSER_SUFFIX[$idx]}"
	local password="${P_PASSWORD[$idx]}"

	log "──── [${src_db} -> ${dst_db}] ────"

	# Check if target DB already exists
	local existing
	existing=$(mysql -N -e "SHOW DATABASES LIKE '${dst_db}'" 2>/dev/null || true)
	if [ -n "$existing" ]; then
		warn "Database '${dst_db}' already exists on target — skipping"
		P_STATUS[$idx]="SKIP"
		P_NOTES[$idx]="target DB already exists"
		return 0
	fi

	local remote_dump="${SQL_DUMP_DIR}/${src_db}.sql.gz"
	local local_dump="${LOCAL_DUMP_DIR}/${src_db}.sql.gz"

	# ---- dump on source ----
	local step_start=$SECONDS
	log "Dumping ${src_db} on source..."
	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would dump ${src_db} -> ${remote_dump}"
	else
		local dump_cmd
		dump_cmd="set -euo pipefail; mkdir -p '${SQL_DUMP_DIR}'; mysqldump --single-transaction --routines --triggers --events '${src_db}' | gzip > '${remote_dump}'"
		if ! sql_ssh "$dump_cmd" 2>&1 | tee -a "$LOG_FILE"; then
			P_STATUS[$idx]="FAIL"
			P_NOTES[$idx]="mysqldump failed"
			warn "mysqldump failed for ${src_db}"
			return 1
		fi
	fi
	local step_elapsed=$(( SECONDS - step_start ))
	log "[STATS] dump complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- transfer ----
	step_start=$SECONDS
	log "Transferring ${src_db}.sql.gz to target..."
	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would rsync ${remote_dump} -> ${local_dump}"
	else
		if ! sql_rsync "$remote_dump" "$local_dump" 2>&1 | tee -a "$LOG_FILE"; then
			P_STATUS[$idx]="FAIL"
			P_NOTES[$idx]="rsync transfer failed"
			warn "rsync failed for ${src_db}"
			return 1
		fi
		if [ ! -f "$local_dump" ]; then
			P_STATUS[$idx]="FAIL"
			P_NOTES[$idx]="dump missing on target after transfer"
			warn "Dump file missing after rsync: ${local_dump}"
			return 1
		fi
		local dump_size
		dump_size=$(stat -c '%s' "$local_dump" 2>/dev/null || stat -f '%z' "$local_dump" 2>/dev/null || echo 0)
		log "Dump transferred: $(fmt_bytes "$dump_size")"
	fi
	step_elapsed=$(( SECONDS - step_start ))
	log "[STATS] transfer complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- create DB via DA API ----
	step_start=$SECONDS
	log "Creating database ${dst_db} (user: ${dst_dbuser}) via DA API..."
	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would create DB ${dst_db} user ${dst_dbuser} for DA user ${da_user}"
	else
		local api_resp
		api_resp=$(create_da_database "$da_user" "$db_suffix" "$dbuser_suffix" "$password" 2>&1)

		if [[ "$api_resp" != *"error=0"* ]]; then
			# If the user already exists, delete it and retry once
			if [[ "$api_resp" == *"user+already+exists"* ]] || [[ "$api_resp" == *"user already exists"* ]]; then
				warn "DB user '${dst_dbuser}' already exists — deleting stale user and retrying"
				local del_resp
				del_resp=$(da_api_as_user "$da_user" "CMD_API_DATABASES" \
					-d "action=delete&select0=${dst_db}" 2>&1) || true
				log "DA API delete response: $(echo "$del_resp" | tr '&' ' ' | head -c 200)"
				api_resp=$(create_da_database "$da_user" "$db_suffix" "$dbuser_suffix" "$password" 2>&1)
				if [[ "$api_resp" != *"error=0"* ]]; then
					echo "$api_resp" >> "$LOG_FILE"
					P_STATUS[$idx]="FAIL"
					P_NOTES[$idx]="DA API create failed after retry: $(echo "$api_resp" | tr '&' ' ' | head -c 200)"
					warn "DA API failed for ${dst_db} after retry: ${api_resp}"
					return 1
				fi
				log "DA API: database + user created (after stale user cleanup)"
			else
				# Log the full response for debugging
				echo "$api_resp" >> "$LOG_FILE"
				P_STATUS[$idx]="FAIL"
				P_NOTES[$idx]="DA API create failed: $(echo "$api_resp" | tr '&' ' ' | head -c 200)"
				warn "DA API failed for ${dst_db}: ${api_resp}"
				return 1
			fi
		else
			log "DA API: database + user created"
		fi
	fi
	step_elapsed=$(( SECONDS - step_start ))
	log "[STATS] DA create complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- import ----
	step_start=$SECONDS
	log "Importing data into ${dst_db}..."
	if [ "$DRY_RUN" -eq 1 ]; then
		log "[DRY-RUN] Would import ${local_dump} into ${dst_db}"
	else
		if ! gunzip -c "$local_dump" | mysql "$dst_db" 2>&1 | tee -a "$LOG_FILE"; then
			P_STATUS[$idx]="FAIL"
			P_NOTES[$idx]="mysql import failed (DB was created — may need manual cleanup)"
			warn "Import failed for ${dst_db} — the database was created but may be empty/partial"
			return 1
		fi

		# Sanity check — count tables
		local table_count
		table_count=$(mysql -N -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='${dst_db}'" 2>/dev/null || echo "?")
		log "Import complete — ${table_count} table(s) in ${dst_db}"
	fi
	step_elapsed=$(( SECONDS - step_start ))
	log "[STATS] import complete — elapsed: $(fmt_elapsed "$step_elapsed")"

	# ---- cleanup dump files ----
	if [ "$DRY_RUN" -eq 0 ]; then
		if [ "$CLEANUP_SOURCE" -eq 1 ]; then
			sql_ssh "rm -f '${remote_dump}'" 2>/dev/null || true
		fi
		if [ "$CLEANUP_LOCAL" -eq 1 ]; then
			rm -f "$local_dump" 2>/dev/null || true
		fi
	fi

	P_STATUS[$idx]="PASS"
	P_NOTES[$idx]="imported OK"
	log "Done: ${src_db} -> ${dst_db}"
	return 0
}

##############################
# wp-config.php patching
##############################

wp_config_get_db_name() {
	# Extract current DB_NAME value from a wp-config.php
	local wpconf="$1"
	sed -n "s/^[[:space:]]*define[[:space:]]*(.*['\"]DB_NAME['\"][[:space:]]*,[[:space:]]*['\"]\\([^'\"]*\\)['\"].*/\\1/p" "$wpconf" | head -1
}

patch_wp_config() {
	# Patch a single wp-config.php — only the lines whose current values match
	# the source DB name / source DB users we're replacing FROM.
	# Args: wp_config_path src_db src_dbusers_csv dst_db dst_dbuser dst_password
	local wpconf="$1" src_db="$2" src_dbusers_csv="$3"
	local dst_db="$4" dst_dbuser="$5" dst_pass="$6"

	# Back up before touching (only once per file)
	if [ ! -f "${wpconf}.sqlmerger-bak" ]; then
		cp -a "$wpconf" "${wpconf}.sqlmerger-bak"
	fi

	# Replace DB_NAME only if it currently matches the source DB
	sed -i \
		-e "s|^\(\s*define\s*(\s*['\"]DB_NAME['\"]\s*,\s*\)['\"]${src_db}['\"]|\1'${dst_db}'|" \
		"$wpconf"

	# Replace DB_USER if it matches any of the source DB users
	local src_user
	IFS=',' read -r -a __src_users <<< "$src_dbusers_csv"
	for src_user in "${__src_users[@]}"; do
		src_user="$(echo "$src_user" | xargs)"
		if [ -n "$src_user" ]; then
			sed -i \
				-e "s|^\(\s*define\s*(\s*['\"]DB_USER['\"]\s*,\s*\)['\"]${src_user}['\"]|\1'${dst_dbuser}'|" \
				"$wpconf"
		fi
	done

	# Always update DB_PASSWORD and DB_HOST (they go with the new DB)
	# But only if DB_NAME was actually changed (check the file now)
	local current_db
	current_db=$(wp_config_get_db_name "$wpconf")
	if [ "$current_db" = "$dst_db" ]; then
		sed -i \
			-e "s|^\(\s*define\s*(\s*['\"]DB_PASSWORD['\"]\s*,\s*\)['\"][^'\"]*['\"]|\1'${dst_pass}'|" \
			-e "s|^\(\s*define\s*(\s*['\"]DB_HOST['\"]\s*,\s*\)['\"][^'\"]*['\"]|\1'localhost'|" \
			"$wpconf"
	fi
}

patch_wp_configs() {
	# Iterate all planned entries; for PASS or DRY-RUN entries, find and patch wp-config.php
	local count="${#P_DOMAIN[@]}"
	local patched=0 not_found=0 skipped=0 no_match=0

	echo ""
	echo "═══════════════════════════════════════════════════════════════════════"
	echo " WP-CONFIG.PHP UPDATES"
	echo "═══════════════════════════════════════════════════════════════════════"

	local i
	for (( i=0; i<count; i++ )); do
		if [ "${P_STATUS[$i]}" != "PASS" ] && [ "${P_STATUS[$i]}" != "DRY-RUN" ]; then
			skipped=$((skipped + 1))
			continue
		fi

		local da_user="${P_DA_USER[$i]}"
		local domain="${P_DOMAIN[$i]}"
		local src_db="${P_SRC_DB[$i]}"
		local src_dbusers="${P_SRC_DBUSERS[$i]}"
		local dst_db="${P_DST_DB[$i]}"
		local dst_dbuser="${P_DST_DBUSER[$i]}"
		local password="${P_PASSWORD[$i]}"
		local wpconf="/home/${da_user}/domains/${domain}/public_html/wp-config.php"

		if [ ! -f "$wpconf" ]; then
			log "  No wp-config.php at ${wpconf} — skipping"
			not_found=$((not_found + 1))
			continue
		fi

		# Check if this wp-config.php actually uses the source DB we're replacing
		local current_db
		current_db=$(wp_config_get_db_name "$wpconf")
		if [ "$current_db" != "$src_db" ]; then
			log "  ${wpconf}: DB_NAME='${current_db}' does not match source '${src_db}' — skipping"
			no_match=$((no_match + 1))
			continue
		fi

		if [ "$DRY_RUN" -eq 1 ]; then
			log "  [DRY-RUN] Would patch ${wpconf}"
			log "    DB_NAME     ${src_db} -> ${dst_db}"
			log "    DB_USER     -> ${dst_dbuser}"
			log "    DB_PASSWORD -> (generated)"
			log "    DB_HOST     -> localhost"
		else
			log "  Patching ${wpconf} ..."
			if patch_wp_config "$wpconf" "$src_db" "$src_dbusers" "$dst_db" "$dst_dbuser" "$password"; then
				log "    DB_NAME     ${src_db} -> ${dst_db}"
				log "    DB_USER     -> ${dst_dbuser}"
				log "    DB_PASSWORD -> (set)"
				log "    DB_HOST     -> localhost"
				log "    Backup at: ${wpconf}.sqlmerger-bak"
			else
				warn "  Failed to patch ${wpconf} — check manually"
			fi
		fi
		patched=$((patched + 1))
	done

	echo ""
	local wp_summary="${patched} patched, ${not_found} no wp-config, ${no_match} DB_NAME mismatch, ${skipped} skipped (non-passing)"
	log "wp-config summary: ${wp_summary}"
	echo "═══════════════════════════════════════════════════════════════════════"
}

##############################
# credential summary
##############################

print_credentials() {
	local count="${#P_DOMAIN[@]}"
	local pass_count=0 fail_count=0 skip_count=0 dryrun_count=0

	local i
	for (( i=0; i<count; i++ )); do
		case "${P_STATUS[$i]}" in
			PASS)    pass_count=$((pass_count + 1)) ;;
			FAIL)    fail_count=$((fail_count + 1)) ;;
			SKIP)    skip_count=$((skip_count + 1)) ;;
			DRY-RUN) dryrun_count=$((dryrun_count + 1)) ;;
		esac
	done

	local summary="${pass_count} passed, ${fail_count} failed, ${skip_count} skipped"
	if [ "$dryrun_count" -gt 0 ]; then summary="${dryrun_count} planned (dry-run)"; fi

	local divider="════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════"
	local header
	header=$(printf ' %-28s %-18s %-28s %-28s %-26s %s' "Domain" "DA User" "Database" "DB User" "Password" "Status")
	local sep
	sep=$(printf ' %-28s %-18s %-28s %-28s %-26s %s' "────────────────────────────" "──────────────────" "────────────────────────────" "────────────────────────────" "──────────────────────────" "──────")

	{
		echo ""
		echo "$divider"
		echo " SQL-MERGER CREDENTIAL REPORT  —  $(ts)"
		echo " Source: ${SQL_USER}@${SQL_HOST}  —  Target: localhost (DA)"
		echo " Results: ${summary}"
		echo "$divider"
		echo ""
		echo "$header"
		echo "$sep"

		for (( i=0; i<count; i++ )); do
			if [ "${P_STATUS[$i]}" != "PASS" ] && [ "${P_STATUS[$i]}" != "DRY-RUN" ]; then continue; fi
			local show_pass="${P_PASSWORD[$i]}"
			if [ "${P_STATUS[$i]}" = "DRY-RUN" ]; then show_pass="(dry-run)"; fi
			printf ' %-28s %-18s %-28s %-28s %-26s %s\n' \
				"${P_DOMAIN[$i]}" "${P_DA_USER[$i]}" "${P_DST_DB[$i]}" "${P_DST_DBUSER[$i]}" "$show_pass" "${P_STATUS[$i]}"
		done

		# Show non-passing entries
		local has_other=0
		for (( i=0; i<count; i++ )); do
			if [ "${P_STATUS[$i]}" = "PASS" ] || [ "${P_STATUS[$i]}" = "DRY-RUN" ]; then continue; fi
			if [ "$has_other" -eq 0 ]; then
				echo ""
				echo " Non-passing:"
				echo "$sep"
				has_other=1
			fi
			printf ' %-28s %-18s %-28s %-28s %-26s %s\n' \
				"${P_DOMAIN[$i]}" "${P_DA_USER[$i]}" "${P_DST_DB[$i]}" "${P_DST_DBUSER[$i]}" "-" "${P_STATUS[$i]}: ${P_NOTES[$i]}"
		done

		echo ""
		echo "$divider"
	} | tee "$CREDS_FILE"

	chmod 600 "$CREDS_FILE"
	log "Credentials saved to: ${CREDS_FILE}"
	warn "PROTECT THIS FILE — it contains database passwords."
}

##############################
# main
##############################

main() {
	echo ""
	echo "sql-merger v${SQL_MERGER_VERSION} — starting up"
	echo ""

	echo "[*] Parsing arguments..."
	parse_args "$@"
	echo "[*] Args OK — host=${SQL_HOST} port=${SQL_PORT} add-key=${ADD_KEY}"

	log "sql-merger ${RUN_ID} (v${SQL_MERGER_VERSION}) on $(hostname -f 2>/dev/null || hostname)"

	echo "[*] Checking root..."
	require_root
	echo "[*] Root OK"

	# --add-key is a quick one-shot — no need for screen/tmux or full deps
	if [ "$ADD_KEY" -eq 1 ]; then
		echo "[*] Running SSH key setup..."
		ensure_basic_dependencies
		setup_ssh_key
		# setup_ssh_key calls exit 0 — should never reach here
		exit 0
	fi

	if [ "$DRY_RUN" -eq 0 ]; then
		echo "[*] Checking multiplexer..."
		require_multiplexer
	fi
	echo "[*] Checking dependencies..."
	ensure_dependencies
	echo "[*] Setting up paths..."
	ensure_paths

	# ---- verify SSH to source ----
	log "Verifying SSH to ${SQL_USER}@${SQL_HOST}:${SQL_PORT}..."
	local ssh_test
	ssh_test=$(sql_ssh "echo sql-merger-ok" 2>&1) || err "SSH to SQL source failed: ${ssh_test}"
	[ "$ssh_test" = "sql-merger-ok" ] || err "Unexpected SSH output: ${ssh_test}"
	log "SSH: OK"

	# ---- verify MySQL access on source ----
	log "Verifying MySQL access on source..."
	local mysql_test
	mysql_test=$(sql_ssh "mysql -N -e 'SELECT 1'" 2>&1) || err "MySQL not accessible on source: ${mysql_test}"
	log "MySQL on source: OK"

	# ---- verify DA components on target ----
	[ -f /etc/virtual/domainowners ] || err "/etc/virtual/domainowners not found — is this a DirectAdmin server?"

	# ---- get DA admin password (skip for dry-run) ----
	if [ "$DRY_RUN" -eq 0 ]; then
		if [ -z "${DA_PASS:-}" ]; then
			echo ""
			read -rsp "DirectAdmin admin (${DA_ADMIN}) password: " DA_PASS
			echo ""
			[ -n "$DA_PASS" ] || err "DA password is required"
		fi

		# ---- verify DA API ----
		log "Verifying DA API access..."
		verify_da_api || err "DA API auth failed — check --da-admin / password / --da-port / --da-proto"
		log "DA API: OK"
	else
		log "[DRY-RUN] Skipping DA password and API verification"
	fi

	# ---- check /etc/trueuserdomains on source ----
	log "Checking /etc/trueuserdomains on source..."
	sql_ssh "test -f /etc/trueuserdomains" 2>/dev/null \
		|| err "/etc/trueuserdomains not found on source — is it a cPanel server?"
	log "Source cPanel: OK"

	# ---- discover ----
	log "Discovering databases on source SQL server..."
	local raw_data
	raw_data=$(discover_source_data) || err "Discovery failed — check source SSH and MySQL access"

	if [ -z "$raw_data" ]; then
		err "No databases found on source. Verify /etc/trueuserdomains and that users have user_* databases."
	fi

	local total_source_dbs
	total_source_dbs=$(echo "$raw_data" | wc -l | tr -d ' ')
	log "Found ${total_source_dbs} database(s) across source cPanel users."

	# ---- match & prompt ----
	local matched=0 skipped_no_da=0 skipped_filter=0

	echo ""
	echo "═══════════════════════════════════════════════════════════════════════"
	echo " DATABASE NAMING — for each source database, choose the target name."
	echo " DA databases are always: <username>_<suffix>"
	echo "═══════════════════════════════════════════════════════════════════════"

	while IFS=$'\t' read -r -u3 domain cp_user src_db src_dbusers; do
		[ -n "$domain" ] || continue

		# Apply domain filter
		if ! domain_in_filter "$domain"; then
			skipped_filter=$((skipped_filter + 1))
			continue
		fi

		# Lookup DA user for this domain
		local da_user
		da_user=$(lookup_da_user_for_domain "$domain")
		if [ -z "$da_user" ]; then
			warn "Domain '${domain}' (source cP user: ${cp_user}) has no DA user — skipping"
			skipped_no_da=$((skipped_no_da + 1))
			continue
		fi

		# Verify DA user directory exists
		if [ ! -d "/usr/local/directadmin/data/users/${da_user}" ]; then
			warn "DA user '${da_user}' data dir missing for domain '${domain}' — skipping"
			skipped_no_da=$((skipped_no_da + 1))
			continue
		fi

		prompt_db_plan "$domain" "$cp_user" "$da_user" "$src_db" "$src_dbusers" || continue
		matched=$((matched + 1))
	done 3<<< "$raw_data"

	echo ""
	if [ "$skipped_filter" -gt 0 ]; then log "${skipped_filter} database(s) skipped — domain not in filter"; fi
	if [ "$skipped_no_da" -gt 0 ]; then warn "${skipped_no_da} database(s) skipped — domain not found on DA"; fi

	if [ "$matched" -eq 0 ]; then
		log "No databases to migrate. Nothing to do."
		exit 0
	fi

	# ---- show plan ----
	show_plan

	if [ "$DRY_RUN" -eq 1 ]; then
		# Mark all entries as DRY-RUN for the report
		for (( i=0; i<${#P_DOMAIN[@]}; i++ )); do
			P_STATUS[$i]="DRY-RUN"
		done
		patch_wp_configs
		log "[DRY-RUN] Plan shown above. No changes made."
		print_credentials
		log "sql-merger v${SQL_MERGER_VERSION} — finished."
		exit 0
	fi

	# ---- confirm ----
	if ! confirm_plan; then
		log "Aborted by user."
		exit 0
	fi

	# ---- execute ----
	log "Starting SQL merger run ${RUN_ID}"
	log "Source: ${SQL_USER}@${SQL_HOST} — Target: localhost"

	local run_start=$SECONDS
	local failed=0
	local rc=0

	for (( i=0; i<${#P_DOMAIN[@]}; i++ )); do
		execute_single_db "$i" && rc=0 || rc=$?
		if [ "$rc" -ne 0 ]; then failed=$((failed + 1)); fi
	done

	local run_elapsed=$(( SECONDS - run_start ))
	log "[STATS] SQL merger run complete — total elapsed: $(fmt_elapsed "$run_elapsed")"

	# ---- patch wp-config.php files ----
	patch_wp_configs

	# ---- summary ----
	print_credentials

	if [ "$failed" -gt 0 ]; then
		err "${failed} database(s) failed. Review log: ${LOG_FILE}"
	fi

	log "All done. Go forth and check your sites, legend."
	log "sql-merger v${SQL_MERGER_VERSION} — finished."
}

main "$@"
