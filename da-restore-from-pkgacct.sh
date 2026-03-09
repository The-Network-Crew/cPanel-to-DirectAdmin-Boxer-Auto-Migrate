#!/bin/bash
# da-restore.sh :: Run on the DirectAdmin server as below...
# 
# bash da-restore.sh >> /usr/local/directadmin/data/task.queue
# /usr/local/directadmin/dataskq d

# shellcheck disable=SC2034

set -euo pipefail
IFS=$'\n\t'

#Who is doing the restore?
OWNER="${OWNER:-admin}"
LOCAL_PATH="${LOCAL_PATH:-/home/${OWNER}/user_backups}"

#choice can be 'file' to get it from the backup
#or 'select' which will use the ip set.
IP_CHOICE="${IP_CHOICE:-select}"
IP="${IP:-}"
USERS_CSV="${USERS_CSV:-}"

[ -d "${LOCAL_PATH}" ] || {
       echo "[ERROR] Local path does not exist: ${LOCAL_PATH}" >&2
       exit 1
}

echo -n "action=restore&local_path=${LOCAL_PATH}&owner=${OWNER}&when=now&where=local&type=admin";

if [ "${IP_CHOICE}" = "select" ]; then
       [ -n "${IP}" ] || {
              echo "[ERROR] IP is required when IP_CHOICE=select" >&2
              exit 1
       }
       echo -n "&ip_choice=select&ip=${IP}";
else
       echo -n "&ip_choice=${IP_CHOICE}";
fi

cd "${LOCAL_PATH}"
COUNT=0

user_in_selection() {
       local user="$1"
       [ -z "$USERS_CSV" ] && return 0
       [[ ",${USERS_CSV}," == *",${user},"* ]]
}

username_from_backup() {
       local backup_file="$1"
       echo "$backup_file" | sed -E 's/^cpmove-([A-Za-z0-9_]+).*\.tar\.gz$/\1/'
}

shopt -s nullglob
for i in *.gz; do
       user=$(username_from_backup "$i")
       if ! user_in_selection "$user"; then
              continue
       fi
       echo -n "&select${COUNT}=$i";
       COUNT=$(( COUNT + 1 ))
done
shopt -u nullglob

echo "";

if [ "${COUNT}" -eq 0 ]; then
       exit 1;
fi

exit 0;
