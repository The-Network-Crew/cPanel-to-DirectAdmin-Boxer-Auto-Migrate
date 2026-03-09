#!/bin/bash

#This script is written by Martynas Bendorius and DirectAdmin
#cPanel->DirectAdmin backup conversion tool

do_exit() {
	if [ "$2" != "" ]; then
		>&2 echo "$2"
	fi
	exit "$1"
}

if [ $# -lt 2 ]; then
	echo "Usage:";
	echo "$0 <full path to cpanel backup> <full path to directadmin backup directory>";
	echo "you gave #$#: $0 $1 $2";
	echo ""
	echo "Supported environment variables:";
	echo "	CP2DA_OUTPUT_FILE - full path to DA backup (output) file";
	echo "	CP2DA_OWNER       - account owner in DA backup";
	exit 0;
fi

EXIT_CODE=0
INPUT_FILE=$1
BACKUP_DIR=$2
SCRIPT_DIR=$(dirname "$0")

if echo "${INPUT_FILE}" | grep -q '^/' | grep -q 'tar\.gz$'; then
	do_exit 99 "${INPUT_FILE} is incorrect FULL path to the backup file. Exiting."
fi


if [ "${BACKUP_DIR}" = "" ] || [ "${BACKUP_DIR}" = "/" ]; then
	do_exit 100 "Full path to DA backups cannot be set to ${BACKUP_DIR}. Exiting..."
fi

if [ ! -w "${BACKUP_DIR}" ]; then
	do_exit 100 "${BACKUP_DIR} is not writeable"
fi

if ! command -v doveadm > /dev/null; then
	do_exit 100 "Command 'doveadmin' is not available, please make sure dovecot is installed. Exiting..."
fi


PIGZ=false
if command -v pigz > /dev/null; then
	PIGZ=true
fi

getOpt() {
	#$1 is option name
	GET_OPTION=$(grep -v "^$1=$" "${CP_ROOT}/cp/${USERNAME}" | grep -m1 "^$1=" | cut -d= -f2)
	if [ "${GET_OPTION}" = "" ]; then
		GET_OPTION=unlimited
	fi
	echo "${GET_OPTION}"
}

getPkgOpt() {
	#$1 is option name, $2 path to package file
	GET_OPTION=$(grep -v "^$1=$" "${2}" | grep -m1 "^$1=" | cut -d= -f2)
	if [ "${GET_OPTION}" = "" ]; then
		GET_OPTION=unlimited
	fi
	echo "${GET_OPTION}"
}

has_dovecot_24() {
	local ver
	ver=$(dovecot --version) || return 1

	[ "${ver:0:3}" = "2.4" ]
}

convert_mail_path() (
	local from=$1
	local to=$2
	local src_format=$3

	local doveadm_bin="doveadm"

	mkdir -p "${to}"
	if [ "$(id -u)" -eq "0" ]; then
		chown -R mail:mail "${from}"
		chown mail:mail "${to}"
		doveadm_bin="su mail -s $(command -v doveadm) --"
	fi

	if has_dovecot_24; then
		$doveadm_bin -o first_valid_uid=1 -o "mail_home=${to}" sync --no-userdb-lookup  -1 -R -f -p mailbox_list_utf8=yes "${src_format}:${from}"
	else
		$doveadm_bin -o first_valid_uid=1 -o "mail_home=${to}" sync                     -1 -R -f "${src_format}:${from}:UTF-8"
	fi
)

# clean_htaccess_cp_php_handlers makes sure CP specific PHP handlers (used by
# cpanel PHP selector) are commended out in a given domains directory.
clean_htaccess_cp_php_handlers() {
	local dir=$1

	if [ -z "${dir}" ]; then
		return 0
	fi
	find "${dir}" -maxdepth 3 -name '.htaccess' -exec sed -i 's|^\(\s*AddHandler application/x-httpd-ea.*\)$|#\1|' '{}' \;
}

USERNAME=$(echo "${INPUT_FILE}" | grep -o '[A-Za-z0-9]*\.tar.gz' | perl -p0 -e 's|\.tar\.gz||g')
TAR_GZ=true
if [ "${USERNAME}" = "" ]; then
	USERNAME=$(echo "${INPUT_FILE}" | grep -o '[A-Za-z0-9]*\.tar' | perl -p0 -e 's|\.tar||g')
	TAR_GZ=false
	PIGZ=false
fi
if [ "${USERNAME}" = "" ]; then
	do_exit 102 "Unable to extract username from tarball name"
fi

CP_ROOT="${BACKUP_DIR}/${USERNAME}_cpanel_to_convert"
DA_ROOT="${BACKUP_DIR}/${USERNAME}"

echo "Converting ${USERNAME} (${INPUT_FILE})..."

#Cleanup
if [ -d "${CP_ROOT}" ]; then
	echo "Found previous ${CP_ROOT}. Removing..."
	rm -rf "${CP_ROOT}"
fi
if [ -d "${DA_ROOT}" ]; then
	echo "Found previous ${DA_ROOT}. Removing..."
	rm -rf "${DA_ROOT}"
fi

mkdir -p "${DA_ROOT}/domains"
mkdir -p "${DA_ROOT}/backup"
mkdir -p "${CP_ROOT}"

if ${PIGZ}; then
	pigz -dc "${INPUT_FILE}" | tar xfC - "${CP_ROOT}" --strip=1 --no-same-owner
	RC=$?
elif ${TAR_GZ}; then
	tar xzfC "${INPUT_FILE}" "${CP_ROOT}" --strip=1 --no-same-owner
	RC=$?
else
	tar xfC "${INPUT_FILE}" "${CP_ROOT}" --strip=1 --no-same-owner
	RC=$?
fi

if [ $RC -ne 0 ]; then
	rm -rf "${CP_ROOT}"
	do_exit 4 "Failed to extract ${INPUT_FILE} to ${CP_ROOT}"
fi

if [ ! -s "${CP_ROOT}/cp/${USERNAME}" ]; then
	do_exit 3 "Unable to find cPanel user configuration in ${CP_ROOT}/cp/${USERNAME}"
fi

ACCOUNT_SUSPENDED="no"
if grep -q '^SUSPENDED=1$' "${CP_ROOT}/cp/${USERNAME}"; then
	ACCOUNT_SUSPENDED="yes"
fi

if [ ! -s "${CP_ROOT}/shadow" ]; then
	echo "Unable to find ${CP_ROOT}/shadow, exiting..."
	echo "Removing ${CP_ROOT}..."
	rm -rf "${CP_ROOT}"
	do_exit 5
fi

if [ -s "${CP_ROOT}/homedir.tar" ]; then
	if [ ! -d "${CP_ROOT}/homedir" ]; then
		echo "Creating empty ${CP_ROOT}/homedir..."
		mkdir -p "${CP_ROOT}/homedir"
	fi
	echo "Extracting homedir.tar..."
	if ! tar xfC "${CP_ROOT}/homedir.tar" "${CP_ROOT}/homedir"; then
		rm -rf "${CP_ROOT}"
		do_exit 6 "Failed to extract ${CP_ROOT}/homedir.tar"
	fi
fi

#Copy the shadow file
cat "${CP_ROOT}/shadow" > "${DA_ROOT}/backup/.shadow"

#Get default domain name
DEFAULT_DOMAIN_NAME=$(getOpt DNS)
if [ "${DEFAULT_DOMAIN_NAME}" = "unlimited" ]; then
	do_exit 103 "Unable to get default domain name..."
fi
DEFAULT_IP=$(getOpt IP)
if [ "${DEFAULT_IP}" = "unlimited" ]; then
	echo "Unable to get default IP address"
	DEFAULT_IP=127.0.0.1
fi
DEFAULT_CGI=$(getOpt HASCGI)
CPANEL_HASCGI=ON
if [ "${DEFAULT_CGI}" = "unlimited" ]; then
	DEFAULT_CGI=OFF
	CPANEL_HASCGI=OFF
elif [ "${DEFAULT_CGI}" = "0" ]; then
	CPANEL_HASCGI=OFF
fi
DEFAULT_SHELL=$(getOpt HASSHELL)
if [ "${DEFAULT_SHELL}" = "1" ]; then
	CPANEL_HASSHELL=ON
else
	CPANEL_HASSHELL=OFF
fi

CPANEL_MAXPARK=$(getOpt MAXPARK)
CPANEL_MAXFTP=$(getOpt MAXFTP)
#System ftp account is there by default, so, limit should be set to 1 in such case
if [ "${CPANEL_MAXFTP}" = "0" ]; then
    CPANEL_MAXFTP="1"
fi
CPANEL_MAXSQL=$(getOpt MAXSQL)
CPANEL_MAXSUB=$(getOpt MAXSUB)
CPANEL_MAXPOP=$(getOpt MAXPOP)
CPANEL_STARTDATE=$(getOpt STARTDATE)

CPANEL_MAXLST=$(getOpt MAXLST)
CPANEL_MAXADDON=$(getOpt MAXADDON)
if [ "${CPANEL_MAXADDON}" = "0" ]; then
    CPANEL_MAXADDON="1"
fi
CPANEL_MAX_EMAILACCT_QUOTA=$(getOpt MAX_EMAILACCT_QUOTA)
CPANEL_BWLIMIT=$(getOpt BWLIMIT)
if [ "${CPANEL_BWLIMIT}" = "0" ]; then
    CPANEL_BWLIMIT="unlimited"
fi
CPANEL_PLAN=$(getOpt PLAN)
CPANEL_PLAN=$(echo "${CPANEL_PLAN}" | tr ' ' '_')
if [ -z "${CP2DA_OWNER}" ]; then
	CP2DA_OWNER=$(getOpt OWNER)
	if [ "${CP2DA_OWNER}" = "root" ] || [ "${CP2DA_OWNER}" = "${USERNAME}" ]; then
		CP2DA_OWNER="admin"
	fi
fi
CPANEL_QUOTA=$(cat "${CP_ROOT}/quota")
if [ "${CPANEL_QUOTA}" = "0" ]; then
    CPANEL_QUOTA="unlimited"
fi
if [ "${CPANEL_BWLIMIT}" != "unlimited" ]; then
	CPANEL_BWLIMIT=$((CPANEL_BWLIMIT / 1048576))
fi

#Set customer email address
if [ -s "${CP_ROOT}/homedir/.contactemail" ]; then
	CUSTOMER_EMAIL=$(head -n1 "${CP_ROOT}/homedir/.contactemail" | cut -d: -f2 | tr -d ' ')
	if ! echo "${CUSTOMER_EMAIL}" | grep -q '@'; then
		CUSTOMER_EMAIL="${USERNAME}@${DEFAULT_DOMAIN_NAME}"
	fi
else
	CUSTOMER_EMAIL="${USERNAME}@${DEFAULT_DOMAIN_NAME}"
fi

doConvertDomain() {
	CONVERTED_DOMAIN=$1
	DEFAULT_DOMAIN=$2
	ASSOCIATED_SUBDOMAIN=$3
	ALIAS_DOMAIN=$4
	echo "Converting ${CONVERTED_DOMAIN} domain..."
	mkdir -p "${DA_ROOT}/domains/${CONVERTED_DOMAIN}"
	mkdir -p "${DA_ROOT}/backup/${CONVERTED_DOMAIN}"

	CONVERTED_DOMAIN_CONFIG=${CP_ROOT}/userdata/${CONVERTED_DOMAIN}
	CONVERTED_DOMAIN_SSL_CONFIG=${CP_ROOT}/userdata/${CONVERTED_DOMAIN}_SSL
	APACHE_TLS_PATH=${CP_ROOT}/apache_tls/${CONVERTED_DOMAIN}
	if [ -n "${ALIAS_DOMAIN}" ]; then
		CONVERTED_DOMAIN_CONFIG=${CP_ROOT}/userdata/${ALIAS_DOMAIN}
		CONVERTED_DOMAIN_SSL_CONFIG=${CP_ROOT}/userdata/${ALIAS_DOMAIN}_SSL
		APACHE_TLS_PATH=${CP_ROOT}/apache_tls/${ALIAS_DOMAIN}
	fi

	#Copy SSL cert/key/cacert
	if [ -e "${CONVERTED_DOMAIN_SSL_CONFIG}" ]; then
		if grep -q '^sslcacertificatefile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}"; then
			CACERT_NAME=$(grep -m1 '^sslcacertificatefile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}" | grep -o '[^/]*\.cabundle')
			if [ -s "${CP_ROOT}/sslcerts/${CACERT_NAME}" ]; then
				echo "Copying ${CACERT_NAME} CA root certificate..."
				mv "${CP_ROOT}/sslcerts/${CACERT_NAME}" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert"
			else
				#No CA Bundle in backup, likely Comodo bundle
				cat << 'EOF' > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert"
-----BEGIN CERTIFICATE-----
MIIGCDCCA/CgAwIBAgIQKy5u6tl1NmwUim7bo3yMBzANBgkqhkiG9w0BAQwFADCB
hTELMAkGA1UEBhMCR0IxGzAZBgNVBAgTEkdyZWF0ZXIgTWFuY2hlc3RlcjEQMA4G
A1UEBxMHU2FsZm9yZDEaMBgGA1UEChMRQ09NT0RPIENBIExpbWl0ZWQxKzApBgNV
BAMTIkNPTU9ETyBSU0EgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMTQwMjEy
MDAwMDAwWhcNMjkwMjExMjM1OTU5WjCBkDELMAkGA1UEBhMCR0IxGzAZBgNVBAgT
EkdyZWF0ZXIgTWFuY2hlc3RlcjEQMA4GA1UEBxMHU2FsZm9yZDEaMBgGA1UEChMR
Q09NT0RPIENBIExpbWl0ZWQxNjA0BgNVBAMTLUNPTU9ETyBSU0EgRG9tYWluIFZh
bGlkYXRpb24gU2VjdXJlIFNlcnZlciBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEP
ADCCAQoCggEBAI7CAhnhoFmk6zg1jSz9AdDTScBkxwtiBUUWOqigwAwCfx3M28Sh
bXcDow+G+eMGnD4LgYqbSRutA776S9uMIO3Vzl5ljj4Nr0zCsLdFXlIvNN5IJGS0
Qa4Al/e+Z96e0HqnU4A7fK31llVvl0cKfIWLIpeNs4TgllfQcBhglo/uLQeTnaG6
ytHNe+nEKpooIZFNb5JPJaXyejXdJtxGpdCsWTWM/06RQ1A/WZMebFEh7lgUq/51
UHg+TLAchhP6a5i84DuUHoVS3AOTJBhuyydRReZw3iVDpA3hSqXttn7IzW3uLh0n
c13cRTCAquOyQQuvvUSH2rnlG51/ruWFgqUCAwEAAaOCAWUwggFhMB8GA1UdIwQY
MBaAFLuvfgI9+qbxPISOre44mOzZMjLUMB0GA1UdDgQWBBSQr2o6lFoL2JDqElZz
30O0Oija5zAOBgNVHQ8BAf8EBAMCAYYwEgYDVR0TAQH/BAgwBgEB/wIBADAdBgNV
HSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwGwYDVR0gBBQwEjAGBgRVHSAAMAgG
BmeBDAECATBMBgNVHR8ERTBDMEGgP6A9hjtodHRwOi8vY3JsLmNvbW9kb2NhLmNv
bS9DT01PRE9SU0FDZXJ0aWZpY2F0aW9uQXV0aG9yaXR5LmNybDBxBggrBgEFBQcB
AQRlMGMwOwYIKwYBBQUHMAKGL2h0dHA6Ly9jcnQuY29tb2RvY2EuY29tL0NPTU9E
T1JTQUFkZFRydXN0Q0EuY3J0MCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5jb21v
ZG9jYS5jb20wDQYJKoZIhvcNAQEMBQADggIBAE4rdk+SHGI2ibp3wScF9BzWRJ2p
mj6q1WZmAT7qSeaiNbz69t2Vjpk1mA42GHWx3d1Qcnyu3HeIzg/3kCDKo2cuH1Z/
e+FE6kKVxF0NAVBGFfKBiVlsit2M8RKhjTpCipj4SzR7JzsItG8kO3KdY3RYPBps
P0/HEZrIqPW1N+8QRcZs2eBelSaz662jue5/DJpmNXMyYE7l3YphLG5SEXdoltMY
dVEVABt0iN3hxzgEQyjpFv3ZBdRdRydg1vs4O2xyopT4Qhrf7W8GjEXCBgCq5Ojc
2bXhc3js9iPc0d1sjhqPpepUfJa3w/5Vjo1JXvxku88+vZbrac2/4EjxYoIQ5QxG
V/Iz2tDIY+3GH5QFlkoakdH368+PUq4NCNk+qKBR6cGHdNXJ93SrLlP7u3r7l+L4
HyaPs9Kg4DdbKDsx5Q5XLVq4rXmsXiBmGqW5prU5wfWYQ//u+aen/e7KJD2AFsQX
j4rBYKEMrltDR5FL1ZoXX/nUh8HCjLfn4g8wGTeGrODcQgPmlKidrv0PJFGUzpII
0fxQ8ANAe4hZ7Q7drNJ3gjTcBpUC2JD5Leo31Rpg0Gcg19hCC0Wvgmje3WYkN5Ap
lBlGGSW4gNfL1IYoakRwJiNiqZ+Gb7+6kHDSVneFeO/qJakXzlByjAA6quPbYzSf
+AZxAeKCINT+b72x
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIFdDCCBFygAwIBAgIQJ2buVutJ846r13Ci/ITeIjANBgkqhkiG9w0BAQwFADBv
MQswCQYDVQQGEwJTRTEUMBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFk
ZFRydXN0IEV4dGVybmFsIFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBF
eHRlcm5hbCBDQSBSb290MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFow
gYUxCzAJBgNVBAYTAkdCMRswGQYDVQQIExJHcmVhdGVyIE1hbmNoZXN0ZXIxEDAO
BgNVBAcTB1NhbGZvcmQxGjAYBgNVBAoTEUNPTU9ETyBDQSBMaW1pdGVkMSswKQYD
VQQDEyJDT01PRE8gUlNBIENlcnRpZmljYXRpb24gQXV0aG9yaXR5MIICIjANBgkq
hkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAkehUktIKVrGsDSTdxc9EZ3SZKzejfSNw
AHG8U9/E+ioSj0t/EFa9n3Byt2F/yUsPF6c947AEYe7/EZfH9IY+Cvo+XPmT5jR6
2RRr55yzhaCCenavcZDX7P0N+pxs+t+wgvQUfvm+xKYvT3+Zf7X8Z0NyvQwA1onr
ayzT7Y+YHBSrfuXjbvzYqOSSJNpDa2K4Vf3qwbxstovzDo2a5JtsaZn4eEgwRdWt
4Q08RWD8MpZRJ7xnw8outmvqRsfHIKCxH2XeSAi6pE6p8oNGN4Tr6MyBSENnTnIq
m1y9TBsoilwie7SrmNnu4FGDwwlGTm0+mfqVF9p8M1dBPI1R7Qu2XK8sYxrfV8g/
vOldxJuvRZnio1oktLqpVj3Pb6r/SVi+8Kj/9Lit6Tf7urj0Czr56ENCHonYhMsT
8dm74YlguIwoVqwUHZwK53Hrzw7dPamWoUi9PPevtQ0iTMARgexWO/bTouJbt7IE
IlKVgJNp6I5MZfGRAy1wdALqi2cVKWlSArvX31BqVUa/oKMoYX9w0MOiqiwhqkfO
KJwGRXa/ghgntNWutMtQ5mv0TIZxMOmm3xaG4Nj/QN370EKIf6MzOi5cHkERgWPO
GHFrK+ymircxXDpqR+DDeVnWIBqv8mqYqnK8V0rSS527EPywTEHl7R09XiidnMy/
s1Hap0flhFMCAwEAAaOB9DCB8TAfBgNVHSMEGDAWgBStvZh6NLQm9/rEJlTvA73g
JMtUGjAdBgNVHQ4EFgQUu69+Aj36pvE8hI6t7jiY7NkyMtQwDgYDVR0PAQH/BAQD
AgGGMA8GA1UdEwEB/wQFMAMBAf8wEQYDVR0gBAowCDAGBgRVHSAAMEQGA1UdHwQ9
MDswOaA3oDWGM2h0dHA6Ly9jcmwudXNlcnRydXN0LmNvbS9BZGRUcnVzdEV4dGVy
bmFsQ0FSb290LmNybDA1BggrBgEFBQcBAQQpMCcwJQYIKwYBBQUHMAGGGWh0dHA6
Ly9vY3NwLnVzZXJ0cnVzdC5jb20wDQYJKoZIhvcNAQEMBQADggEBAGS/g/FfmoXQ
zbihKVcN6Fr30ek+8nYEbvFScLsePP9NDXRqzIGCJdPDoCpdTPW6i6FtxFQJdcfj
Jw5dhHk3QBN39bSsHNA7qxcS1u80GH4r6XnTq1dFDK8o+tDb5VCViLvfhVdpfZLY
Uspzgb8c8+a4bmYRBbMelC1/kZWSWfFMzqORcUx8Rww7Cxn2obFshj5cqsQugsv5
B5a6SE2Q8pTIqXOi6wZ7I53eovNNVZ96YUWYGGjHXkBrI/V5eu+MtWuLt29G9Hvx
PUsE2JOAWVrgQSQdso8VYFhH2+9uRv0V9dlfmrPb2LjkQLPNlzmuhbsdjrzch5vR
pu/xO28QOG8=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIENjCCAx6gAwIBAgIBATANBgkqhkiG9w0BAQUFADBvMQswCQYDVQQGEwJTRTEU
MBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFkZFRydXN0IEV4dGVybmFs
IFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBFeHRlcm5hbCBDQSBSb290
MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFowbzELMAkGA1UEBhMCU0Ux
FDASBgNVBAoTC0FkZFRydXN0IEFCMSYwJAYDVQQLEx1BZGRUcnVzdCBFeHRlcm5h
bCBUVFAgTmV0d29yazEiMCAGA1UEAxMZQWRkVHJ1c3QgRXh0ZXJuYWwgQ0EgUm9v
dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALf3GjPm8gAELTngTlvt
H7xsD821+iO2zt6bETOXpClMfZOfvUq8k+0DGuOPz+VtUFrWlymUWoCwSXrbLpX9
uMq/NzgtHj6RQa1wVsfwTz/oMp50ysiQVOnGXw94nZpAPA6sYapeFI+eh6FqUNzX
mk6vBbOmcZSccbNQYArHE504B4YCqOmoaSYYkKtMsE8jqzpPhNjfzp/haW+710LX
a0Tkx63ubUFfclpxCDezeWWkWaCUN/cALw3CknLa0Dhy2xSoRcRdKn23tNbE7qzN
E0S3ySvdQwAl+mG5aWpYIxG3pzOPVnVZ9c0p10a3CitlttNCbxWyuHv77+ldU9U0
WicCAwEAAaOB3DCB2TAdBgNVHQ4EFgQUrb2YejS0Jvf6xCZU7wO94CTLVBowCwYD
VR0PBAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wgZkGA1UdIwSBkTCBjoAUrb2YejS0
Jvf6xCZU7wO94CTLVBqhc6RxMG8xCzAJBgNVBAYTAlNFMRQwEgYDVQQKEwtBZGRU
cnVzdCBBQjEmMCQGA1UECxMdQWRkVHJ1c3QgRXh0ZXJuYWwgVFRQIE5ldHdvcmsx
IjAgBgNVBAMTGUFkZFRydXN0IEV4dGVybmFsIENBIFJvb3SCAQEwDQYJKoZIhvcN
AQEFBQADggEBALCb4IUlwtYj4g+WBpKdQZic2YR5gdkeWxQHIzZlj7DYd7usQWxH
YINRsPkyPef89iYTx4AWpb9a/IfPeHmJIZriTAcKhjW88t5RxNKWt9x+Tu5w/Rw5
6wwCURQtjr0W4MHfRnXnJK3s9EK0hZNwEGe6nQY1ShjTK3rMUUKhemPR5ruhxSvC
Nr4TDea9Y355e6cJDUCrat2PisP29owaQgVR1EX1n6diIWgVIEM8med8vSTYqZEX
c4g/VhsxOBi0cQ+azcgOno4uG+GMmIPLHzHxREzGBHNJdmAPx/i9F4BrLunMTA5a
mnkPIAou1Z5jJh5VkpTYghdae9C8x49OhgQ=
-----END CERTIFICATE-----
EOF
			fi
		fi
		if grep -q '^sslcertificatefile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}"; then
			CERT_NAME=$(grep -m1 '^sslcertificatefile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}" | grep -o '[^/]*\.crt')
			if [ -s "${CP_ROOT}/sslcerts/${CERT_NAME}" ]; then
				echo "Copying ${CERT_NAME} SSL certificate..."
				mv "${CP_ROOT}/sslcerts/${CERT_NAME}" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cert"
			fi
		fi
		if grep -q '^sslcacertificatefile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}"; then
			KEY_NAME=$(grep -m1 '^sslcertificatekeyfile: ' "${CONVERTED_DOMAIN_SSL_CONFIG}" | grep -o '[^/]*\.key')
			if [ -s "${CP_ROOT}/sslkeys/${KEY_NAME}" ]; then
				echo "Copying ${CERT_NAME} SSL private key..."
				mv "${CP_ROOT}/sslkeys/${KEY_NAME}" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.key"
			fi
		fi
	fi
	#We get it from apache_tls
	if [ -s "${APACHE_TLS_PATH}" ]; then
		openssl x509 -in "${APACHE_TLS_PATH}" -out "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cert" 2>&1
		openssl rsa -in "${APACHE_TLS_PATH}" -out "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.key" 2>&1
		awk '/----BEGIN CERTIFICATE----/ { flag = 1; ++ctr } flag && ctr >= 2 { print } /-----END CERTIFICATE-----/ { flag = 0 }' "${APACHE_TLS_PATH}" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert"
		if [ ! -s "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert" ]; then
			rm -f "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert"
		fi
	fi
	

        #Move AWstats to the correct folder
	if [ -d "${CP_ROOT}/homedir/tmp/awstats" ]; then
	        AWStats_DataDir="${DA_ROOT}/domains/${CONVERTED_DOMAIN}/awstats/.data"
		mkdir -p "${AWStats_DataDir}"
	        echo "AWSTATS: moving awstats data"
		for FN in $(find "${CP_ROOT}/homedir/tmp/awstats/" -maxdepth 1 -name "awstats*.${CONVERTED_DOMAIN}.txt" -printf '%P\n'); do
			http_fl="${CP_ROOT}/homedir/tmp/awstats/${FN}"
	                https_fl="${CP_ROOT}/homedir/tmp/awstats/ssl/${FN}"
			if [ -e "${http_fl}" ]; then
				http_size=$(wc -c "${http_fl}" | cut -d' ' -f 1)
			else
				http_size=0
			fi
			if [ -e "${https_fl}" ]; then
				https_size=$(wc -c "${https_fl}" | cut -d' ' -f 1)
			else
				https_size=0
			fi
#	                echo "AWSTATS: ${FN}: ${http_size}, ${https_size}"
	                if [ ${http_size} -gt ${https_size} ]; then
	                    	cp -f "${http_fl}" "${AWStats_DataDir}"
        	        else
                	    	cp -f "${https_fl}" "${AWStats_DataDir}"
	                fi
        	done
	fi

	#Local or remote delivery
	CPANEL_MAILDELIVERY=$(getOpt "MXCHECK-${CONVERTED_DOMAIN}")
	DIRECTADMIN_DOMAIN_LOCALDELIVERY=1
	if echo "${CPANEL_MAILDELIVERY}" | grep -q "remote"; then
		DIRECTADMIN_DOMAIN_LOCALDELIVERY=0
	fi
	
	#Create domain.usage
	{
		echo "bandwidth=0"
		echo "log_quota=0"
		echo "quota=0"
	} > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.usage"

	#Create domain.ip_list
	echo "${DEFAULT_IP}" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.ip_list"
	if [ -s "${CP_ROOT}/ips/related_ips" ]; then
		perl -pi -e 's|0000|0|g' "${CP_ROOT}/ips/related_ips"
		cat "${CP_ROOT}/ips/related_ips" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.ip_list"
		echo '' >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.ip_list"
	fi
	
	#Create domain.conf
	{
		echo "UseCanonicalName=OFF"
		echo "active=yes"
		echo "bandwidth=unlimited"
		echo "cgi=${CPANEL_HASCGI}"
		echo "domain=${CONVERTED_DOMAIN}"
		echo "ip=${DEFAULT_IP}"
		echo "local_domain=${DIRECTADMIN_DOMAIN_LOCALDELIVERY}"
		echo "php=ON"
		echo "private_html_is_link=1"
		echo "quota=unlimited"
		echo "safemode=OFF"
		echo "ssl=ON"
		echo "suspended=${ACCOUNT_SUSPENDED}"
		echo "username=${USERNAME}"

		# We already have copied cert files to DA_ROOT
		if [ -f "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cacert" ]; then
			echo "SSLCACertificateFile=/usr/local/directadmin/data/users/${USERNAME}/domains/${CONVERTED_DOMAIN}.cacert"
		fi
		if [ -f "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cert" ]; then
			echo "SSLCertificateFile=/usr/local/directadmin/data/users/${USERNAME}/domains/${CONVERTED_DOMAIN}.cert"
		fi
		if [ -f "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.key" ]; then
			echo "SSLCertificateKeyFile=/usr/local/directadmin/data/users/${USERNAME}/domains/${CONVERTED_DOMAIN}.key"
		fi
		if ${DEFAULT_DOMAIN}; then
			echo "defaultdomain=yes"
		else
			echo "defaultdomain=no"
		fi
	} > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.conf"
	if [ -s "${CONVERTED_DOMAIN_CONFIG}" ]; then
		if grep -q '^phpversion:' "${CONVERTED_DOMAIN_CONFIG}" && [ -s /usr/local/directadmin/custombuild/options.conf ]; then
			DOMAIN_PHP_VERSION=$(grep -m1 '^phpversion:' "${CONVERTED_DOMAIN_CONFIG}" | awk '{print $2}' | grep -o '[0-9]*')
			#Match PHP version to the one used on the system
			if grep 'php._release' /usr/local/directadmin/custombuild/options.conf | tr -d '.' | grep -q "=${DOMAIN_PHP_VERSION}$"; then
				DOMAIN_PHP_VERSION_IN_OPTIONS=$(grep 'php._release' /usr/local/directadmin/custombuild/options.conf | tr -d '.' | grep -m1 "=${DOMAIN_PHP_VERSION}$" | cut -d_ -f1 | grep -o '[0-9]*')
				echo "Assigning domain PHP${DOMAIN_PHP_VERSION} (php${DOMAIN_PHP_VERSION_IN_OPTIONS}_release)..."
				echo "php1_select=${DOMAIN_PHP_VERSION_IN_OPTIONS}" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.conf"
			fi
		fi
	fi

	#Create ftp.conf
	{
		echo "Anonymous=no"
		echo "AnonymousUpload=no"
		echo "AuthUserFile=/usr/local/directadmin/data/users/${USERNAME}/ftp.passwd"
		echo "DefaultRoot=/home/${USERNAME}/domains/${CONVERTED_DOMAIN}/public_ftp"
		echo "ExtendedLog=/var/log/proftpd/${DEFAULT_IP}.bytes"
		echo "MaxClients=10"
		echo "MaxLoginAttempts=3"
		echo "ServerAdmin=${CUSTOMER_EMAIL}"
		echo "ServerName=ProFTPd"
		if ${DEFAULT_DOMAIN}; then
			echo "defaultdomain=yes"
		else
			echo "defaultdomain=no"
		fi
		echo "ip=${DEFAULT_IP}"
	} > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/ftp.conf"
	
	#Create ftp.passwd
	CONVERTED_DOMAIN_FTP_PASSWD="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/ftp.passwd"
	: > "${CONVERTED_DOMAIN_FTP_PASSWD}"
	CPANEL_FTP_PASSWD=${CP_ROOT}/proftpdpasswd
	local domain_config="${CP_ROOT}/userdata/${CONVERTED_DOMAIN}"
	if ! ${DEFAULT_DOMAIN}; then
		domain_config="${CP_ROOT}/userdata/${ASSOCIATED_SUBDOMAIN}"
	fi
	local homedir documentroot
	homedir=$(awk '/^homedir:/ {print $2}' "${domain_config}")
	documentroot=$(awk '/^documentroot:/ {print $2}' "${domain_config}")
	if [ -s "${CPANEL_FTP_PASSWD}" ] && [ -z "${ALIAS_DOMAIN}" ]; then
		while read -r i; do
			FTP_USER=$(echo "${i}" | cut -d: -f1)
			FTP_PASS=$(echo "${i}" | cut -d: -f2)
			#FTP_UID=$(echo "${i}" | cut -d: -f3)
			#FTP_GID=$(echo "${i}" | cut -d: -f4)
			FTP_DIR=$(echo "${i}" | cut -d: -f6)

			# Exclude custom paths having /etc prefix
			if [ "${FTP_DIR#/etc}" != "${FTP_DIR}" ]; then
				continue
			fi
			if [ "${FTP_PASS}" = "" ]; then
				continue
			fi
		
			if [ "${FTP_USER}" = "${USERNAME}" ]; then
				echo "${FTP_USER}=passwd=${FTP_PASS}&path=${FTP_DIR}&type=system" >> "${CONVERTED_DOMAIN_FTP_PASSWD}"
			else
				if echo "${FTP_USER}" | grep -q '\@'; then
					FTP_USER_AT_DOMAIN=${FTP_USER}
				else
					FTP_USER_AT_DOMAIN="${FTP_USER}@${CONVERTED_DOMAIN}"
				fi
				if [ "${FTP_DIR}" = "${documentroot}" ]; then
					echo "${FTP_USER_AT_DOMAIN}=passwd=${FTP_PASS}&path=${FTP_DIR}&type=domain" >> "${CONVERTED_DOMAIN_FTP_PASSWD}"
				elif [ "${FTP_DIR:0:${#homedir}}" = "${homedir}" ]; then
					echo "${FTP_USER_AT_DOMAIN}=passwd=${FTP_PASS}&path=${FTP_DIR}&type=custom" >> "${CONVERTED_DOMAIN_FTP_PASSWD}"
				else
					EXIT_CODE=2
					>&2 echo "WARNING! FTP account ${FTP_USER_AT_DOMAIN} is ignored because it is configured to access files at ${FTP_DIR} which is outside of user home dir ${homedir}"
				fi
			fi
		done < "${CPANEL_FTP_PASSWD}"
	fi
	
	#Create subdomains.list and make sure subdomain path is correct
	CONVERTED_DOMAIN_SUBDOMAINS="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/subdomain.list"
	CONVERTED_DOMAIN_SUBDOMAINS_SDOCROOTS="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.subdomains.docroot.override"
	: > "${CONVERTED_DOMAIN_SUBDOMAINS}"
	for i in $(grep "${CONVERTED_DOMAIN}" "${CP_ROOT}/sds"); do
		if grep -q "=${i}$" "${CP_ROOT}/addons"; then
			echo "Skipping ${i} as subdomain, because it's an add-on domain..."
			continue
		fi
		SKIP_SUBDOMAIN=false
		SUBDOMAIN_PART=$(echo "${i}" | cut -d= -f2 | cut -d_ -f1)
		DOMAIN_PART=$(echo "${i}" | cut -d_ -f2)
		SUBDOCROOT=$(grep "^${SUBDOMAIN_PART}_${DOMAIN_PART}=" "${CP_ROOT}/sds2" | cut -d= -f2)
		# Some hosts have things like 2 subdomains pointed to the same docroot, and alias domain for them, make sure it's not the case
		for sds2 in $(grep "=${SUBDOCROOT}$" "${CP_ROOT}/sds2" | cut -d= -f1 | grep -v "^${i}$"); do
			if grep -q "=${sds2}$" "${CP_ROOT}/addons"; then
				SKIP_SUBDOMAIN=true
			fi
		done
		if ${SKIP_SUBDOMAIN}; then
			echo "Skipping ${i} as subdomain, because it's an add-on domain..."
			continue
		fi
		SUBDOMAIN_PATH=""
		if ! echo "${SUBDOCROOT}" | grep -q "public_html/${SUBDOMAIN_PART}$" && [ -d "${CP_ROOT}/homedir/${SUBDOCROOT}" ]; then
			>&2 echo "WARNING! ${SUBDOMAIN_PART}.${DOMAIN_PART} path was set to custom in cPanel: ${SUBDOCROOT}"
			if [ "${SUBDOCROOT}" = "public_html" ]; then
				NEWPATH="${SUBDOCROOT}"
				echo "${SUBDOMAIN_PART}=public_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}&private_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}" >> "${CONVERTED_DOMAIN_SUBDOMAINS_SDOCROOTS}"
				SUBDOMAIN_PATH="${CP_ROOT}/homedir/${NEWPATH}"
				# Sometimes people have custom chmods, preventing subdomains from working
				chmod 755 "${SUBDOMAIN_PATH}"
			elif echo "${SUBDOCROOT}" | grep -q "public_html/"; then
				NEWPATH=$(echo "${SUBDOCROOT}" | grep -o 'public_html/.*')
				echo "${SUBDOMAIN_PART}=public_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}&private_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}" >> "${CONVERTED_DOMAIN_SUBDOMAINS_SDOCROOTS}"
				SUBDOMAIN_PATH="${CP_ROOT}/homedir/${NEWPATH}"
				# Sometimes people have custom chmods, preventing subdomains from working
				chmod 755 "${SUBDOMAIN_PATH}"
			elif ! echo "${SUBDOCROOT}" | grep -q "/"; then
				NEWPATH=${SUBDOCROOT}
				echo "${SUBDOMAIN_PART}=public_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}&private_html=/domains/${CONVERTED_DOMAIN}/${NEWPATH}" >> "${CONVERTED_DOMAIN_SUBDOMAINS_SDOCROOTS}"
				SUBDOMAIN_PATH="${CP_ROOT}/${CONVERTED_DOMAIN}_domainsdir/${NEWPATH}"
				#we must move the domain to domains/${CONVERTED_DOMAIN} if it's outside public_html
				mkdir -p "${CP_ROOT}/${CONVERTED_DOMAIN}_domainsdir/"
				echo "Renaming ${SUBDOCROOT} to domains/${CONVERTED_DOMAIN}/${SUBDOCROOT}"
				mv "${CP_ROOT}/homedir/${SUBDOCROOT}" "${SUBDOMAIN_PATH}"
				# Sometimes people have custom chmods, preventing subdomains from working
				chmod 755 "${SUBDOMAIN_PATH}"
			else
				if ${DEFAULT_DOMAIN}; then
					if [ ! -d "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" ]; then
						echo "Renaming ${SUBDOCROOT} to public_html/${SUBDOMAIN_PART}"
						mv "${CP_ROOT}/homedir/${SUBDOCROOT}" "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}"
						# Sometimes people have custom chmods, preventing subdomains from working
						chmod 755 "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}"
						SUBDOMAIN_PATH="${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}"
						# Sometimes people have custom chmods, preventing subdomains from working
						chmod 755 "${SUBDOMAIN_PATH}"
					else
						>&2 echo "WARNING! Not moving custom ${SUBDOMAIN_PART}.${DOMAIN_PART} path ${SUBDOCROOT} to DirectAdmin location, because public_html/${SUBDOMAIN_PART} folder also exists in public_html."
					fi
				else
					if [ ! -d "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}" ] && [ -d "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}" ]; then
						echo "Renaming ${SUBDOCROOT} to public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
						mv "${CP_ROOT}/homedir/${SUBDOCROOT}" "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
						# Sometimes people have custom chmods, preventing subdomains from working
						chmod 755 "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
						SUBDOMAIN_PATH="${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
					else
						>&2 echo "WARNING! Not moving custom ${SUBDOMAIN_PART}.${DOMAIN_PART} path ${SUBDOCROOT} to DirectAdmin location, because public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART} folder also exists in public_html."
					fi
				fi
			fi
		elif ! ${DEFAULT_DOMAIN}; then
			if [ ! -d "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}" ] && [ -d "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}" ] && [ -d "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" ]; then
				echo "Renaming public_html/${SUBDOMAIN_PART} to public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
				mv "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
				SUBDOMAIN_PATH="${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
				# Sometimes people have custom chmods, preventing subdomains from working
				chmod 755 "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
			fi
		fi
		
		if [ ! -e "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" ]; then
			if ! echo "${SUBDOCROOT}" | grep -q "public_html/${SUBDOMAIN_PART}$"; then
				FIRST_DOMAIN_SUBDOCROOT=$(grep -m1 "=${SUBDOCROOT}$" "${CP_ROOT}/sds2" | cut -d_ -f1)
				if [ -d "${CP_ROOT}/homedir/public_html/${FIRST_DOMAIN_SUBDOCROOT}" ] && [ ! -e "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" ]; then
					echo "Found ${CP_ROOT}/homedir/public_html/${FIRST_DOMAIN_SUBDOCROOT} as the target of subdomain, linking...."
					ln -sr "${CP_ROOT}/homedir/public_html/${FIRST_DOMAIN_SUBDOCROOT}" "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}"
				fi
			fi
		fi
	
		#Auto-find and comment out any ea PHP handlers in .htaccess files
		clean_htaccess_cp_php_handlers "${SUBDOMAIN_PATH}"
		
		if [ "${DOMAIN_PART}" = "${CONVERTED_DOMAIN}" ]; then
			echo "Converting subdomain ${SUBDOMAIN_PART}.${DOMAIN_PART}..."
			echo "${SUBDOMAIN_PART}" >> "${CONVERTED_DOMAIN_SUBDOMAINS}"
			if [ -d "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}" ]; then
				chmod 755 "${CP_ROOT}/homedir/public_html/${CONVERTED_DOMAIN}/${SUBDOMAIN_PART}"
			fi
                        if [ -d "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}" ]; then
                                chmod 755 "${CP_ROOT}/homedir/public_html/${SUBDOMAIN_PART}"
                        fi
		fi
	done
	
	#Create DNS zone
	CONVERTED_DOMAIN_DNS="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/${CONVERTED_DOMAIN}.db" 
	CPANEL_DNS_DATA=${CP_ROOT}/dnszones/${CONVERTED_DOMAIN}.db
	if [ -s "${CPANEL_DNS_DATA}" ]; then
		NS1=$(awk 'BEGIN {N=0} /[[:space:]]IN[[:space:]]+NS[[:space:]]/ {N=N+1; gsub(/\.$/, "", $5); if (N == 1) {print $5}}' "${CPANEL_DNS_DATA}")
		NS2=$(awk 'BEGIN {N=0} /[[:space:]]IN[[:space:]]+NS[[:space:]]/ {N=N+1; gsub(/\.$/, "", $5); if (N == 2) {print $5}}' "${CPANEL_DNS_DATA}")
		if [ -z "${NS1}" ]; then
			NS1="ns1.da.direct"
		fi
		if [ -z "${NS2}" ]; then
			NS2="ns2.da.direct"
		fi
		cp -f "${CPANEL_DNS_DATA}" "${CONVERTED_DOMAIN_DNS}"
		perl -pi -e "s|${CONVERTED_DOMAIN}.*IN.SOA|@\tIN\tSOA|g" "${CONVERTED_DOMAIN_DNS}"
		# Change DKIM records to have quoted strings
		#for i in `grep DKIM1 ${CONVERTED_DOMAIN_DNS} | grep -o '" [a-zA-Z0-9+]*$' | awk '{print $2}'`; do { REPLACE=`echo "${i}" | perl -p0 -e 's|\+|\\\\+|g' | perl -p0 -e 's|\/|\\\\/|g'`; perl -pi -e "s|${REPLACE}|\"${i}\"|g" ${CONVERTED_DOMAIN_DNS}; }; done
		sed -i '/^x\._domainkey/d' "${CONVERTED_DOMAIN_DNS}"
		sed -i '/^default\._domainkey/d' "${CONVERTED_DOMAIN_DNS}"
		
		perl -pi -e 's|^;.*||g' "${CONVERTED_DOMAIN_DNS}"
		if ! grep -q ^smtp "${CONVERTED_DOMAIN_DNS}"; then
			echo "smtp    3600   IN      A       ${DEFAULT_IP}" >> "${CONVERTED_DOMAIN_DNS}"
		fi
		if ! grep -q ^pop "${CONVERTED_DOMAIN_DNS}"; then
			echo "pop    3600   IN      A       ${DEFAULT_IP}" >> "${CONVERTED_DOMAIN_DNS}"
		fi
	else
		NS1="ns1.da.direct"
		NS2="ns2.da.direct"
		cat << EOF > "${CONVERTED_DOMAIN_DNS}"
\$TTL 3600
@       IN      SOA     ${NS1}.      hostmaster.${CONVERTED_DOMAIN}. (
												2003120200
												3600
												3600
												1209600
												86400 )

${CONVERTED_DOMAIN}.     3600   IN      NS      ${NS1}.
${CONVERTED_DOMAIN}.     3600   IN      NS      ${NS2}.

ftp     3600   IN      A       ${DEFAULT_IP}
${CONVERTED_DOMAIN}.     3600   IN      A       ${DEFAULT_IP}
mail    3600   IN      A       ${DEFAULT_IP}
pop    3600   IN      A       ${DEFAULT_IP}
smtp    3600   IN      A       ${DEFAULT_IP}
www     3600   IN      A       ${DEFAULT_IP}
EOF
	
		#Subdomain DNS records
		for i in $(grep "${CONVERTED_DOMAIN}" "${CP_ROOT}/sds"); do {
			SUBDOMAIN_PART=$(echo "${i}" | cut -d= -f2 | cut -d_ -f1)
			#Create temporary DNS records list
			echo "${SUBDOMAIN_PART}     3600   IN      A       ${DEFAULT_IP}" >> "${CONVERTED_DOMAIN_DNS}"
		}
		done
	
		# Copy MX records
		if [ -s "${CPANEL_DNS_DATA}" ]; then
			grep 'IN.*MX' "${CPANEL_DNS_DATA}" >> "${CONVERTED_DOMAIN_DNS}"
		fi
	fi
	
	#Create empty email data directory
	mkdir -p "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data"
	
	#Move data for non-default domains
	if ! ${DEFAULT_DOMAIN}; then
		if [ -z "${ALIAS_DOMAIN}" ]; then
			echo "Moving add-on domains data"
			PATH_TO_FILES=$(grep '^documentroot: ' "${CP_ROOT}/userdata/${ASSOCIATED_SUBDOMAIN}" | awk '{print $2}' | perl -p0 -e "s|^/.*/${USERNAME}/||g")
			if [ "${PATH_TO_FILES}" != "public_html" ]; then
				if [ -d "${CP_ROOT}/homedir/${PATH_TO_FILES}" ]; then
					mv "${CP_ROOT}/homedir/${PATH_TO_FILES}" "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/public_html"
				else
					mkdir -p "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/public_html"
				fi
				#Move out-of public_html subdomains
				if [ -d "${CP_ROOT}/${CONVERTED_DOMAIN}_domainsdir" ]; then
					mv "${CP_ROOT}/${CONVERTED_DOMAIN}_domainsdir/"* "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/" 2>/dev/null
				fi
				if [ ! -e "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/private_html" ]; then
					ln -sf ./public_html "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/private_html"
				fi
				DOMAIN_PATH="${DA_ROOT}/domains/${CONVERTED_DOMAIN}/public_html"
			else
				for cust_file in ${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_httpd ${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_nginx ${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_openlitespeed; do {
					echo "|*if !SUB|" > "${cust_file}"
					echo "|?DOCROOT=\`HOME\`/${PATH_TO_FILES}|" >> "${cust_file}"
					echo "|*endif|" >> "${cust_file}"
				};
				done
				DOMAIN_PATH="${DA_ROOT}/${PATH_TO_FILES}"
			fi
			#Move password protected directories
			if [ -d "${CP_ROOT}/homedir/.htpasswds/${PATH_TO_FILES}" ]; then
				if [ ! -d "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd" ]; then
					mkdir -p "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd"
					chmod 711 "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd"
				fi
				mv "${CP_ROOT}/homedir/.htpasswds/${PATH_TO_FILES}" "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/public_html"
				find "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/" -name 'passwd' -execdir mv {} .htpasswd \;
				find "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/" -name '.htpasswd' -type f -printf "/domains/${CONVERTED_DOMAIN}/%P\n" > "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/.protected.list"
				perl -pi -e 's|/\.htpasswd||g' "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/.protected.list"
				while read -r LINE; do {
					FULLPATH="${DA_ROOT}/${LINE}"
					if [ -s "${FULLPATH}/.htaccess" ]; then
						if grep -q '^AuthUserFile ' "${FULLPATH}/.htaccess"; then
							perl -pi -e "s|^AuthUserFile \"/home/${USERNAME}/\.htpasswds|AuthUserFile \"/home/${USERNAME}/domains/${CONVERTED_DOMAIN}/.htpasswd|g" "${FULLPATH}/.htaccess"
							perl -pi -e 's|/passwd"|/.htpasswd"|g' "${FULLPATH}/.htaccess"
						fi
					fi
				};
				done < "${DA_ROOT}/domains/${CONVERTED_DOMAIN}/.htpasswd/.protected.list"
			fi
			#Auto-find and comment out any ea PHP handlers in .htaccess files
			clean_htaccess_cp_php_handlers "${DOMAIN_PATH}"
		fi
	fi
	
	if [ -n "${ALIAS_DOMAIN}" ]; then
		echo "|?DOCROOT=\`HOME\`/domains/${ALIAS_DOMAIN}/public_html|" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_httpd"
		echo "|?DOCROOT=\`HOME\`/domains/${ALIAS_DOMAIN}/public_html|" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_nginx"
		echo "|?DOCROOT=\`HOME\`/domains/${ALIAS_DOMAIN}/public_html|" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/domain.cust_openlitespeed"
	fi

	#Create dependencies
	touch "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/autoresponder.conf"
	CATCHALL_VALUE=$(grep -m1 '^*:' "${CP_ROOT}/va/${CONVERTED_DOMAIN}" | cut -d: -f2,3,4 | cut -d' ' -f2)
	echo "catchall=${CATCHALL_VALUE}" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/email.conf"
	touch "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/vacation.conf"
	
	#Copy email aliases
	echo "${USERNAME}:${USERNAME}" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/aliases"
	if [ -s "${CP_ROOT}/va/${CONVERTED_DOMAIN}" ]; then
		echo "Copying email aliases..."
		grep -v '^*:' "${CP_ROOT}/va/${CONVERTED_DOMAIN}" | perl -p0 -e "s|\@${CONVERTED_DOMAIN}: |:|g" | perl -p0 -e 's|::|: :|g' | grep -v "^${USERNAME}:" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/aliases"
		for autoresponder in $(grep '/usr/local/cpanel/bin/autorespond' "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/aliases" | cut -d: -f1); do
			AUTORESPONDER_FILE=$(grep "^${autoresponder}:" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/aliases" | grep -o "/usr/local/cpanel/bin/autorespond [^ ]*" | head -n1 | awk '{print $2}')
			if [ -s "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}" ]; then
				CURRENT_TIMESTAMP=$(date +%s)
				BACKUP_AUTORESPONDER=false
				if [ -e "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}.json" ]; then
					START_TIMESTAMP=$(grep -o 'start":[0-9]*' "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}.json" | cut -d: -f2)
					if [ -z "${START_TIMESTAMP}" ]; then
						START_TIMESTAMP=${CURRENT_TIMESTAMP}
					fi
					STOP_TIMESTAMP=$(grep -o 'stop":[0-9]*' "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}.json" | cut -d: -f2)
					if [ -z "${STOP_TIMESTAMP}" ]; then
						BACKUP_AUTORESPONDER=true
					elif [ "${CURRENT_TIMESTAMP}" -lt "${STOP_TIMESTAMP}" ]; then
						BACKUP_AUTORESPONDER=true
					elif [ "${CURRENT_TIMESTAMP}" -gt "${STOP_TIMESTAMP}" ]; then
						BACKUP_AUTORESPONDER=false
					fi
				fi
				if ${BACKUP_AUTORESPONDER}; then
					echo "Creating ${autoresponder}@${CONVERTED_DOMAIN} autoresponder..."
					if [ ! -d "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply" ]; then
						mkdir -p "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply"
					fi
					awk -v 'RS=\n\n' '1;{exit}' "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers"
					perl -pi -e 's|utf-8|UTF-8|g' "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers"
					perl -pi -e 's|Content-type:|Content-Type:|g' "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers"
					if grep -q '^Subject: ' "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers"; then
						grep -m1 '^Subject: ' "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers" | perl -p0 -e 's|Subject: ||g' | perl -p0 -e 's|: %subject%$||g' | tr -cd 'a-zA-Z0-9 ' > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.subject"
						perl -ni -e "print unless /^Subject:/" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.headers"
					else
						echo 'Vacation' > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.subject"
					fi
					if [ -n "${STOP_TIMESTAMP}" ]; then
						START_YEAR=$(date -d "@${START_TIMESTAMP}" +%Y)
						STOP_YEAR=$(date -d "@${STOP_TIMESTAMP}" +%Y)
						START_MONTH=$(date -d "@${START_TIMESTAMP}" +%m)
						STOP_MONTH=$(date -d "@${STOP_TIMESTAMP}" +%m)
						START_DAY=$(date -d "@${START_TIMESTAMP}" +%d)
						STOP_DAY=$(date -d "@${STOP_TIMESTAMP}" +%d)
						echo "${autoresponder}: endday=${STOP_DAY}&endmonth=${STOP_MONTH}&endtime=evening&endyear=${STOP_YEAR}&startday=${START_DAY}&startmonth=${START_MONTH}&starttime=morning&startyear=${START_YEAR}" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/vacation.conf"
					else
						echo "${autoresponder}:" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/autoresponder.conf"
					fi
					printf '2d' > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.once_time"
					sed '1,/^$/d' "${CP_ROOT}/homedir/.autorespond/${AUTORESPONDER_FILE}" > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/reply/${autoresponder}.msg"

				fi
			fi
		done
		# first script removes autoresponder action, but keep additional alias actions (like forwards)
		# second script detects and completely removes autoresponder ONLY rules
		sed -i \
			-e 's#^\([^:]*:\s*\)"|/usr/local/cpanel/bin/autorespond[^"]*",\(.*\)$#\1\2#g' \
			-e '\#^\([^:]*:\s*\)"|/usr/local/cpanel/bin/autorespond[^"]*"\s*$#d' \
			"${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/aliases"
	fi
	
	#Convert RoundCube data
	if [ -s "${SCRIPT_DIR}/cpanel_da_roundcube.php" ] && [ -s "${CP_ROOT}/mysql/roundcube.sql" ]; then
		NO_OF_LINES=$(head -n2 "${CP_ROOT}/mysql/roundcube.sql" | wc -l)
		if [ "${NO_OF_LINES}" -gt 1 ]; then
			echo "Generating roundcube.xml..."
			if ${DEFAULT_DOMAIN}; then
				SCRIPT_ARG="is_maindomain=1"
			else
				SCRIPT_ARG="is_maindomain=0"
			fi
			php "${SCRIPT_DIR}/cpanel_da_roundcube.php" "${CP_ROOT}/mysql/roundcube.sql" "${CONVERTED_DOMAIN}" ${SCRIPT_ARG} "${DA_ROOT}/backup" 2>&1
		fi
	fi
	
	#Convert RoundCube data from sqlite
	if [ -s "${SCRIPT_DIR}/cpanel_sqlite_da_roundcube.php" ]; then
		if [ $(ls "${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}"/*.rcube.db.latest 2>/dev/null | wc -l) -gt 0 ]; then
			FILES_LIST="`readlink -f ${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/*.rcube.db.latest | grep -o "[^/]*$" | perl -p -e "s|^|${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/|g" | tr '\n' ' '`"
			php "${SCRIPT_DIR}/cpanel_sqlite_da_roundcube.php" --output="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/roundcube.xml" ${FILES_LIST} >/dev/null
		else
			php "${SCRIPT_DIR}/cpanel_sqlite_da_roundcube.php" --output="${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/roundcube.xml" --pattern=${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/*.rcube.db >/dev/null
		fi
	fi

	#Transfer emails
	if [ ! -s "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/passwd" ]; then
		: > "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/passwd"
	fi

	if [ -s "${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/shadow" ]; then
		MAILBOX_FORMAT=maildir
		if [ -s "${CP_ROOT}/homedir/mail/mailbox_format.cpanel" ]; then
			if grep -q mdbox "${CP_ROOT}/homedir/mail/mailbox_format.cpanel"; then
				MAILBOX_FORMAT=mdbox
			fi
		fi
		for i in $(cat "${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/shadow"); do
			EMAIL_USER=$(echo "${i}" | cut -d: -f1)
			EMAIL_PASSWORD=$(echo "${i}" | cut -d: -f2 | perl -p -e 's|\*LOCKED\*||g')
			if [ -z "${EMAIL_USER}" ]; then
				echo "EMAIL_USER is empty, skipping..."
				continue
			fi
			echo "${EMAIL_USER}:${EMAIL_PASSWORD}" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/passwd"
			if [ "${CPANEL_MAX_EMAILACCT_QUOTA}" != "unlimited" ] && [ "${CPANEL_MAX_EMAILACCT_QUOTA}" != "0" ] && [ -n "${CPANEL_MAX_EMAILACCT_QUOTA}" ]; then
				echo "${EMAIL_USER}:$((CPANEL_MAX_EMAILACCT_QUOTA * 1024 * 1024))" >> "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/quota"
			fi
			if [ -s "${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/${EMAIL_USER}/filter" ] && [ ! -s "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/filter" ]; then
				mv -f "${CP_ROOT}/homedir/etc/${CONVERTED_DOMAIN}/${EMAIL_USER}/filter" "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/filter"
			fi
			mkdir -p "${DA_ROOT}/imap/${CONVERTED_DOMAIN}/${EMAIL_USER}"
			echo "Moving email data of ${EMAIL_USER}@${CONVERTED_DOMAIN}"
			if [ -d "${CP_ROOT}/homedir/mail/${CONVERTED_DOMAIN}/${EMAIL_USER}" ]; then
				if [ "${EMAIL_USER}" = "${USERNAME}" ]; then
					>&2 echo "WARNING! ${EMAIL_USER}@${CONVERTED_DOMAIN} matches system account name, merging mailbox with system user's one."
					convert_mail_path "${CP_ROOT}/homedir/mail/${CONVERTED_DOMAIN}/${EMAIL_USER}" "${CP_ROOT}/homedir" "${MAILBOX_FORMAT}" || EXIT_CODE=2
				else
					convert_mail_path "${CP_ROOT}/homedir/mail/${CONVERTED_DOMAIN}/${EMAIL_USER}" "${DA_ROOT}/imap/${CONVERTED_DOMAIN}/${EMAIL_USER}" "${MAILBOX_FORMAT}" || EXIT_CODE=2
				fi
			else
				echo "Not moving ${EMAIL_USER}@${CONVERTED_DOMAIN}, as it was likely moved already to the parent (main) domain."
			fi
			if [ -s "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/imap/${EMAIL_USER}/Maildir/courierimapsubscribed" ]; then
				mv "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/imap/${EMAIL_USER}/Maildir/courierimapsubscribed" "${DA_ROOT}/imap/${CONVERTED_DOMAIN}/${EMAIL_USER}/.mailboxlist"
			fi
		done
		mkdir -p "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/imap"
		touch "${DA_ROOT}/backup/${CONVERTED_DOMAIN}/email/data/imap/.direct_imap_backup"
	else
		echo "No emails to transfer for ${CONVERTED_DOMAIN}."
	fi
	rm -rf "${CP_ROOT}/homedir/mail/${CONVERTED_DOMAIN}"
}

#Creating domain pointers
echo "Adding domain aliases..."
find "${CP_ROOT}/vad" -maxdepth 1 -mindepth 1 -printf '%f\n' | while read -r i; do
	if [ "${i}" = "${DEFAULT_DOMAIN_NAME}" ]; then
		continue
	fi
	if tr '_' '.' < "${CP_ROOT}/sds" | grep -q "^${i}$"; then
		continue
	fi
	if grep -q "^${i}=" "${CP_ROOT}/addons"; then
		continue
	fi
	FORWARDER_PARENT_DOMAIN=$(head -n1 "${CP_ROOT}/vad/${i}" | cut -d':' -f2 | awk '{print $1}')
	if [ "${FORWARDER_PARENT_DOMAIN}" = "" ]; then
		FORWARDER_PARENT_DOMAIN=${DEFAULT_DOMAIN_NAME}
	fi
	if [ -s "${CP_ROOT}/homedir/etc/${i}/shadow" ]; then
		>&2 echo "WARNING! Creating ${i} as a regular domain, not a domain alias of ${FORWARDER_PARENT_DOMAIN}, because it contains email accounts. Docroot will be set to one of ${FORWARDER_PARENT_DOMAIN} using Custom HTTPd Configuration."
		doConvertDomain "${i}" false '' "${FORWARDER_PARENT_DOMAIN}"
		continue
	fi
	mkdir -p "${DA_ROOT}/backup/${FORWARDER_PARENT_DOMAIN}"
	touch "${DA_ROOT}/backup/${FORWARDER_PARENT_DOMAIN}/domain.pointers"

	echo "${i}=alias" >> "${DA_ROOT}/backup/${FORWARDER_PARENT_DOMAIN}/domain.pointers"
	#Create DNS zone
	CPANEL_POINTER_DNS_DATA=${CP_ROOT}/dnszones/${i}.db
	CONVERTED_POINTER_DNS="${DA_ROOT}/backup/${FORWARDER_PARENT_DOMAIN}/${i}.db"
	if [ -s "${CPANEL_POINTER_DNS_DATA}" ]; then
		cp -f "${CPANEL_POINTER_DNS_DATA}" "${CONVERTED_POINTER_DNS}"
		perl -pi -e "s|${i}.*IN.SOA|@\tIN\tSOA|g" "${CONVERTED_POINTER_DNS}"
		#for i in `grep DKIM1 ${CONVERTED_POINTER_DNS} | grep -o '" [a-zA-Z0-9+]*$' | awk '{print $2}'`; do { REPLACE=`echo "${i}" | perl -p0 -e 's|\+|\\\\+|g' | perl -p0 -e 's|\/|\\\\/|g'`; perl -pi -e "s|${REPLACE}|\"${i}\"|g" ${CONVERTED_POINTER_DNS}; }; done
		sed -i '/^x\._domainkey/d' "${CONVERTED_POINTER_DNS}"
		sed -i '/^default\._domainkey/d' "${CONVERTED_POINTER_DNS}"
		perl -pi -e 's|^;.*||g' "${CONVERTED_POINTER_DNS}"
	else
		{
			echo "\$TTL 3600"
			echo "@       IN      SOA     ${NS1}.      hostmaster.${i}. ("
			echo "												2003120200"
			echo "												3600"
			echo "												3600"
			echo "												1209600"
			echo "												86400 )"
			echo ""
			echo "${i}.     3600   IN      NS      ${NS1}."
			echo "${i}.     3600   IN      NS      ${NS2}."
			echo ""
			echo "ftp     3600   IN      A       ${DEFAULT_IP}"
			echo "${i}.     3600   IN      A       ${DEFAULT_IP}"
			echo "mail    3600   IN      A       ${DEFAULT_IP}"
			echo "pop    3600   IN      A       ${DEFAULT_IP}"
			echo "smtp    3600   IN      A       ${DEFAULT_IP}"
			echo "www     3600   IN      A       ${DEFAULT_IP}"
			# Copy MX records
			if [ -s "${CPANEL_POINTER_DNS_DATA}" ]; then
				grep 'IN.*MX' "${CPANEL_POINTER_DNS_DATA}"
			fi
		} > "${CONVERTED_POINTER_DNS}"
	fi
done

#Convert default domain
doConvertDomain "${DEFAULT_DOMAIN_NAME}" true

# Convert addon domains
grep '=' "${CP_ROOT}/addons" | while read -r line; do
	# Example line:
	#     addon.example.com=subdomain_example.net
	ADDON_DOMAIN=$(echo "${line}" | cut -d= -f1)
	ASSOCIATED_SUBDOMAIN=$(echo "${line}" | cut -d= -f2 | tr '_' '.')
	doConvertDomain "${ADDON_DOMAIN}" false "${ASSOCIATED_SUBDOMAIN}"
done

#Move squirrelmail data
if [ -e "${CP_ROOT}/homedir/.sqmaildata" ]; then
	echo "Moving squirrelmail data"
	mkdir -p "${DA_ROOT}/backup/email_data"
	mv "${CP_ROOT}/homedir/.sqmaildata" "${DA_ROOT}/backup/email_data/squirrelmail"
fi

#Move main domain
echo "Moving default domain ${DEFAULT_DOMAIN_NAME} data..."
if [ -d "${CP_ROOT}/homedir/public_html" ]; then
	mv "${CP_ROOT}/homedir/public_html" "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/public_html"
	DOMAIN_PATH="${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/public_html"
	#Move out-of public_html subdomains
	if [ -d "${CP_ROOT}/${DEFAULT_DOMAIN_NAME}_domainsdir" ]; then
		mv "${CP_ROOT}/${DEFAULT_DOMAIN_NAME}_domainsdir"/* "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/" 2>/dev/null
	fi
	#Move password protected directories
	if [ -d "${CP_ROOT}/homedir/.htpasswds/public_html" ]; then
		mkdir -p "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd"
		mv "${CP_ROOT}/homedir/.htpasswds/public_html" "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/public_html"
		find "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/" -name 'passwd' -execdir mv {} .htpasswd \;
		find "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/" -name .htpasswd -type f -printf "/domains/${DEFAULT_DOMAIN_NAME}/%P\n" > "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/.protected.list"
		perl -pi -e 's|/\.htpasswd||g' "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/.protected.list"
		while read -r LINE; do
			FULLPATH="${DA_ROOT}/${LINE}"
			if [ -s "${FULLPATH}/.htaccess" ]; then
				if grep -q '^AuthUserFile ' "${FULLPATH}/.htaccess"; then
					perl -pi -e "s|^AuthUserFile \"/home/${USERNAME}/\.htpasswds|AuthUserFile \"/home/${USERNAME}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd|g" "${FULLPATH}/.htaccess"
					perl -pi -e 's|/passwd"|/.htpasswd"|g' "${FULLPATH}/.htaccess"
				fi
			fi
		done < "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/.htpasswd/.protected.list"
	fi
	#Auto-find and comment out any ea PHP handlers in .htaccess files
	clean_htaccess_cp_php_handlers "${DOMAIN_PATH}"
else
	mkdir -p "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/public_html"
fi

if [ ! -e "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/private_html" ]; then
	ln -sf ./public_html "${DA_ROOT}/domains/${DEFAULT_DOMAIN_NAME}/private_html"
fi

#Move user emails
echo "Moving main email account..."
MAILBOX_FORMAT=maildir
if [ -s "${CP_ROOT}/homedir/mail/mailbox_format.cpanel" ]; then
	if grep -q mdbox "${CP_ROOT}/homedir/mail/mailbox_format.cpanel"; then
		MAILBOX_FORMAT=mdbox
	fi
fi

#Remove symlinks to virtual mail inboxes before conversion
find "${CP_ROOT}/homedir/mail" -maxdepth 1 -name '.*' -type l -delete
convert_mail_path "${CP_ROOT}/homedir/mail" "${CP_ROOT}/homedir" "${MAILBOX_FORMAT}" || EXIT_CODE=2

echo "Removing symlinks from home folder:"
find "${CP_ROOT}/homedir" -maxdepth 1 -type l -delete -print
find "${CP_ROOT}/homedir" -maxdepth 3 -name 'public_html' -type l -delete -print
if [ -d "${CP_ROOT}/homedir/mail" ]; then
	echo "Renaming mail/ folder (in case something missing is still there), all converted to imap/"
	mv "${CP_ROOT}/homedir/mail" "${CP_ROOT}/homedir/.mail_unused_folder_from_cpanel"
fi
if [ -d "${CP_ROOT}/homedir/.trash" ]; then
	echo "Renaming .trash to .trash.cpanel"
	mv "${CP_ROOT}/homedir/.trash" "${CP_ROOT}/homedir/.trash.cpanel"
fi

RESELLER=0
if [ -s "${CP_ROOT}/resellerconfig/resellers" ]; then
	if grep -q "^${USERNAME}:" "${CP_ROOT}/resellerconfig/resellers"; then
		RESELLER=1
	fi
fi

if [ -z "${CP2DA_OUTPUT_FILE}" ]; then
	TAR_ENDING="tar.gz"
	if command -v zstd > /dev/null; then
		TAR_ENDING="tar.zst"
	fi
	if [ ${RESELLER} -eq 0 ]; then
		CP2DA_OUTPUT_FILE="${BACKUP_DIR}/user.${CP2DA_OWNER}.${USERNAME}.${TAR_ENDING}"
	else
		CP2DA_OUTPUT_FILE="${BACKUP_DIR}/reseller.${CP2DA_OWNER}.${USERNAME}.${TAR_ENDING}"
	fi
fi

HOME_TAR_FILE=${DA_ROOT}/backup/home.tar.gz
HOME_TAR_COMPRESS=--gzip
if ${PIGZ}; then
	HOME_TAR_COMPRESS=--use-compress-program=pigz
fi
if [ "${CP2DA_OUTPUT_FILE}" != "${CP2DA_OUTPUT_FILE%.zst}" ]; then
	HOME_TAR_FILE=${DA_ROOT}/backup/home.tar.zst
	HOME_TAR_COMPRESS=--use-compress-program=zstd
fi
tar --create --file="${HOME_TAR_FILE}" ${HOME_TAR_COMPRESS} --directory="${CP_ROOT}/homedir" --preserve-permissions \
	--exclude=./.cphorde \
	--exclude=./.gemrc \
	--exclude=./.contactemail \
	--exclude=./.lastlogin \
	--exclude=./.cpanel \
	--exclude=./.cpaddons \
	--exclude=./.zshrc \
	--exclude=./cpbackup-exclude.conf \
	--exclude=./tmp \
	--exclude=./ssl \
	--exclude=./logs \
	--exclude=./cpanel3-skel \
	--exclude=./.shadow \
	--exclude=./backups \
	--exclude=./imap \
	--exclude=./mail \
	--exclude=./user_backups \
	--exclude=./admin_backups \
	--exclude=./public_html \
	--exclude=./domains \
	.

#Create dependencies
touch "${DA_ROOT}/backup/bandwidth.tally"
touch "${DA_ROOT}/backup/user.usage"

#Convert cronjobs
if [ -s "${CP_ROOT}/cron/${USERNAME}" ]; then
	echo "Converting cronjobs..."
	awk '/^[0-9*]/ {n=n+1;print n "=" $0}' "${CP_ROOT}/cron/${USERNAME}" > "${DA_ROOT}/backup/crontab.conf"
	#Fix softaculous cronjobs
	perl -pi -e 's|/usr/local/cpanel/3rdparty/bin/php|/usr/local/bin/php|g' "${DA_ROOT}/backup/crontab.conf"
	perl -pi -e 's|/usr/local/cpanel/whostmgr/docroot/cgi/softaculous|/usr/local/directadmin/plugins/softaculous|g' "${DA_ROOT}/backup/crontab.conf"
fi

DATE_CREATED=$(date +'%a %b %d %H:%m:%S %Y')
if [ -n "${CPANEL_STARTDATE}" ]; then
	DATE_CREATED=$(date -d "@${CPANEL_STARTDATE}" +'%a %b %d %H:%m:%S %Y')
fi
#Generate user.conf
DA_USER_CONF=${DA_ROOT}/backup/user.conf
{
	echo "account=ON"
	echo "additional_bandwidth=0"
	echo "aftp=ON"
	echo "api_with_password=yes"
	echo "bandwidth=${CPANEL_BWLIMIT}"
	echo "catchall=ON"
	echo "cgi=${CPANEL_HASCGI}"
	echo "creator=${CP2DA_OWNER}"
	echo "cron=ON"
	echo "date_created=${DATE_CREATED}"
	echo "dnscontrol=ON"
	echo "docsroot=./data/skins/evolution"
	echo "domain=${DEFAULT_DOMAIN_NAME}"
	echo "domainptr=${CPANEL_MAXPARK}"
	echo "email=${CUSTOMER_EMAIL}"
	echo "ftp=${CPANEL_MAXFTP}"
	echo "inode=unlimited"
	echo "ip=${DEFAULT_IP}"
	echo "language=en"
	echo "login_keys=OFF"
	echo "mysql=${CPANEL_MAXSQL}"
	echo "name=${USERNAME}"
	echo "nemailf=unlimited"
	echo "nemailml=${CPANEL_MAXLST}"
	echo "nemailr=unlimited"
	echo "nemails=${CPANEL_MAXPOP}"
	echo "notify_on_all_question_failures=yes"
	echo "notify_on_all_twostep_auth_failures=yes"
	echo "ns1=${NS1}"
	echo "ns2=${NS2}"
	echo "nsubdomains=${CPANEL_MAXSUB}"
	echo "package=${CPANEL_PLAN}"
	echo "php=ON"
	echo "quota=${CPANEL_QUOTA}"
	echo "security_questions=no"
	echo "serverip=${DEFAULT_IP}"
	echo "sentwarning=no"
	echo "skin=evolution"
	echo "spam=ON"
	echo "ssh=${CPANEL_HASSHELL}"
	echo "ssl=ON"
	echo "suspend_at_limit=ON"
	echo "suspended=${ACCOUNT_SUSPENDED}"
	echo "sysinfo=ON"
	echo "twostep_auth=no"
	echo "username=${USERNAME}"
	echo "usertype=user"
	echo "vdomains=${CPANEL_MAXADDON}"
	echo "zoom=100"
} > "${DA_USER_CONF}"

#Generate user_ip.list
if [ ! -e "${DA_ROOT}/backup/user_ip.list" ]; then
	if grep -q "^ip=" "${DA_USER_CONF}"; then
		grep "^ip=" "${DA_USER_CONF}" | cut -d= -f2 > "${DA_ROOT}/backup/user_ip.list"
	fi
	if [ -s "${CP_ROOT}/ips/related_ips" ]; then
		perl -pi -e 's|0000|0|g' "${CP_ROOT}/ips/related_ips"
		cat "${CP_ROOT}/ips/related_ips" >> "${DA_ROOT}/backup/user_ip.list"
		echo '' >> "${DA_ROOT}/backup/user_ip.list"
	fi
fi

#Create MySQL databases/users
CPANEL_SQL_FILE=${CP_ROOT}/mysql.sql

for i in $(grep -o 'ON `.*' "${CPANEL_SQL_FILE}" | cut -d\` -f2 | tr -d \\\\ | sort -u); do {
	if [ "${i}" = "${USERNAME}_%" ]; then
		echo "Skipping ${USERNAME}_%..."
		continue
	fi
	if [ -s "${CP_ROOT}/mysql/${i}.sql.gz" ]; then
		"gunzip ${CP_ROOT}/mysql/${i}.sql.gz"
	fi
	if [ ! -e "${CP_ROOT}/mysql/${i}.sql" ]; then
		echo "Skipping ${i}, because database file not found in cPanel backup..."
		continue
	fi
	
	DIFFERENT_DB_NAME_IN_BACKUP=false
	DA_DATABASE_CONF="${DA_ROOT}/backup/${i}.conf"
	DATABASE_NAME="${i}"
	if echo "${i}" | grep -q '_'; then
		DA_DATABASE_FIRST_PART=$(echo "${i}" | cut -d_ -f1)
		DA_DATABASE_WITHOUT_FIRST_PART=$(echo "${i}" | perl -p0 -e "s|^${DA_DATABASE_FIRST_PART}_||g")
		if echo "${DA_DATABASE_WITHOUT_FIRST_PART}" | grep -m1 -E -q '\.|\*|\+|\-'; then
			DA_DATABASE_WITHOUT_FIRST_PART=$(echo "${DA_DATABASE_WITHOUT_FIRST_PART}" | tr -d '\.|\*|\+|\-')
			DATABASE_NAME="${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
			DA_DATABASE_CONF="${DA_ROOT}/backup/${DATABASE_NAME}.conf"
			DIFFERENT_DB_NAME_IN_BACKUP=true
		fi
		if [ "${DA_DATABASE_FIRST_PART}" != "${USERNAME}" ]; then
			>&2 echo "WARNING! ${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART} cannot be owned by ${USERNAME}, renaming database user to ${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
			EXIT_CODE=2
			DA_DATABASE_CONF="${DA_ROOT}/backup/${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}.conf"
			DATABASE_NAME="${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
			#Auto-find and replace old MySQL DB name with new one, with making a copy of the file before that, max 3 levels depth for efficiency
			if [ "${DA_DATABASE_FIRST_PART}" != "" ]; then
				DIFFERENT_DB_NAME_IN_BACKUP=true
				>&2 echo "Trying to find files in public_html to rename ${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART} to ${DATABASE_NAME}. A copy of the file will have '.cpanel_backup_copy_dbname.php' appended at the end."
				>&2 find ${DA_ROOT}/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART}" {} \; -exec cp -pf {} "{}.cpanel_backup_copy_dbname.php" \; -exec perl -pi -e "s|${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART}|${DATABASE_NAME}|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${DATABASE_NAME} \;
				if [ ! -e "${CP_ROOT}/mysql/found_${DATABASE_NAME}" ]; then
					>&2 find ${CP_ROOT}/homedir -maxdepth 3 -path ${CP_ROOT}/homedir/public_html -prune -o \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART}" {} \; -exec cp -pf {} "{}.cpanel_backup_copy_dbname.php" \; -exec perl -pi -e "s|${DA_DATABASE_FIRST_PART}_${DA_DATABASE_WITHOUT_FIRST_PART}|${DATABASE_NAME}|g" {} \;
				fi
			fi
		fi
	else
		DA_DATABASE_WITHOUT_FIRST_PART="${i}"
		DIFFERENT_DB_NAME_IN_BACKUP=true
		if echo "${DA_DATABASE_WITHOUT_FIRST_PART}" | grep -q '\.|\*|\+|\-'; then
			DA_DATABASE_WITHOUT_FIRST_PART=$(echo "${DA_DATABASE_WITHOUT_FIRST_PART}" | tr -d '\.|\*|\+|\-')
			DATABASE_NAME="${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
			DA_DATABASE_CONF="${DA_ROOT}/backup/${DATABASE_NAME}.conf"
			DIFFERENT_DB_NAME_IN_BACKUP=true
		fi
		>&2 echo "WARNING! ${DA_DATABASE_WITHOUT_FIRST_PART} cannot be owned by ${USERNAME}, renaming database user to ${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
		EXIT_CODE=2
		DA_DATABASE_CONF="${DA_ROOT}/backup/${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}.conf"
		DATABASE_NAME="${USERNAME}_${DA_DATABASE_WITHOUT_FIRST_PART}"
		#Auto-find and replace old MySQL DB name with new one, with making a copy of the file before that, max 3 levels depth for efficiency
		if [ "${DA_DATABASE_WITHOUT_FIRST_PART}" != "" ]; then
			DIFFERENT_DB_NAME_IN_BACKUP=true
			>&2 echo "Trying to find files in public_html to rename ${DA_DATABASE_WITHOUT_FIRST_PART} to ${DATABASE_NAME}. A copy of the file will have '.cpanel_backup_copy_dbname.php' appended at the end."
			>&2 find ${DA_ROOT}/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "'${DA_DATABASE_WITHOUT_FIRST_PART}'" {} \; -exec cp -pf {} "{}.cpanel_backup_copy_dbname.php" \; -exec perl -pi -e "s|'${DA_DATABASE_WITHOUT_FIRST_PART}'|'${DATABASE_NAME}'|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${DATABASE_NAME} \;
			if [ ! -e "${CP_ROOT}/mysql/found_${DATABASE_NAME}" ]; then
				>&2 find ${DA_ROOT}/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "\"${DA_DATABASE_WITHOUT_FIRST_PART}\"" {} \; -exec cp -pf {} "{}.cpanel_backup_copy_dbname.php" \; -exec perl -pi -e "s|\"${DA_DATABASE_WITHOUT_FIRST_PART}\"|\"${DATABASE_NAME}\"|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${DATABASE_NAME} \;
			fi
			if [ ! -e "${CP_ROOT}/mysql/found_${DATABASE_NAME}" ]; then
				>&2 find ${CP_ROOT}/homedir -maxdepth 3 -path ${CP_ROOT}/homedir/public_html -prune -o \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "'${DA_DATABASE_WITHOUT_FIRST_PART}'" {} \; -exec cp -pf {} "{}.cpanel_backup_copy_dbname.php" \; -exec perl -pi -e "s|'${DA_DATABASE_WITHOUT_FIRST_PART}'|'${DATABASE_NAME}'|g" {} \;
			fi
		fi
	fi
	:> "${DA_DATABASE_CONF}"
	ACCESSHOSTS_STRING="accesshosts"
	COUNTER=0
	ESCAPED_DB_NAME=`echo "${i}" | perl -p0 -e 's|\_|\\\\\\\\_|g'`
	:> "${DA_DATABASE_CONF}.tmp"
	USERNAMES_REPLACED=":"
	for u in `grep -o "ON \\\`${ESCAPED_DB_NAME}\\\`.*TO '[^ ]*" ${CPANEL_SQL_FILE} | awk '{print $4}' | tr -d ';'; grep -o "ON \\\`${i}\\\`.*TO '[^ ]*" ${CPANEL_SQL_FILE} | awk '{print $4}' | tr -d ';'`; do {
		COLLATION=""
		if [ -s "${CP_ROOT}/mysql/${i}.create" ]; then
			COLLATION=$(grep -o 'DEFAULT CHARACTER SET [^ ]*' "${CP_ROOT}/mysql/${i}.create" | awk '{print $4}')
		fi
		if [ "${COLLATION}" = "" ]; then
			COLLATION="latin1"
		fi
		if ! grep -q "^db_collation=" "${DA_DATABASE_CONF}.tmp"; then
			echo "db_collation=CATALOG_NAME=def&DEFAULT_CHARACTER_SET_NAME=${COLLATION}&SCHEMA_NAME=${DATABASE_NAME}&SQL_PATH=" >> "${DA_DATABASE_CONF}.tmp"
		fi
		PASSWORD="`grep 'IDENTIFIED BY PASSWORD' ${CPANEL_SQL_FILE} | tr -d '\`' | grep \"TO ${u}[^ ]*\" | grep -m1 -o \"IDENTIFIED BY PASSWORD '[^']*\" | cut -d\' -f2 | perl -p0 -e 's|^-|*|g'`"
		if [ "${PASSWORD}" = "" ]; then
			PASSWORD='unknown'
			>&2 echo "WARNING: unable to find database user password for ${u}"
			EXIT_CODE=2
		fi
		USERNAME_PART="`echo \"${u}\" | cut -d\@ -f1 | cut -d\' -f2`"
		if echo "${USERNAME_PART}" | grep -q '_'; then
			USERNAME_FIRST_PART="`echo \"${USERNAME_PART}\" | cut -d_ -f1`"
			USERNAME_WITHOUT_FIRST_PART="`echo \"${USERNAME_PART}\" | perl -p0 -e \"s|^${USERNAME_FIRST_PART}_||g\"`"
			USERNAME_PART="${USERNAME}_${USERNAME_WITHOUT_FIRST_PART}"
			if [ "${USERNAME_FIRST_PART}" != "${USERNAME}" ] && ! echo "${USERNAMES_REPLACED}" | grep -q ":${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}:"; then
				>&2 echo "WARNING! ${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART} username cannot be owned by ${USERNAME}, renaming database user to ${USERNAME}_${USERNAME_WITHOUT_FIRST_PART}"
				EXIT_CODE=2
				#Auto-find and replace old MySQL username with new one, with making a copy of the file before that, max 3 levels depth for efficiency
				if [ "${USERNAME_FIRST_PART}" != "" ]; then
					>&2 echo "Trying to find files in public_html to rename ${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART} to ${USERNAME_PART}. A copy of the file will have '.cpanel_backup_copy.php' appended at the end."
					>&2 find ${DA_ROOT}/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}" {} \; -exec cp -pf {} "{}.cpanel_backup_copy.php" \; -exec perl -pi -e "s|${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}|${USERNAME_PART}|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${USERNAME_PART} \;
					if [ ! -e "${CP_ROOT}/mysql/found_${USERNAME_PART}" ]; then
						>&2 find "${CP_ROOT}/homedir" -maxdepth 3 -path "${CP_ROOT}/homedir/public_html" -prune -o \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}" {} \; -exec cp -pf {} "{}.cpanel_backup_copy.php" \; -exec perl -pi -e "s|${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}|${USERNAME_PART}|g" {} \;
					fi
				fi
				USERNAMES_REPLACED="${USERNAMES_REPLACED}${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}:"
			fi
		elif [ "${USERNAME_PART}" != "${USERNAME}" ]; then
			USERNAME_WITHOUT_FIRST_PART="${USERNAME_PART}"
			USERNAME_PART="${USERNAME}_${USERNAME_WITHOUT_FIRST_PART}"
			>&2 echo "WARNING! ${USERNAME_WITHOUT_FIRST_PART} username cannot be owned by ${USERNAME}, renaming database user to ${USERNAME}_${USERNAME_WITHOUT_FIRST_PART}"
			EXIT_CODE=2
			#Auto-find and replace old MySQL username with new one, with making a copy of the file before that, max 3 levels depth for efficiency
			if [ "${USERNAME_WITHOUT_FIRST_PART}" != "" ]; then
				>&2 echo "Trying to find files in public_html to rename ${USERNAME_WITHOUT_FIRST_PART} to ${USERNAME_PART}. A copy of the file will have '.cpanel_backup_copy.php' appended at the end."
				>&2 find "${DA_ROOT}"/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "'${USERNAME_WITHOUT_FIRST_PART}'" {} \; -exec cp -pf {} "{}.cpanel_backup_copy.php" \; -exec perl -pi -e "s|'${USERNAME_WITHOUT_FIRST_PART}'|'${USERNAME_PART}'|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${USERNAME_PART} \;
				if [ ! -e "${CP_ROOT}/mysql/found_${USERNAME_PART}" ]; then
					>&2 find "${DA_ROOT}"/domains/*/public_html -maxdepth 3 \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "\"${USERNAME_WITHOUT_FIRST_PART}\"" {} \; -exec cp -pf {} "{}.cpanel_backup_copy.php" \; -exec perl -pi -e "s|\"${USERNAME_WITHOUT_FIRST_PART}\"|\"${USERNAME_PART}\"|g" {} \; -exec touch ${CP_ROOT}/mysql/found_${USERNAME_PART} \;
				fi
				if [ ! -e "${CP_ROOT}/mysql/found_${USERNAME_PART}" ]; then
					>&2 find "${CP_ROOT}/homedir" -maxdepth 3 -path "${CP_ROOT}/homedir/public_html" -prune -o \( -name "*.php" -o -name '.env' \) ! -name '*.cpanel_backup_copy.php' ! -name '*.cpanel_backup_copy_dbname.php' -exec grep -m1 -l "'${USERNAME_WITHOUT_FIRST_PART}'" {} \; -exec cp -pf {} "{}.cpanel_backup_copy.php" \; -exec perl -pi -e "s|'${USERNAME_WITHOUT_FIRST_PART}'|'${USERNAME_PART}'|g" {} \;
				fi
			fi
			USERNAMES_REPLACED="${USERNAMES_REPLACED}${USERNAME_FIRST_PART}_${USERNAME_WITHOUT_FIRST_PART}:"
		fi
		ACCESSHOST_PART="`echo \"${u}\" | cut -d\@ -f2 | cut -d\' -f2`"
		if ! echo "${ACCESSHOSTS_STRING}" | grep -q "=${ACCESSHOST_PART}$"; then
			if ! echo "${ACCESSHOSTS_STRING}" | grep -q "=${ACCESSHOST_PART}&"; then
				if [ ${COUNTER} -eq 0 ]; then
					ACCESSHOSTS_STRING="${ACCESSHOSTS_STRING}=${COUNTER}=${ACCESSHOST_PART}"
				else
					ACCESSHOSTS_STRING="${ACCESSHOSTS_STRING}&${COUNTER}=${ACCESSHOST_PART}"
				fi
				COUNTER=$((COUNTER + 1))
			fi
		fi
		if ! grep -q "^${USERNAME_PART}=" "${DA_DATABASE_CONF}.tmp"; then
			echo "Creating database user ${USERNAME_PART} for database ${i}..."
			echo "${USERNAME_PART}=alter_priv=Y&alter_routine_priv=Y&create_priv=Y&create_routine_priv=Y&create_tmp_table_priv=Y&create_view_priv=Y&delete_priv=Y&drop_priv=Y&event_priv=Y&execute_priv=Y&grant_priv=N&index_priv=Y&insert_priv=Y&lock_tables_priv=Y&passwd=${PASSWORD}&references_priv=Y&select_priv=Y&show_view_priv=Y&trigger_priv=Y&update_priv=Y" >> "${DA_DATABASE_CONF}.tmp"
		fi
	}
	done
	if ! grep -q "^${USERNAME}=" "${DA_DATABASE_CONF}.tmp"; then
		PASSWORD="`grep 'IDENTIFIED BY PASSWORD' ${CPANEL_SQL_FILE} | tr -d '\`' | grep \"TO '${USERNAME}'[^ ]*\" | grep -m1 -o \"IDENTIFIED BY PASSWORD '[^']*\" | cut -d\' -f2 | perl -p0 -e 's|^-|*|g'`"
		if [ "${PASSWORD}" != "" ]; then
			echo "Creating database user ${USERNAME} for database ${i}..."
			echo "${USERNAME}=alter_priv=Y&alter_routine_priv=Y&create_priv=Y&create_routine_priv=Y&create_tmp_table_priv=Y&create_view_priv=Y&delete_priv=Y&drop_priv=Y&event_priv=Y&execute_priv=Y&grant_priv=N&index_priv=Y&insert_priv=Y&lock_tables_priv=Y&passwd=${PASSWORD}&references_priv=Y&select_priv=Y&show_view_priv=Y&trigger_priv=Y&update_priv=Y" >> "${DA_DATABASE_CONF}.tmp"
		fi
	fi
	echo "${ACCESSHOSTS_STRING}" > "${DA_DATABASE_CONF}"
	cat "${DA_DATABASE_CONF}.tmp" >> "${DA_DATABASE_CONF}"
	rm -f "${DA_DATABASE_CONF}.tmp"
	if [ -e "${CP_ROOT}/mysql/${i}.sql" ]; then
		echo "Moving database ${i} files..."
		mv "${CP_ROOT}/mysql/${i}.sql" "${DA_ROOT}/backup/${DATABASE_NAME}.sql"
		if ${DIFFERENT_DB_NAME_IN_BACKUP}; then
			sed -i '/^CREATE DATABASE /d' "${DA_ROOT}/backup/${DATABASE_NAME}.sql"
		        sed -i '/^USE `/d' "${DA_ROOT}/backup/${DATABASE_NAME}.sql"
		fi
	fi
}
done

if [ -d "${CP_ROOT}/psql" ]; then
	NUMBER_OF_FILES=$(ls "${CP_ROOT}/psql/" | wc -l)
	if [ "${NUMBER_OF_FILES}" -gt 0 ]; then
		>&2 echo "WARNING! PostgreSQL databases detected in pgsql/, these will not be restored:"
		>&2 ls "${CP_ROOT}/psql/"
		EXIT_CODE=2
	fi
fi

if [ -d "${CP_ROOT}/mma/priv" ]; then
	NUMBER_OF_FILES=$(ls "${CP_ROOT}/mma/priv/" | wc -l)
	if [ "${NUMBER_OF_FILES}" -gt 0 ]; then
		>&2 echo "WARNING! Mailman files detected in mma/priv/, these will not be restored:"
		>&2 ls "${CP_ROOT}/mma/priv/"
		EXIT_CODE=2
	fi
fi

if [ -d "${CP_ROOT}/mma/pub" ]; then
	NUMBER_OF_FILES=$(ls "${CP_ROOT}/mma/pub/" | wc -l)
	if [ "${NUMBER_OF_FILES}" -gt 0 ]; then
		>&2 echo "WARNING! Mailman files detected in mma/pub/, these will not be restored:"
		>&2 ls "${CP_ROOT}/mma/pub/"
		EXIT_CODE=2
	fi
fi

if [ -d "${CP_ROOT}/mm" ]; then
	NUMBER_OF_FILES=$(ls "${CP_ROOT}/mm/" | wc -l)
	if [ "${NUMBER_OF_FILES}" -gt 0 ]; then
		>&2 echo "WARNING! Mailman files detected in mm/, these will not be restored:"
		>&2 ls "${CP_ROOT}/mm/"
		EXIT_CODE=2
	fi
fi

if [ ${RESELLER} -gt 0 ]; then
	if [ -d "${CP_ROOT}/homedir/cpanel3-skel/public_html" ]; then
		mv "${CP_ROOT}/homedir/cpanel3-skel/public_html" "${DA_ROOT}/domains/default"
	fi
	if [ ! -d "${DA_ROOT}/domains/suspended" ] && [ -d "/usr/local/directadmin/data/templates/suspended" ]; then
		cp -R /usr/local/directadmin/data/templates/suspended "${DA_ROOT}/domains/suspended"
	fi
	perl -pi -e 's/usertype=user/usertype=reseller/' "${DA_USER_CONF}"
	# Creating backup.conf
	USER_BACKUP_CONF=${DA_ROOT}/backup/backup.conf
	if [ ! -e "${USER_BACKUP_CONF}" ]; then
		{
			echo "ftp_ip="
			echo "ftp_password="
			echo "ftp_path=/"
			echo "ftp_username="
			echo "local_path="
		} > "${USER_BACKUP_CONF}"
	fi

	# Creating reseller.conf

if [ -s "${CP_ROOT}/resellerconfig/my_reseller-limits.yaml" ]; then
	CPANEL_RESELLER_BWLIMIT=$(sed -n 'H; /"type":/h; ${g;p;}' "${CP_ROOT}/resellerconfig/my_reseller-limits.yaml" | grep -m1 '"bw"' | awk '{print $2}')
fi
if [ -z "${CPANEL_RESELLER_BWLIMIT}" ]; then
	CPANEL_RESELLER_BWLIMIT=${CPANEL_BWLIMIT}
fi

if [ -s "${CP_ROOT}/resellerconfig/my_reseller-limits.yaml" ]; then
	CPANEL_RESELLER_QUOTA=`sed -n 'H; /"type":/h; ${g;p;}' ${CP_ROOT}/resellerconfig/my_reseller-limits.yaml | grep -m1 '"disk"' | awk '{print $2}'`
fi
if [ -z "${CPANEL_RESELLER_QUOTA}" ]; then
	CPANEL_RESELLER_QUOTA=${CPANEL_QUOTA}
fi

NS1_RESELLER=$(head -n1 "${CP_ROOT}/resellerconfig/resellers-nameservers" | cut -d: -f2 | cut -d, -f1)
if [ -n "${NS1_RESELLER}" ]; then
	NS2_RESELLER=$(head -n1 "${CP_ROOT}/resellerconfig/resellers-nameservers" | cut -d: -f2 | cut -d, -f2)
	if [ -z "${NS2_RESELLER}" ]; then
		NS2_RESELLER="${NS2}"
	fi
else
	NS1_RESELLER="${NS1}"
	NS2_RESELLER="${NS2}"
fi

{
	echo "additional_bandwidth=0"
	echo "aftp=OFF"
	echo "api_with_password=yes"
	echo "bandwidth=${CPANEL_RESELLER_BWLIMIT}"
	echo "catchall=OFF"
	echo "cgi=${CPANEL_HASCGI}"
	echo "cron=OFF"
	echo "dnscontrol=OFF"
	echo "domainptr=${CPANEL_MAXPARK}"
	echo "ftp=${CPANEL_MAXFTP}"
	echo "inode=unlimited"
	echo "login_keys=OFF"
	echo "mysql=${CPANEL_MAXSQL}"
	echo "nemailf=${CPANEL_MAXPOP}"
	echo "nemailml=${CPANEL_MAXLST}"
	echo "nemailr=unlimited"
	echo "nemails=unlimited"
	echo "notify_on_all_question_failures=yes"
	echo "notify_on_all_twostep_auth_failures=yes"
	echo "ns1=${NS1}"
	echo "ns2=${NS2}"
	echo "nsubdomains=${CPANEL_MAXSUB}"
	echo "package=${CPANEL_PLAN}"
	echo "php=ON"
	echo "quota=${CPANEL_RESELLER_QUOTA}"
	echo "security_questions=no"
	echo "sentwarning=no"
	echo "spam=OFF"
	echo "ssh=${CPANEL_HASSHELL}"
	echo "ssl=ON"
	echo "sysinfo=OFF"
	echo "twostep_auth=no"
	echo "vdomains=${CPANEL_MAXADDON}"
	echo "userssh=${CPANEL_HASSHELL}"
	echo "dns=ON"
	echo "ip=shared"
	echo "ips=0"
	echo "oversell=ON"
	echo "serverip=ON"
	echo "subject=Your account for |domain| is now ready for use."
} > "${DA_ROOT}/backup/reseller.conf"

	# Creating ip.list
	if [ ! -e "${DA_ROOT}/backup/ip.list" ]; then
		grep "ip=" "${USER_BACKUP_CONF}" | cut -d= -f2 > "${DA_ROOT}/backup/ip.list"
		if [ -s "${CP_ROOT}/ips/related_ips" ]; then
			perl -pi -e 's|0000|0|g' "${CP_ROOT}/ips/related_ips"
			cat "${CP_ROOT}/ips/related_ips" >> "${DA_ROOT}/backup/ip.list"
			echo '' >> "${DA_ROOT}/backup/ip.list"
		fi
	fi

	# Creating everything else
	touch "${DA_ROOT}/backup/login.hist"
	touch "${DA_ROOT}/backup/reseller.history"
	touch "${DA_ROOT}/backup/users.list"
	if [ -e /usr/local/directadmin/data/users/admin/u_welcome.txt ]; then
		cp -f /usr/local/directadmin/data/users/admin/u_welcome.txt "${DA_ROOT}/backup/u_welcome.txt"
	fi
	
	# Creating empty packages
	mkdir -p "${DA_ROOT}/backup/packages"
	echo -n '' > "${DA_ROOT}/backup/packages.list"
	
	# Transfer packages
	find "${CP_ROOT}/resellerpackages" -print0 | while IFS= read -r -d $'\0' file
	do
		PACKAGEFILE="${file}"
		if [ "${PACKAGEFILE}" != "${CP_ROOT}/resellerpackages" ]; then
			PACKAGENAME=$(basename "${PACKAGEFILE}")
			if [ "${PACKAGENAME}" != "" ] && [ -s "${PACKAGEFILE}" ]; then
				echo "Converting package ${PACKAGENAME}..."
				PACKAGENAME=$(echo "${PACKAGENAME}" | tr ' ' '_')
				PACKAGE_TO_WRITE="${DA_ROOT}/backup/packages/${PACKAGENAME}.pkg"
				#Get default domain name
				PKGCPANEL_CGI=$(getPkgOpt CGI "${PACKAGEFILE}")
				if [ "${PKGCPANEL_CGI}" = "y" ]; then
					PKGCPANEL_CGI="ON"
				else
					PKGCPANEL_CGI="OFF"
				fi
				PKGCPANEL_HASSHELL=$(getPkgOpt HASSHELL "${PACKAGEFILE}")
				if [ "${PKGCPANEL_HASSHELL}" = "y" ]; then
					PKGCPANEL_HASSHELL="ON"
				else
					PKGCPANEL_HASSHELL="OFF"
				fi
				PKGCPANEL_MAXPARK=$(getPkgOpt MAXPARK "${PACKAGEFILE}")
				PKGCPANEL_MAXFTP=$(getPkgOpt MAXFTP "${PACKAGEFILE}")
				PKGCPANEL_MAXSQL=$(getPkgOpt MAXSQL "${PACKAGEFILE}")
				PKGCPANEL_MAXSUB=$(getPkgOpt MAXSUB "${PACKAGEFILE}")
				PKGCPANEL_MAXPOP=$(getPkgOpt MAXPOP "${PACKAGEFILE}")
				PKGCPANEL_MAXLST=$(getPkgOpt MAXLST "${PACKAGEFILE}")
				PKGCPANEL_MAXADDON=$(getPkgOpt MAXADDON "${PACKAGEFILE}")
				if [ "${PKGCPANEL_MAXADDON}" = "0" ]; then
					PKGCPANEL_MAXADDON="1"
				fi
				PKGCPANEL_BWLIMIT=$(getPkgOpt BWLIMIT "${PACKAGEFILE}")
				PKGCPANEL_QUOTA=$(getPkgOpt QUOTA "${PACKAGEFILE}")
				echo "${PACKAGENAME}" >> "${DA_ROOT}/backup/packages.list"
				{
					echo "aftp=OFF"
					echo "bandwidth=${PKGCPANEL_BWLIMIT}"
					echo "catchall=OFF"
					echo "cgi=${PKGCPANEL_CGI}"
					echo "cron=ON"
					echo "dnscontrol=ON"
					echo "domainptr=${PKGCPANEL_MAXPARK}"
					echo "ftp=${PKGCPANEL_MAXFTP}"
					echo "inode=unlimited"
					echo "language=en"
					echo "login_keys=OFF"
					echo "mysql=${PKGCPANEL_MAXSQL}"
					echo "nemailf=unlimited"
					echo "nemailml=${PKGCPANEL_MAXLST}"
					echo "nemailr=unlimited"
					echo "nemails=${PKGCPANEL_MAXPOP}"
					echo "nsubdomains=${PKGCPANEL_MAXSUB}"
					echo "php=ON"
					echo "quota=${PKGCPANEL_QUOTA}"
					echo "skin=evolution"
					echo "spam=ON"
					echo "ssh=${PKGCPANEL_HASSHELL}"
					echo "ssl=ON"
					echo "suspend_at_limit=ON"
					echo "sysinfo=ON"
					echo "vdomains=${PKGCPANEL_MAXADDON}"
				} > "${PACKAGE_TO_WRITE}"
			else
				echo "Unable to convert ${PACKAGEFILE}..."
			fi
		fi
	done
fi

echo "Creating DirectAdmin tarball..."

if [ ! -d "${DA_ROOT}/imap" ]; then
	mkdir -p "${DA_ROOT}/imap"
fi
touch "${CP2DA_OUTPUT_FILE}"
chmod 640 "${CP2DA_OUTPUT_FILE}"

MAIN_TAR_COMPRESS=--gzip
if ${PIGZ}; then
	MAIN_TAR_COMPRESS=--use-compress-program=pigz
fi
if [ "${CP2DA_OUTPUT_FILE}" != "${CP2DA_OUTPUT_FILE%.zst}" ]; then
	MAIN_TAR_COMPRESS=--use-compress-program=zstd
fi

if ! tar --create --file="${CP2DA_OUTPUT_FILE}" "${MAIN_TAR_COMPRESS}" \
		--directory="${DA_ROOT}" \
		--preserve-permissions \
		domains backup imap; then
	rm -rf "${CP_ROOT}"
	do_exit 7 "Unable to create DirectAdmin backup ${CP2DA_OUTPUT_FILE}. Exiting..."
fi

echo "Cleaning up..."
rm -rf "${CP_ROOT}"
rm -rf "${DA_ROOT}"

echo "Done! Backup is ready: ${CP2DA_OUTPUT_FILE}"
exit ${EXIT_CODE}
