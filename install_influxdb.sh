#!/bin/sh -e
# shellcheck disable=SC2059

readonly GREEN='\033[0;32m'
readonly BOLD='\033[1m'
readonly BOLDGREEN='\033[1;32m'
readonly DIM='\033[2m'
readonly NC='\033[0m' # No Color

ARCHITECTURE=$(uname -m)
ARTIFACT=""
IS_MUSL=""
OS=""
INSTALL_LOC=~/.influxdb
BINARY_NAME="influxdb3"
PORT=8181

EDITION="Core"
EDITION_TAG="core"
if [ "$1" = "enterprise" ]; then
    EDITION="Enterprise"
    EDITION_TAG="enterprise"
    shift 1
fi

### OS AND ARCHITECTURE DETECTION ###
case "$(uname -s)" in
    Linux*)     OS="Linux";;
    Darwin*)    OS="Darwin";;
    *)         OS="UNKNOWN";;
esac

if [ "${OS}" = "Linux" ]; then
    # ldd is a shell script but on some systems (eg Ubuntu) security hardening
    # prevents it from running when invoked directly. Since we only want to
    # use '--verbose', find the path to ldd, then invoke under sh to bypass ldd
    # hardening.
    # XXX: use 'uname -o | grep GNU' instead?
    ldd_exec=$(command -v ldd)
    if [ "${ARCHITECTURE}" = "x86_64" ] || [ "${ARCHITECTURE}" = "amd64" ]; then
        # Check if we're on a GNU/Linux system, otherwise default to musl
        if [ -n "$ldd_exec" ] && sh -c "$ldd_exec --version" 2>&1 | grep -Eq "(GNU|GLIBC)"; then
            ARTIFACT="x86_64-unknown-linux-gnu"
        else
            ARTIFACT="x86_64-unknown-linux-musl"
            IS_MUSL="yes"
        fi
    elif [ "${ARCHITECTURE}" = "aarch64" ] || [ "${ARCHITECTURE}" = "arm64" ]; then
        if [ -n "$ldd_exec" ] && sh -c "$ldd_exec --version" 2>&1 | grep -Eq "(GNU|GLIBC)"; then
            ARTIFACT="aarch64-unknown-linux-gnu"
        else
            ARTIFACT="aarch64-unknown-linux-musl"
            IS_MUSL="yes"
        fi
    fi
elif [ "${OS}" = "Darwin" ]; then
    if [ "${ARCHITECTURE}" = "x86_64" ]; then
        printf "Intel Mac support is coming soon!\n"
        printf "Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
        printf "View alternative binaries on our Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}.\n"
        exit 1
    else
        ARTIFACT="aarch64-apple-darwin"
    fi
fi

# Exit if unsupported system
[ -n "${ARTIFACT}" ] || {
    printf "Unfortunately this script doesn't support your '${OS}' | '${ARCHITECTURE}' setup, or was unable to identify it correctly.\n"
    printf "Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
    printf "View alternative binaries on our Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}.\n"
    exit 1
}

URL="https://dl.influxdata.com/influxdb/snapshots/influxdb3-${EDITION_TAG}_${ARTIFACT}.tar.gz"

START_TIME=$(date +%s)

# Attempt to clear screen and show welcome message
clear 2>/dev/null || true  # clear isn't available everywhere
printf "┌───────────────────────────────────────────────────┐\n"
printf "│ ${BOLD}Welcome to InfluxDB!${NC} We'll make this quick.       │\n"
printf "└───────────────────────────────────────────────────┘\n"

echo
printf "${BOLD}Select Installation Type${NC}\n"
echo
printf "1) ${GREEN}Docker Image${NC} ${DIM}(More Powerful, More Complex)${NC}\n"
printf "   ├─ Requires knowledge of Docker and Docker management\n"
printf "   └─ Includes the Processing Engine for real-time data transformation,\n"
printf "      enrichment, and general custom Python code execution.\n\n"
printf "2) ${GREEN}Simple Download${NC} ${DIM}(Automated Install, Quick Setup)${NC}\n"
printf "   ├─ No external dependencies required\n"
printf "   └─ The Processing Engine will be available soon for binary installations,\n"
printf "      bringing the same powerful processing capabilities to local deployments.\n"
echo
printf "Enter your choice (1-2): "
read -r INSTALL_TYPE

case "$INSTALL_TYPE" in
    1)
        printf "\n\n${BOLD}Download and Tag Docker Image${NC}\n"
        printf "├─ ${DIM}docker pull quay.io/influxdb/influxdb3-${EDITION_TAG}:latest${NC}\n"
        printf "└─ ${DIM}docker tag quay.io/influxdb/influxdb3-${EDITION_TAG}:latest influxdb3-${EDITION_TAG}${NC}\n\n"
        if ! docker pull "quay.io/influxdb/influxdb3-${EDITION_TAG}:latest"; then
            printf "└─ Error: Failed to download Docker image.\n"
            exit 1
        fi
        docker tag quay.io/influxdb/influxdb3-${EDITION_TAG}:latest influxdb3-${EDITION_TAG}
        # Exit script after Docker installation
        echo
        printf "${BOLD}NEXT STEPS${NC}\n"
        printf "1) Run the Docker image:\n"
        printf "   ├─ ${BOLD}mkdir plugins${NC} ${DIM}(To store and access plugins)${NC}\n"
        printf "   └─ ${BOLD}docker run -it -p ${PORT}:${PORT} -v ./plugins:/plugins influxdb3-${EDITION_TAG} serve --object-store memory --node-id node0 --plugin-dir /plugins${NC} ${DIM}(To start)${NC}\n"
        printf "2) View documentation at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}\n\n"

        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))

        out=" Time is everything. This process took $DURATION seconds. "
        mid=""
        for _ in $(seq 1 ${#out}); do
            mid="${mid}─"
        done
        printf "┌%s┐\n" "$mid"
        printf "│%s│\n" "$out"
        printf "└%s┘\n" "$mid"
        exit 0
        ;;
    2)
        printf "\n\n"
        ;;
    *)
        printf "Invalid choice. Defaulting to binary installation.\n\n"
        ;;
esac

# attempt to find the user's shell config
shellrc=
if [ -n "$SHELL" ]; then
    tmp=~/.$(basename "$SHELL")rc
    if [ -e "$tmp" ]; then
        shellrc="$tmp"
    fi
fi

printf "${BOLD}Downloading InfluxDB 3 %s to %s${NC}\n" "$EDITION" "$INSTALL_LOC"
printf "├─${DIM} mkdir -p '%s'${NC}\n" "$INSTALL_LOC"
mkdir -p "$INSTALL_LOC"
printf "└─${DIM} curl -sS '%s' -o '%s/influxdb3.tar.gz'${NC}\n" "${URL}" "$INSTALL_LOC"
curl -sS "${URL}" -o "$INSTALL_LOC/influxdb3.tar.gz"

echo
printf "${BOLD}Verifying '%s/influxdb3.tar.gz'${NC}\n" "$INSTALL_LOC"
printf "└─${DIM} curl -sS '%s.sha256' -o '%s/influxdb3.tar.gz.sha256'${NC}\n" "${URL}" "$INSTALL_LOC"
curl -sS "${URL}.sha256" -o "$INSTALL_LOC/influxdb3.tar.gz.sha256"
dl_sha=$(cut -d ' ' -f 1 "$INSTALL_LOC/influxdb3.tar.gz.sha256" | grep -E '^[0-9a-f]{64}$')
if [ -z "$dl_sha" ]; then
    printf "Could not find properly formatted SHA256 in '%s/influxdb3.tar.gz.sha256'. Aborting.\n" "$INSTALL_LOC"
    exit 1
fi
printf "└─${DIM} sha256sum '%s/influxdb3.tar.gz'" "$INSTALL_LOC"
ch_sha=$(sha256sum "$INSTALL_LOC/influxdb3.tar.gz" | cut -d ' ' -f 1)
if [ "$ch_sha" = "$dl_sha" ]; then
    printf " (OK: %s = %s)${NC}\n" "$ch_sha" "$dl_sha"
else
    printf " (ERROR: %s != %s). Aborting.${NC}\n" "$ch_sha" "$dl_sha"
    exit 1
fi
printf "└─${DIM} rm '%s/influxdb3.tar.gz.sha256'${NC}\n" "$INSTALL_LOC"
rm "$INSTALL_LOC/influxdb3.tar.gz.sha256"

echo
printf "${BOLD}Extracting and Processing${NC}\n"
printf "├─${DIM} tar -xf '%s/influxdb3.tar.gz' -C '%s'${NC}\n" "$INSTALL_LOC" "$INSTALL_LOC"
tar -xf "$INSTALL_LOC/influxdb3.tar.gz" -C "$INSTALL_LOC"
printf "└─${DIM} rm '%s/influxdb3.tar.gz'${NC}\n" "$INSTALL_LOC"
rm "$INSTALL_LOC/influxdb3.tar.gz"

if [ -n "$shellrc" ] && ! grep -q "export PATH=.*$INSTALL_LOC" "$shellrc"; then
    echo
    printf "${BOLD}Adding InfluxDB to '%s'${NC}\n" "$shellrc"
    printf "└─${DIM} export PATH=\"\$PATH:%s/\" >> '%s'${NC}\n" "$INSTALL_LOC" "$shellrc"
    echo "export PATH=\"\$PATH:$INSTALL_LOC/\"" >> "$shellrc"
fi

if [ "${EDITION}" = "Core" ]; then
    # Prompt user to start the service
    echo
    printf "${BOLD}Configuration Options${NC}\n"


    printf "└─ Start InfluxDB Now? (y/n): "
    read -r START_SERVICE
    if echo "$START_SERVICE" | grep -q "^[Yy]$" ; then
        # Prompt for Node ID
        echo
        printf "${BOLD}Enter Your Node ID${NC}\n"
        printf "├─ A Node ID is a unique, uneditable identifier for a service.\n"
        printf "└─ Enter a Node ID (default: node0): "
        read -r NODE_ID
        NODE_ID=${NODE_ID:-node0}

        # Prompt for storage solution
        echo
        printf "${BOLD}Select Your Storage Solution${NC}\n"
        printf "├─ 1) In-memory storage (Fastest, data cleared on restart)\n"
        printf "├─ 2) File storage (Persistent local storage)\n"
        printf "├─ 3) Object storage (Cloud-compatible storage)\n"
        printf "└─ Enter your choice (1-3): "
        read -r STORAGE_CHOICE

        case "$STORAGE_CHOICE" in
            1)
                STORAGE_TYPE="memory"
                STORAGE_FLAGS="--object-store=memory"
                STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                ;;
            2)
                STORAGE_TYPE="File Storage"
                echo
                printf "Enter storage path (default: %s/data): " "${INSTALL_LOC}"
                read -r STORAGE_PATH
                STORAGE_PATH=${STORAGE_PATH:-"${INSTALL_LOC}/data"}
                STORAGE_FLAGS="--object-store=file --data-dir ${STORAGE_PATH}"
                STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                ;;
            3)
                STORAGE_TYPE="Object Storage"
                echo
                printf "${BOLD}Select Cloud Provider${NC}\n"
                printf "├─ 1) Amazon S3\n"
                printf "├─ 2) Azure Storage\n"
                printf "├─ 3) Google Cloud Storage\n"
                printf "└─ Enter your choice (1-3): "
                read -r CLOUD_CHOICE

                case $CLOUD_CHOICE in
                    1)  # AWS S3
                        echo
                        printf "${BOLD}AWS S3 Configuration${NC}\n"
                        printf "├─ Enter AWS Access Key ID: "
                        read -r AWS_KEY

                        printf "├─ Enter AWS Secret Access Key: "
                        stty -echo
                        read -r AWS_SECRET
                        stty echo

                        echo
                        printf "├─ Enter S3 Bucket: "
                        read -r AWS_BUCKET

                        printf "└─ Enter AWS Region (default: us-east-1): "
                        read -r AWS_REGION
                        AWS_REGION=${AWS_REGION:-"us-east-1"}

                        STORAGE_FLAGS="--object-store=s3 --bucket=${AWS_BUCKET}"
                        if [ -n "$AWS_REGION" ]; then
                            STORAGE_FLAGS="$STORAGE_FLAGS --aws-default-region=${AWS_REGION}"
                        fi
                        STORAGE_FLAGS="$STORAGE_FLAGS --aws-access-key-id=${AWS_KEY}"
                        STORAGE_FLAGS_ECHO="$STORAGE_FLAGS --aws-secret-access-key=..."
                        STORAGE_FLAGS="$STORAGE_FLAGS --aws-secret-access-key=${AWS_SECRET}"
                        ;;

                    2)  # Azure Storage
                        echo
                        printf "${BOLD}Azure Storage Configuration${NC}\n"
                        printf "├─ Enter Storage Account Name: "
                        read -r AZURE_ACCOUNT

                        printf "└─ Enter Storage Access Key: "
                        stty -echo
                        read -r AZURE_KEY
                        stty echo

                        echo
                        STORAGE_FLAGS="--object-store=azure --azure-storage-account=${AZURE_ACCOUNT}"
                        STORAGE_FLAGS_ECHO="$STORAGE_FLAGS --azure-storage-access-key=..."
                        STORAGE_FLAGS="$STORAGE_FLAGS --azure-storage-access-key=${AZURE_KEY}"
                        ;;

                    3)  # Google Cloud Storage
                        echo
                        printf "${BOLD}Google Cloud Storage Configuration${NC}\n"
                        printf "└─ Enter path to service account JSON file: "
                        read -r GOOGLE_SA
                        STORAGE_FLAGS="--object-store=google --google-service-account=${GOOGLE_SA}"
                        STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                        ;;

                    *)
                        printf "Invalid cloud provider choice. Defaulting to file storage.\n"
                        STORAGE_TYPE="File Storage"
                        STORAGE_FLAGS="--object-store=file --data-dir ${INSTALL_LOC}/data"
                        STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                        ;;
                esac
                ;;

            *)
                printf "Invalid choice. Defaulting to in-memory.\n"
                STORAGE_TYPE="Memory"
                STORAGE_FLAGS="--object-store=memory"
                STORAGE_FLAGS_ECHO="$STORAGE_FLAGS"
                ;;
        esac

        # Ensure port is available; if not, find a new one. If IS_MUSL is set,
        # assume we are on a busybox-like system whose lsof doesn't support the
        # args we need
        lsof_exec=$(command -v lsof) && {
            while [ -n "$lsof_exec" ] && [ "$IS_MUSL" != "yes" ] && lsof -i:"$PORT" -t >/dev/null 2>&1; do
                printf "├─${DIM} Port %s is in use. Finding new port.${NC}\n" "$PORT"
                PORT=$((PORT + 1))
                if [ "$PORT" -gt 32767 ]; then
                    printf "└─${DIM} Could not find an available port. Aborting.${NC}\n"
                    exit 1
                fi
                if ! "$lsof_exec" -i:"$PORT" -t >/dev/null 2>&1; then
                    printf "└─${DIM} Found an available port: %s${NC}\n" "$PORT"
                    break
                fi
            done
        }

        # Start and give up to 30 seconds to respond
        echo
        printf "${BOLD}Starting InfluxDB${NC}\n"
        printf "├─${DIM} Node ID: %s${NC}\n" "$NODE_ID"
        printf "├─${DIM} Storage: %s${NC}\n" "$STORAGE_TYPE"
        printf "├─${DIM} '%s' serve --node-id='%s' --http-bind='0.0.0.0:%s' %s${NC}\n" "$INSTALL_LOC/$BINARY_NAME" "$NODE_ID" "$PORT" "$STORAGE_FLAGS_ECHO"
        "$INSTALL_LOC/$BINARY_NAME" serve --node-id="$NODE_ID" --http-bind="0.0.0.0:$PORT" "$STORAGE_FLAGS" > /dev/null &
        PID="$!"

        SUCCESS=0
        for _ in $(seq 1 30); do
            # on systems without a usable lsof, sleep a second to see if the pid is
            # still there to give influxdb a chance to error out in case an already
            # running influxdb is running on this port
            if [ -z "$lsof_exec" ] || [ "$IS_MUSL" = "yes" ]; then
                sleep 1
            fi

            if ! kill -0 "$PID" 2>/dev/null ; then
                break
            fi

            if curl --max-time 3 -s "http://localhost:$PORT/health" >/dev/null 2>&1; then
                printf "└─${BOLDGREEN} ✓ InfluxDB 3 ${EDITION} is now installed and running on port %s. Nice!${NC}\n" "$PORT"
                SUCCESS=1
                break
            fi
            sleep 1
        done

        if [ $SUCCESS -eq 0 ]; then
            printf "└─${BOLD} ERROR: InfluxDB failed to start; check permissions or other potential issues.${NC}\n" "$PORT"
            exit 1
        fi

        else
        echo
        printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
    fi
else
    echo
    printf "${BOLDGREEN}✓ InfluxDB 3 ${EDITION} is now installed. Nice!${NC}\n"
fi

### SUCCESS INFORMATION ###
echo
printf "${BOLD}Further Info${NC}\n"
if [ -n "$shellrc" ]; then
    printf "├─ Run ${BOLD}source '%s'${NC}, then access InfluxDB with ${BOLD}influxdb3${NC} command.\n" "$shellrc"
else
    printf "├─ Access InfluxDB with the ${BOLD}%s${NC} command.\n" "$INSTALL_LOC/$BINARY_NAME"
fi
printf "├─ View the Getting Started guide at \033[4;94mhttps://docs.influxdata.com/influxdb3/${EDITION_TAG}/${NC}.\n"
printf "└─ Visit our public Discord at \033[4;94mhttps://discord.gg/az4jPm8x${NC} for additional guidance.\n"
echo

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

out=" Time is everything. This process took $DURATION seconds. "
mid=""
for _ in $(seq 1 ${#out}); do
    mid="${mid}─"
done
printf "┌%s┐\n" "$mid"
printf "│%s│\n" "$out"
printf "└%s┘\n" "$mid"
