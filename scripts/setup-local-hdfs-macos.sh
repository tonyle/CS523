#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

HDFS_HOST="${HDFS_HOST:-localhost}"
HDFS_PORT="${HDFS_PORT:-9000}"
HDFS_WEB_PORT="${HDFS_WEB_PORT:-9870}"
HDFS_USER="${HDFS_USER:-$USER}"
HDFS_PROJECT_DIR="${HDFS_PROJECT_DIR:-/user/$HDFS_USER/cs523}"
HDFS_METADATA_PATH="${HDFS_METADATA_PATH:-$HDFS_PROJECT_DIR/crypto_metadata.csv}"
LOCAL_METADATA_CSV="${LOCAL_METADATA_CSV:-$ROOT/data/crypto_metadata.csv}"
HADOOP_DATA_ROOT="${HADOOP_DATA_ROOT:-$ROOT/.local/hadoop}"
FORCE_HDFS_FORMAT="${FORCE_HDFS_FORMAT:-false}"
START_HDFS="${START_HDFS:-true}"
HDFS_START_MODE="${HDFS_START_MODE:-auto}"
PASSWORDLESS_SSH_READY="false"

log() {
  printf '\n==> %s\n' "$1"
}

require_command() {
  local cmd="$1"
  local hint="$2"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing command: $cmd" >&2
    echo "$hint" >&2
    exit 1
  fi
}

xml_escape() {
  printf '%s' "$1" \
    | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g'
}

backup_file() {
  local file="$1"
  if [ -f "$file" ] && [ ! -f "$file.cs523.bak" ]; then
    cp "$file" "$file.cs523.bak"
  fi
}

install_hadoop() {
  require_command brew "Install Homebrew first: https://brew.sh"

  if ! brew list hadoop >/dev/null 2>&1; then
    log "Installing Hadoop with Homebrew"
    brew install hadoop
  else
    log "Hadoop is already installed by Homebrew"
  fi
}

detect_hadoop_home() {
  HADOOP_HOME="${HADOOP_HOME:-$(brew --prefix hadoop)/libexec}"
  HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

  if [ ! -x "$HADOOP_HOME/bin/hdfs" ]; then
    echo "Could not find hdfs under HADOOP_HOME=$HADOOP_HOME" >&2
    exit 1
  fi

  export HADOOP_HOME HADOOP_CONF_DIR
  export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
}

detect_java_home() {
  if [ -z "${JAVA_HOME:-}" ]; then
    if /usr/libexec/java_home -v 17 >/dev/null 2>&1; then
      JAVA_HOME="$(/usr/libexec/java_home -v 17)"
    else
      JAVA_HOME="$(/usr/libexec/java_home)"
    fi
    export JAVA_HOME
  fi
}

write_hadoop_config() {
  local core_site="$HADOOP_CONF_DIR/core-site.xml"
  local hdfs_site="$HADOOP_CONF_DIR/hdfs-site.xml"
  local hadoop_env="$HADOOP_CONF_DIR/hadoop-env.sh"
  local name_dir="$HADOOP_DATA_ROOT/name"
  local data_dir="$HADOOP_DATA_ROOT/data"
  local tmp_dir="$HADOOP_DATA_ROOT/tmp"
  local logs_dir="$HADOOP_DATA_ROOT/logs"

  mkdir -p "$name_dir" "$data_dir" "$tmp_dir" "$logs_dir"

  backup_file "$core_site"
  backup_file "$hdfs_site"
  backup_file "$hadoop_env"

  log "Writing Hadoop config in $HADOOP_CONF_DIR"
  cat > "$core_site" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$(xml_escape "$HDFS_HOST"):$HDFS_PORT</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>$(xml_escape "file://$tmp_dir")</value>
  </property>
</configuration>
EOF

  cat > "$hdfs_site" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>$(xml_escape "file://$name_dir")</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>$(xml_escape "file://$data_dir")</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>$(xml_escape "$HDFS_HOST"):$HDFS_WEB_PORT</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

  if grep -q '^export JAVA_HOME=' "$hadoop_env"; then
    sed -i '' "s|^export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" "$hadoop_env"
  else
    printf '\nexport JAVA_HOME=%s\n' "$JAVA_HOME" >> "$hadoop_env"
  fi

  if grep -q '^export HADOOP_LOG_DIR=' "$hadoop_env"; then
    sed -i '' "s|^export HADOOP_LOG_DIR=.*|export HADOOP_LOG_DIR=$logs_dir|" "$hadoop_env"
  else
    printf 'export HADOOP_LOG_DIR=%s\n' "$logs_dir" >> "$hadoop_env"
  fi
}

ensure_ssh_localhost() {
  log "Checking passwordless ssh localhost"
  mkdir -p "$HOME/.ssh"
  chmod 700 "$HOME/.ssh"

  if [ ! -f "$HOME/.ssh/id_rsa" ]; then
    ssh-keygen -t rsa -P '' -f "$HOME/.ssh/id_rsa"
  fi

  touch "$HOME/.ssh/authorized_keys"
  chmod 600 "$HOME/.ssh/authorized_keys"

  if ! grep -qF "$(cat "$HOME/.ssh/id_rsa.pub")" "$HOME/.ssh/authorized_keys"; then
    cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
  fi

  if ! ssh -o BatchMode=yes -o ConnectTimeout=5 localhost true >/dev/null 2>&1; then
    PASSWORDLESS_SSH_READY="false"
    cat <<EOF

Passwordless ssh localhost is not ready. The script will use direct Hadoop daemon startup instead.

If you prefer start-dfs.sh, enable:
  System Settings -> General -> Sharing -> Remote Login -> On

EOF
    return 1
  fi

  PASSWORDLESS_SSH_READY="true"
  return 0
}

format_namenode_if_needed() {
  local version_file="$HADOOP_DATA_ROOT/name/current/VERSION"

  if [ "$FORCE_HDFS_FORMAT" = "true" ]; then
    log "Formatting HDFS NameNode because FORCE_HDFS_FORMAT=true"
    hdfs namenode -format -force -nonInteractive
    return
  fi

  if [ ! -f "$version_file" ]; then
    log "Formatting HDFS NameNode for first use"
    hdfs namenode -format -nonInteractive
  else
    log "HDFS NameNode already formatted; skipping format"
  fi
}

start_hdfs() {
  if [ "$START_HDFS" != "true" ]; then
    log "Skipping HDFS startup because START_HDFS=$START_HDFS"
    return
  fi

  if [ "$HDFS_START_MODE" = "ssh" ]; then
    if [ "$PASSWORDLESS_SSH_READY" != "true" ]; then
      echo "HDFS_START_MODE=ssh requires passwordless ssh localhost." >&2
      exit 1
    fi
    log "Starting HDFS daemons with start-dfs.sh"
    start-dfs.sh
    return
  fi

  if [ "$HDFS_START_MODE" = "auto" ] && [ "$PASSWORDLESS_SSH_READY" = "true" ]; then
    log "Starting HDFS daemons with start-dfs.sh"
    start-dfs.sh
    return
  fi

  if [ "$HDFS_START_MODE" != "auto" ] && [ "$HDFS_START_MODE" != "daemon" ]; then
    echo "Invalid HDFS_START_MODE=$HDFS_START_MODE. Use auto, ssh, or daemon." >&2
    exit 1
  fi

  log "Starting HDFS daemons directly without ssh"
  hdfs --daemon start namenode
  hdfs --daemon start datanode
  hdfs --daemon start secondarynamenode
}

upload_metadata() {
  log "Uploading metadata CSV to HDFS"
  hdfs dfs -mkdir -p "$HDFS_PROJECT_DIR"
  hdfs dfs -put -f "$LOCAL_METADATA_CSV" "$HDFS_METADATA_PATH"
  hdfs dfs -ls "$HDFS_PROJECT_DIR"
}

print_next_steps() {
  local hdfs_uri="hdfs://$HDFS_HOST:$HDFS_PORT$HDFS_METADATA_PATH"

  cat <<EOF

HDFS setup finished.

Use this for the Spark enrichment bonus:

  export METADATA_CSV="$hdfs_uri"
  export CHECKPOINT_DIR="$ROOT/checkpoints/streaming-hdfs"
  ./scripts/spark-submit-local.sh

Useful checks:

  hdfs dfs -ls $HDFS_PROJECT_DIR
  jps
  open http://$HDFS_HOST:$HDFS_WEB_PORT

Stop HDFS:

  stop-dfs.sh

If stop-dfs.sh cannot use ssh localhost:

  hdfs --daemon stop secondarynamenode
  hdfs --daemon stop datanode
  hdfs --daemon stop namenode

To reformat the local HDFS data directory later:

  FORCE_HDFS_FORMAT=true ./scripts/setup-local-hdfs-macos.sh

EOF
}

main() {
  if [ "$(uname -s)" != "Darwin" ]; then
    echo "This helper is intended for macOS. For Linux, install Hadoop and use the same XML values manually." >&2
    exit 1
  fi

  install_hadoop
  detect_hadoop_home
  detect_java_home
  write_hadoop_config
  ensure_ssh_localhost || true
  format_namenode_if_needed
  start_hdfs
  upload_metadata
  print_next_steps
}

main "$@"
