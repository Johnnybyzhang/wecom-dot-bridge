#!/bin/sh
# Create, or verify, the isolated dot-bridge Incus environment on an Incus host.
#
# Each object is created in ONE API call from a complete JSON spec (JSON is YAML, which
# the Incus 6.0 `create` commands read from stdin), so an interrupted run cannot leave a
# half-configured object. Every object, new or pre-existing, is then compared with its
# spec, and the script aborts on any mismatch rather than trusting a name.
# Only these objects are created: ACL dot-egress, network dot0, project dot, and inside
# that project the profile dot-bridge, volume dot-state and instance dot-bridge.
# Nothing else is modified: no other project, network, ACL, the pool layout, or host
# boot/update settings.
# The container uses an isolated ID map. This needs a free 65536-ID block in the host's
# root subordinate-ID pool (/etc/subuid, /etc/subgid): enlarge the pool by one block if
# other isolated containers use it all, then restart only the Incus daemon. A
# non-isolated container must NOT be used instead: it receives the whole pool and so
# overlaps the isolated maps of other containers.
# Run on the host:  sudo sh incus-create.sh      (requires incus and jq)
set -eu

PROJECT=dot
POOL=agents
INSTANCE=dot-bridge
IMAGE=images:debian/13
MARK="managed-by=agentbox-dot-bridge"

die() { echo "ABORT: $*" >&2; exit 1; }
exists() { incus query "$1" >/dev/null 2>&1 </dev/null; }

ACL_SPEC='{"description":"dot-bridge egress: public HTTP(S) and approved DNS only ('"$MARK"')",
 "egress":[
  {"action":"drop","destination":"10.0.0.0/8","description":"Deny RFC1918 10/8","state":"enabled"},
  {"action":"drop","destination":"172.16.0.0/12","description":"Deny RFC1918 172.16/12","state":"enabled"},
  {"action":"drop","destination":"192.168.0.0/16","description":"Deny RFC1918 192.168/16","state":"enabled"},
  {"action":"drop","destination":"100.64.0.0/10","description":"Deny shared address space","state":"enabled"},
  {"action":"drop","destination":"169.254.0.0/16","description":"Deny link local and metadata","state":"enabled"},
  {"action":"drop","destination":"224.0.0.0/4","description":"Deny multicast","state":"enabled"},
  {"action":"drop","destination":"240.0.0.0/4","description":"Deny reserved","state":"enabled"},
  {"action":"allow","protocol":"tcp","destination_port":"80,443","description":"Public HTTP(S)","state":"enabled"},
  {"action":"allow","destination":"1.1.1.1,223.5.5.5","protocol":"udp","destination_port":"53","description":"DNS over UDP","state":"enabled"},
  {"action":"allow","destination":"1.1.1.1,223.5.5.5","protocol":"tcp","destination_port":"53","description":"DNS over TCP","state":"enabled"}],
 "ingress":[], "config":{}}'

NETWORK_SPEC='{"description":"dot-bridge isolated bridge ('"$MARK"')",
 "config":{"ipv4.address":"10.251.0.1/24","ipv4.nat":"true","ipv6.address":"none",
  "dns.domain":"dot.agentbox.internal","security.acls":"dot-egress",
  "security.acls.default.egress.action":"reject","security.acls.default.ingress.action":"reject"}}'

PROJECT_SPEC='{"description":"dot-bridge ('"$MARK"')",
 "config":{"features.images":"false","features.networks":"false","features.networks.zones":"false",
  "features.profiles":"true","features.storage.volumes":"true","features.storage.buckets":"false",
  "restricted":"true","restricted.containers.privilege":"isolated",
  "restricted.containers.nesting":"block","restricted.containers.lowlevel":"block",
  "restricted.devices.disk":"managed","restricted.devices.nic":"managed",
  "restricted.devices.proxy":"block","restricted.networks.access":"dot0",
  "restricted.snapshots":"allow","restricted.backups":"allow",
  "limits.containers":"1","limits.virtual-machines":"0","limits.cpu":"2",
  "limits.memory":"1GiB","limits.processes":"1024","limits.disk.pool.agents":"20GiB"}}'

VOLUME_SPEC='{"description":"dot-bridge state ('"$MARK"')",
 "config":{"size":"2GiB","snapshots.schedule":"0 3 * * *","snapshots.expiry":"30d"}}'

PROFILE_SPEC='{"description":"dot-bridge container ('"$MARK"')",
 "config":{"limits.cpu":"1","limits.memory":"512MiB","limits.processes":"512",
  "security.idmap.isolated":"true","security.nesting":"false","security.privileged":"false"},
 "devices":{
  "eth0":{"type":"nic","name":"eth0","network":"dot0"},
  "root":{"type":"disk","path":"/","pool":"agents","size":"8GiB"},
  "state":{"type":"disk","path":"/var/lib/dot-bridge","pool":"agents","source":"dot-state"}}}'

# Compare a live object with its spec: the ownership marker in the description, every
# spec config key, ACL rules exactly, profile devices exactly, plus an optional extra check.
verify() { # api-path spec [extra-jq-condition]
  live=$(incus query "$1" </dev/null) || die "cannot read $1"
  printf '%s' "$live" | jq -e --argjson s "$2" '.description == $s.description' >/dev/null \
    || die "$1 exists but is not managed by dot-bridge (description differs); not touching it"
  printf '%s' "$live" | jq -e --argjson s "$2" '
    # Every rule field except the free-text description must match exactly
    # (source, source_port, icmp_type, icmp_code, ... included); empty values are dropped.
    def rules: map(del(.description) | with_entries(select(.value != "" and .value != null)));
    . as $o
    | ((($s.config // {}) | to_entries) | all(. as $e | $o.config[$e.key] == $e.value))
      and (if $s.egress then (($o.egress // []) | rules) == ($s.egress | rules)
                             and (($o.ingress // []) | length) == 0 else true end)
      and (if $s.devices then $o.devices == $s.devices else true end)' >/dev/null \
    || die "$1 configuration differs from spec"
  if [ -n "${3:-}" ]; then
    printf '%s' "$live" | jq -e "$3" >/dev/null || die "$1 failed check: $3"
  fi
  echo "verified $1"
}

command -v jq >/dev/null || die "jq is required"
[ "$(sed -n 's/^root:[0-9]*://p' /etc/subuid)" -ge 131072 ] 2>/dev/null \
  || die "/etc/subuid has no free 65536-ID block for an isolated map (see header)"
exists "/1.0/storage-pools/$POOL" || die "storage pool $POOL not found"
if ! exists "/1.0/networks/dot0?project=default" && ip -4 route | grep -q '^10\.251\.0\.'; then
  die "10.251.0.0/24 is already routed on this host"
fi

exists "/1.0/network-acls/dot-egress?project=default" || \
  printf '%s' "$ACL_SPEC" | incus network acl create dot-egress --project default
verify "/1.0/network-acls/dot-egress?project=default" "$ACL_SPEC"

exists "/1.0/networks/dot0?project=default" || \
  printf '%s' "$NETWORK_SPEC" | incus network create dot0 --type bridge --project default
verify "/1.0/networks/dot0?project=default" "$NETWORK_SPEC" '.managed == true and .type == "bridge"'

exists "/1.0/projects/$PROJECT" || \
  printf '%s' "$PROJECT_SPEC" | incus project create "$PROJECT"
verify "/1.0/projects/$PROJECT" "$PROJECT_SPEC"

exists "/1.0/storage-pools/$POOL/volumes/custom/dot-state?project=$PROJECT" || \
  printf '%s' "$VOLUME_SPEC" | incus storage volume create "$POOL" dot-state --project "$PROJECT"
verify "/1.0/storage-pools/$POOL/volumes/custom/dot-state?project=$PROJECT" "$VOLUME_SPEC"

exists "/1.0/profiles/dot-bridge?project=$PROJECT" || \
  printf '%s' "$PROFILE_SPEC" | incus profile create dot-bridge --project "$PROJECT"
verify "/1.0/profiles/dot-bridge?project=$PROJECT" "$PROFILE_SPEC"

if ! exists "/1.0/instances/$INSTANCE?project=$PROJECT"; then
  incus launch "$IMAGE" "$INSTANCE" --project "$PROJECT" --profile dot-bridge </dev/null
fi
# The instance must use only our profile and add no devices or config of its own.
incus query "/1.0/instances/$INSTANCE?project=$PROJECT" </dev/null | jq -e --argjson p "$PROFILE_SPEC" '
  .profiles == ["dot-bridge"] and .devices == {} and .expanded_devices == $p.devices
  and ([.config | keys[] | select(startswith("volatile.") or startswith("image.") | not)] == [])
  and .expanded_config["security.privileged"] == "false"
  and .expanded_config["security.nesting"] == "false"
  and .expanded_config["security.idmap.isolated"] == "true"' >/dev/null \
  || die "instance $INSTANCE does not match the dot-bridge profile"
echo "verified /1.0/instances/$INSTANCE?project=$PROJECT"
