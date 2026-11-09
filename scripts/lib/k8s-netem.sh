#!/usr/bin/env bash
# Helpers: apply / clear Linux netem or iptables partition inside a bookie pod netns
# via crictl + nsenter on the Colima / k3s node.
#
# Requires: kubectl, colima, python3, host tc/iptables (present on Colima VM).

netem_pod_ctr() {
  local ns=$1 pod=$2
  colima ssh -- bash -lc "NS=$(printf %q "$ns") POD=$(printf %q "$pod") python3 - <<'PY'
import json, os, subprocess, sys
ns, pod = os.environ['NS'], os.environ['POD']
out = subprocess.check_output(['sudo','crictl','ps','-a','-o','json'], text=True)
data = json.loads(out)
for c in data.get('containers', []):
    labels = c.get('labels') or {}
    if labels.get('io.kubernetes.pod.namespace') == ns and labels.get('io.kubernetes.pod.name') == pod:
        if labels.get('io.kubernetes.container.name') == 'openledgerd' and c.get('state') == 'CONTAINER_RUNNING':
            print(c['id'])
            raise SystemExit(0)
raise SystemExit(f'running openledgerd not found for {ns}/{pod}')
PY"
}

netem_pod_pid() {
  local ctr=$1
  colima ssh -- bash -lc "CTR=$(printf %q "$ctr"); sudo crictl inspect \"\$CTR\" | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"info\"][\"pid\"])'"
}

# Apply delay on the pod's eth0 (C07).
netem_apply_delay() {
  local ns=$1 pod=$2 delay=${3:-100ms}
  local ctr pid
  ctr=$(netem_pod_ctr "$ns" "$pod")
  [[ -n "$ctr" ]] || { echo "netem: no container for $ns/$pod" >&2; return 1; }
  pid=$(netem_pod_pid "$ctr")
  [[ -n "$pid" ]] || { echo "netem: no pid for $ctr" >&2; return 1; }
  colima ssh -- bash -lc "sudo nsenter -t $(printf %q "$pid") -n tc qdisc replace dev eth0 root netem delay $(printf %q "$delay")"
  echo "netem delay=$delay applied to $ns/$pod (pid=$pid)"
}

# Soft partition: drop all eth0 traffic (C12). Keeps lo so local sidecar↔bookie still works.
netem_apply_partition() {
  local ns=$1 pod=$2
  local ctr pid
  ctr=$(netem_pod_ctr "$ns" "$pod")
  [[ -n "$ctr" ]] || { echo "netem: no container for $ns/$pod" >&2; return 1; }
  pid=$(netem_pod_pid "$ctr")
  [[ -n "$pid" ]] || { echo "netem: no pid for $ctr" >&2; return 1; }
  colima ssh -- bash -lc "
set -euo pipefail
sudo nsenter -t $(printf %q "$pid") -n iptables -C INPUT -i eth0 -j DROP 2>/dev/null || sudo nsenter -t $(printf %q "$pid") -n iptables -I INPUT -i eth0 -j DROP
sudo nsenter -t $(printf %q "$pid") -n iptables -C OUTPUT -o eth0 -j DROP 2>/dev/null || sudo nsenter -t $(printf %q "$pid") -n iptables -I OUTPUT -o eth0 -j DROP
"
  echo "iptables eth0 DROP applied to $ns/$pod (pid=$pid)"
}

netem_clear() {
  local ns=$1 pod=$2
  local ctr pid
  ctr=$(netem_pod_ctr "$ns" "$pod") || return 0
  [[ -n "$ctr" ]] || return 0
  pid=$(netem_pod_pid "$ctr") || return 0
  [[ -n "$pid" ]] || return 0
  colima ssh -- bash -lc "
set +e
sudo nsenter -t $(printf %q "$pid") -n tc qdisc del dev eth0 root >/dev/null 2>&1
sudo nsenter -t $(printf %q "$pid") -n iptables -D INPUT -i eth0 -j DROP >/dev/null 2>&1
sudo nsenter -t $(printf %q "$pid") -n iptables -D OUTPUT -o eth0 -j DROP >/dev/null 2>&1
true
"
  echo "netem/iptables cleared on $ns/$pod"
}
