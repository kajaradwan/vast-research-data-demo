#!/usr/bin/env bash
# Read-only preflight: checks the cluster, the VIP range, names, features and the client. Changes nothing.
cd "$(dirname "$0")"
PY=.venv/bin/python; [ -x $PY ] || { python3 -m venv .venv && .venv/bin/pip install -q vastpy requests; }
exec $PY mgmt/preflight.py
