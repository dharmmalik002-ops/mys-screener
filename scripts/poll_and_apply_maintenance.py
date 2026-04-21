#!/usr/bin/env python3
"""Poll a Hugging Face Space health endpoint and POST the maintenance trigger.

Usage:
  python scripts/poll_and_apply_maintenance.py --space dharmmalik/stock-scanner-backend --token $MAINT_TOKEN

This uses only the Python standard library so it's safe to run in environments without extra deps.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def space_to_hostname(space: str) -> str:
    return space.replace("/", "-")


def poll_health(space: str, attempts: int = 30, wait: int = 10) -> bool:
    host = space_to_hostname(space)
    url = f"https://{host}.hf.space/api/health"
    for i in range(1, attempts + 1):
        try:
            req = Request(url)
            with urlopen(req, timeout=10) as resp:
                code = resp.getcode()
            print(f"Attempt {i}: {code}")
            if code == 200:
                return True
        except HTTPError as e:
            print(f"Attempt {i}: HTTPError {e.code}")
        except URLError as e:
            print(f"Attempt {i}: URLError {e}")
        except Exception as e:
            print(f"Attempt {i}: Exception {e}")
        time.sleep(wait)
    return False


def call_maintenance(space: str, token: str) -> dict | None:
    host = space_to_hostname(space)
    url = f"https://{host}.hf.space/api/maintenance/eod-refresh"
    data = b"{}"
    req = Request(url, method="POST", data=data, headers={"x-maintenance-token": token, "Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            try:
                return json.loads(body)
            except Exception:
                return {"raw": body}
    except HTTPError as e:
        print(f"HTTPError: {e.code} - {e.reason}")
        try:
            print(e.read().decode())
        except Exception:
            pass
    except URLError as e:
        print(f"URLError: {e}")
    return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--space", required=True, help="Hugging Face Spaces repo, e.g. owner/space-name")
    p.add_argument("--token", required=True, help="Maintenance token to send to the service (x-maintenance-token header)")
    p.add_argument("--attempts", type=int, default=30)
    p.add_argument("--wait", type=int, default=10)
    args = p.parse_args(argv)

    ok = poll_health(args.space, attempts=args.attempts, wait=args.wait)
    if not ok:
        print("Space did not become healthy in time")
        return 2

    print("Space healthy — calling maintenance endpoint...")
    resp = call_maintenance(args.space, args.token)
    print("Maintenance response:", resp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
