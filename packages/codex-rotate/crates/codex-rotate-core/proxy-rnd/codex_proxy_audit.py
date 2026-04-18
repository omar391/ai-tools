#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import random
import re
import shlex
import signal
import socket
import string
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


HTTPBIN_ANYTHING_URL = "https://httpbin.org/anything"
HTTPBIN_IP_URL = "https://httpbin.org/ip"
IPIFY_URL = "https://api.ipify.org?format=json"
POSTMAN_ECHO_URL = "https://postman-echo.com/get"
DEFAULT_APP_BINARY = "/Applications/Codex.app/Contents/MacOS/Codex"
DEFAULT_CLI_COMMAND = 'codex exec "Reply exactly: proxy-audit-ok"'
DEFAULT_METADATA_ENDPOINTS = [
    ("httpbin-anything", HTTPBIN_ANYTHING_URL),
    ("httpbin-ip", HTTPBIN_IP_URL),
    ("postman-echo", POSTMAN_ECHO_URL),
    ("ipify", IPIFY_URL),
]
SUSPICIOUS_HEADER_NAMES = {
    "client-ip",
    "forwarded",
    "proxy-authorization",
    "true-client-ip",
    "via",
    "x-client-ip",
    "x-forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
}
APP_MATCHERS = [
    re.compile(r"/Applications/Codex\.app/Contents/MacOS/Codex\b"),
    re.compile(r"/Applications/Codex\.app/Contents/Frameworks/Codex Helper"),
    re.compile(r"/Applications/Codex\.app/Contents/Resources/codex\b"),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_token() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "proxy-audit-" + "".join(random.choice(alphabet) for _ in range(12))


def run_command(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
        timeout=timeout,
        check=check,
    )


def safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def normalize_headers(raw_headers: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(raw_headers, dict):
        return {}
    normalized: dict[str, str] = {}
    for key, value in raw_headers.items():
        normalized[str(key).lower()] = str(value)
    return normalized


def flatten_public_request_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    if "headers" in payload or "origin" in payload:
        return payload

    if "args" in payload and "headers" in payload:
        return payload

    if "headers" in payload:
        return payload

    if "data" in payload and isinstance(payload["data"], dict):
        return payload["data"]

    return payload


def parse_proxy_url(proxy_url: str) -> tuple[str, int]:
    parsed = urlparse(proxy_url)
    if not parsed.scheme or not parsed.hostname or parsed.port is None:
        raise ValueError(f"Invalid proxy URL: {proxy_url}")
    return parsed.hostname, parsed.port


def resolve_host(host: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return []

    resolved = []
    seen = set()
    for info in infos:
        ip = info[4][0]
        if ip not in seen:
            seen.add(ip)
            resolved.append(ip)
    return resolved


def normalize_ip(host: str) -> str | None:
    candidate = host.strip()
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return None


def is_loopback(host: str) -> bool:
    ip = normalize_ip(host)
    if ip is None:
        return host in {"localhost", "*"}
    return ipaddress.ip_address(ip).is_loopback


def split_host_port(endpoint: str) -> tuple[str | None, int | None]:
    endpoint = endpoint.strip()
    if not endpoint:
        return None, None

    if endpoint.startswith("["):
        end = endpoint.find("]")
        if end == -1:
            return endpoint, None
        host = endpoint[1:end]
        rest = endpoint[end + 1 :]
        if rest.startswith(":"):
            rest = rest[1:]
        try:
            return host, int(rest)
        except ValueError:
            return host, None

    if endpoint.count(":") >= 2 and endpoint.rsplit(":", 1)[1].isdigit():
        host, port = endpoint.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            return host, None

    if ":" not in endpoint:
        return endpoint, None

    host, port = endpoint.rsplit(":", 1)
    try:
        return host, int(port)
    except ValueError:
        return host, None


def parse_lsof_name(name: str) -> dict[str, Any]:
    cleaned = name.strip()
    protocol = None
    state = None

    if cleaned.startswith("TCP "):
        protocol = "TCP"
        cleaned = cleaned[4:]
    elif cleaned.startswith("UDP "):
        protocol = "UDP"
        cleaned = cleaned[4:]

    state_match = re.search(r"\(([^)]+)\)\s*$", cleaned)
    if state_match:
        state = state_match.group(1)
        cleaned = cleaned[: state_match.start()].strip()

    local_host = local_port = remote_host = remote_port = None
    if "->" in cleaned:
        local_part, remote_part = cleaned.split("->", 1)
        local_host, local_port = split_host_port(local_part)
        remote_host, remote_port = split_host_port(remote_part)
    else:
        local_host, local_port = split_host_port(cleaned)

    return {
        "protocol": protocol,
        "state": state,
        "local_host": local_host,
        "local_port": local_port,
        "remote_host": remote_host,
        "remote_port": remote_port,
        "raw_name": name,
    }


def parse_lsof_output(text: str) -> list[dict[str, Any]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) <= 1:
        return []

    entries: list[dict[str, Any]] = []
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        entry = {
            "command": parts[0],
            "pid": int(parts[1]),
            "user": parts[2],
            "fd": parts[3],
            "type": parts[4],
            "device": parts[5],
            "size_off": parts[6],
            "node": parts[7],
            "name": " ".join(parts[8:]),
        }
        entry.update(parse_lsof_name(entry["name"]))
        entries.append(entry)
    return entries


def current_process_table() -> list[tuple[int, int, str]]:
    result = run_command(["ps", "-axo", "pid=,ppid=,command="], timeout=15, check=True)
    rows = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        rows.append((int(parts[0]), int(parts[1]), parts[2]))
    return rows


def descendants_of(root_pids: set[int]) -> set[int]:
    rows = current_process_table()
    children: dict[int, set[int]] = {}
    for pid, ppid, _command in rows:
        children.setdefault(ppid, set()).add(pid)

    seen = set(root_pids)
    queue = list(root_pids)
    while queue:
        current = queue.pop(0)
        for child in children.get(current, set()):
            if child not in seen:
                seen.add(child)
                queue.append(child)
    return seen


def matching_app_processes() -> dict[int, str]:
    matches: dict[int, str] = {}
    for pid, _ppid, command in current_process_table():
        if any(pattern.search(command) for pattern in APP_MATCHERS):
            matches[pid] = command
    return matches


def sample_connections_for_pids(pids: set[int]) -> list[dict[str, Any]]:
    if not pids:
        return []
    pid_arg = ",".join(str(pid) for pid in sorted(pids))
    result = run_command(
        ["lsof", "-nP", "-a", "-p", pid_arg, "-i"],
        timeout=20,
    )
    if result.returncode not in {0, 1}:
        return []
    return parse_lsof_output(result.stdout)


def classify_connection(
    entry: dict[str, Any],
    *,
    allowed_proxy_ips: set[str],
    proxy_port: int,
) -> dict[str, Any]:
    remote_host = entry.get("remote_host")
    remote_port = entry.get("remote_port")
    protocol = entry.get("protocol")
    local_host = entry.get("local_host")
    issues: list[str] = []

    if remote_host is None:
        if protocol == "UDP" and local_host not in {None, "*"} and not is_loopback(str(local_host)):
            issues.append("udp-bound-non-loopback")
        return {"status": "pass", "issues": issues}

    normalized_remote_ip = normalize_ip(str(remote_host))
    normalized_local_ip = normalize_ip(str(local_host)) if local_host else None

    if normalized_remote_ip in allowed_proxy_ips and remote_port == proxy_port:
        return {"status": "pass", "issues": issues}

    if remote_host and is_loopback(str(remote_host)):
        return {"status": "pass", "issues": issues}

    if protocol == "TCP":
        issues.append("direct-remote-tcp")
    elif protocol == "UDP":
        issues.append("direct-remote-udp")
    else:
        issues.append("direct-remote-unknown")

    if normalized_remote_ip:
        try:
            remote_ip = ipaddress.ip_address(normalized_remote_ip)
            if remote_ip.is_private:
                issues.append("remote-private-address")
        except ValueError:
            pass

    if normalized_local_ip and not ipaddress.ip_address(normalized_local_ip).is_loopback:
        issues.append("non-loopback-local-bind")

    return {"status": "fail", "issues": issues}


def monitor_pid_set(
    *,
    pid_supplier,
    duration_seconds: int,
    poll_interval_seconds: float,
    allowed_proxy_ips: set[str],
    proxy_port: int,
) -> dict[str, Any]:
    started_at = time.time()
    samples: list[dict[str, Any]] = []
    seen_failures: dict[str, dict[str, Any]] = {}
    seen_pids: set[int] = set()

    while True:
        now = time.time()
        if now - started_at > duration_seconds:
            break

        pids = set(pid_supplier())
        seen_pids.update(pids)
        connections = sample_connections_for_pids(pids)
        failures_this_sample = []
        for entry in connections:
            verdict = classify_connection(
                entry,
                allowed_proxy_ips=allowed_proxy_ips,
                proxy_port=proxy_port,
            )
            if verdict["status"] == "fail":
                key = f'{entry["pid"]}:{entry["protocol"]}:{entry["raw_name"]}'
                failure = {
                    "pid": entry["pid"],
                    "command": entry["command"],
                    "protocol": entry["protocol"],
                    "local_host": entry["local_host"],
                    "local_port": entry["local_port"],
                    "remote_host": entry["remote_host"],
                    "remote_port": entry["remote_port"],
                    "issues": verdict["issues"],
                    "raw_name": entry["raw_name"],
                }
                seen_failures.setdefault(key, failure)
                failures_this_sample.append(failure)

        samples.append(
            {
                "captured_at": utc_now(),
                "pids": sorted(pids),
                "connection_count": len(connections),
                "failures": failures_this_sample,
            }
        )
        time.sleep(poll_interval_seconds)

    return {
        "samples": samples,
        "observed_pids": sorted(seen_pids),
        "suspicious_connections": list(seen_failures.values()),
        "passed": not seen_failures,
    }


def curl_fetch(
    *,
    url: str,
    proxy_url: str | None,
    token: str,
    max_time: int,
) -> dict[str, Any]:
    cmd = [
        "curl",
        "-sS",
        "--location",
        "--max-time",
        str(max_time),
        "-H",
        f"User-Agent: codex-proxy-audit/1.0 ({token})",
        "-H",
        f"X-Proxy-Audit-Token: {token}",
        url,
    ]
    if proxy_url:
        cmd.extend(["--proxy", proxy_url])

    result = run_command(cmd, timeout=max_time + 10)
    payload = safe_json_loads(result.stdout)
    return {
        "command": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "json": payload,
    }


def inspect_metadata_payload(label: str, payload: dict[str, Any], direct_origin: str | None) -> dict[str, Any]:
    body = flatten_public_request_payload(payload.get("json"))
    headers = normalize_headers(body.get("headers"))
    origin = body.get("origin")
    if origin is None and isinstance(body.get("ip"), str):
        origin = body["ip"]

    suspicious_headers = sorted(name for name in headers if name in SUSPICIOUS_HEADER_NAMES)
    issues: list[str] = []
    warnings: list[str] = []

    if payload["returncode"] != 0:
        issues.append("request-failed")

    if suspicious_headers:
        issues.append("proxy-added-public-metadata-headers")

    if isinstance(origin, str) and "," in origin:
        issues.append("multiple-origin-addresses-observed")

    if isinstance(origin, str) and direct_origin and origin == direct_origin:
        warnings.append("proxy-origin-matches-direct-origin")

    if payload["returncode"] == 0 and headers.get("x-proxy-audit-token") is None and label != "ipify":
        warnings.append("echo-service-did-not-reflect-audit-token")

    return {
        "origin": origin,
        "headers": headers,
        "suspicious_headers": suspicious_headers,
        "issues": issues,
        "warnings": warnings,
        "passed": not issues,
    }


@dataclass
class PhaseResult:
    name: str
    passed: bool
    details: dict[str, Any]


def run_metadata_phase(args: argparse.Namespace) -> PhaseResult:
    token = make_token()
    endpoint_results = []
    direct_origin_for_compare: str | None = None

    for label, url in DEFAULT_METADATA_ENDPOINTS:
        direct = None
        if args.direct_baseline:
            direct = curl_fetch(url=url, proxy_url=None, token=token, max_time=args.metadata_timeout)
            direct_check = inspect_metadata_payload(label, direct, None)
            if isinstance(direct_check["origin"], str) and direct_origin_for_compare is None:
                direct_origin_for_compare = direct_check["origin"]

        proxied = curl_fetch(url=url, proxy_url=args.proxy_url, token=token, max_time=args.metadata_timeout)
        proxied_check = inspect_metadata_payload(label, proxied, direct_origin_for_compare)

        endpoint_results.append(
            {
                "label": label,
                "url": url,
                "direct": {
                    "executed": bool(args.direct_baseline),
                    "returncode": None if direct is None else direct["returncode"],
                    "stderr": None if direct is None else direct["stderr"],
                    "inspection": None if direct is None else inspect_metadata_payload(label, direct, None),
                },
                "proxied": {
                    "returncode": proxied["returncode"],
                    "stderr": proxied["stderr"],
                    "inspection": proxied_check,
                },
            }
        )

    passed = all(item["proxied"]["inspection"]["passed"] for item in endpoint_results)
    return PhaseResult(
        name="metadata",
        passed=passed,
        details={
            "token": token,
            "direct_baseline": bool(args.direct_baseline),
            "endpoints": endpoint_results,
        },
    )


def build_proxy_env(proxy_url: str) -> dict[str, str]:
    env = os.environ.copy()
    env["ALL_PROXY"] = proxy_url
    env["HTTP_PROXY"] = proxy_url
    env["HTTPS_PROXY"] = proxy_url
    env["NO_PROXY"] = "localhost,127.0.0.1,::1"
    env["all_proxy"] = proxy_url
    env["http_proxy"] = proxy_url
    env["https_proxy"] = proxy_url
    env["no_proxy"] = env["NO_PROXY"]
    return env


def terminate_process_tree(root_pid: int) -> None:
    try:
        pids = descendants_of({root_pid})
    except Exception:
        pids = {root_pid}

    for pid in sorted(pids, reverse=True):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue

    time.sleep(2)

    for pid in sorted(pids, reverse=True):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            continue


def run_cli_phase(args: argparse.Namespace, proxy_ips: set[str], proxy_port: int) -> PhaseResult:
    env = build_proxy_env(args.proxy_url)
    process = subprocess.Popen(
        ["/bin/zsh", "-lc", args.cli_command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        cwd=args.cwd,
    )

    def cli_pid_supplier() -> set[int]:
        if process.poll() is None:
            return descendants_of({process.pid})
        return descendants_of({process.pid}) - {process.pid}

    monitor = monitor_pid_set(
        pid_supplier=cli_pid_supplier,
        duration_seconds=args.cli_timeout,
        poll_interval_seconds=args.poll_interval,
        allowed_proxy_ips=proxy_ips,
        proxy_port=proxy_port,
    )

    timed_out = False
    if process.poll() is None:
        timed_out = True
        terminate_process_tree(process.pid)

    stdout, stderr = process.communicate(timeout=30)
    passed = process.returncode == 0 and monitor["passed"] and not timed_out
    return PhaseResult(
        name="cli",
        passed=passed,
        details={
            "command": args.cli_command,
            "returncode": process.returncode,
            "timed_out_and_killed": timed_out,
            "stdout": stdout[-4000:],
            "stderr": stderr[-4000:],
            "monitor": monitor,
        },
    )


def run_app_phase(args: argparse.Namespace, proxy_ips: set[str], proxy_port: int) -> PhaseResult:
    app_binary = Path(args.app_binary)
    if not app_binary.exists():
        return PhaseResult(
            name="app",
            passed=False,
            details={
                "error": f"App binary not found: {app_binary}",
            },
        )

    user_data_dir = args.app_user_data_dir
    if user_data_dir is None:
        user_data_dir = f"/tmp/codex-proxy-audit-app-{make_token()}"

    Path(user_data_dir).mkdir(parents=True, exist_ok=True)

    existing = matching_app_processes()
    env = build_proxy_env(args.proxy_url)

    cmd = [
        str(app_binary),
        "--proxy-server=" + args.proxy_url_for_electron,
        "--proxy-bypass-list=<local>",
        "--user-data-dir=" + user_data_dir,
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        cwd=args.cwd,
    )

    time.sleep(args.app_warmup)

    launch_time = utc_now()

    def app_pid_supplier() -> set[int]:
        if process.poll() is None:
            return descendants_of({process.pid})
        return set()

    monitor = monitor_pid_set(
        pid_supplier=app_pid_supplier,
        duration_seconds=args.app_duration,
        poll_interval_seconds=args.poll_interval,
        allowed_proxy_ips=proxy_ips,
        proxy_port=proxy_port,
    )

    if args.close_launched_app:
        terminate_process_tree(process.pid)

    surviving = matching_app_processes()
    new_pids = sorted(set(surviving) - set(existing))
    passed = monitor["passed"]
    return PhaseResult(
        name="app",
        passed=passed,
        details={
            "binary": str(app_binary),
            "command": cmd,
            "user_data_dir": user_data_dir,
            "launch_started_at": launch_time,
            "existing_pids_before_launch": existing,
            "new_pids_after_launch": new_pids,
            "monitor": monitor,
            "close_launched_app": bool(args.close_launched_app),
        },
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit Codex Desktop and Codex CLI traffic through a SOCKS/HTTP proxy.",
    )
    parser.add_argument("--proxy-url", required=True, help="Proxy URL, e.g. socks5h://127.0.0.1:1080")
    parser.add_argument(
        "--proxy-url-for-electron",
        help="Electron proxy switch value. Defaults to --proxy-url with socks5h:// rewritten to socks5://.",
    )
    parser.add_argument("--cwd", default=os.getcwd(), help="Working directory used for launched processes.")
    parser.add_argument("--cli-command", default=DEFAULT_CLI_COMMAND, help="CLI command to run during the CLI phase.")
    parser.add_argument("--app-binary", default=DEFAULT_APP_BINARY, help="Path to the Codex Desktop executable.")
    parser.add_argument("--app-user-data-dir", help="Optional user data directory for the app phase. Defaults to a unique temp dir.")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Socket sampling interval in seconds.")
    parser.add_argument("--metadata-timeout", type=int, default=20, help="Per-request timeout for metadata probes.")
    parser.add_argument("--cli-timeout", type=int, default=90, help="How long to monitor the CLI phase.")
    parser.add_argument("--app-warmup", type=int, default=8, help="Seconds to wait after launching the app before monitoring.")
    parser.add_argument("--app-duration", type=int, default=60, help="How long to monitor the app phase.")
    parser.add_argument("--output-json", help="Optional path for the JSON report.")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip public echo-service checks.")
    parser.add_argument("--skip-cli", action="store_true", help="Skip the CLI phase.")
    parser.add_argument("--skip-app", action="store_true", help="Skip the desktop app phase.")
    parser.add_argument("--no-direct-baseline", dest="direct_baseline", action="store_false", help="Skip direct non-proxied metadata baseline requests.")
    parser.add_argument("--leave-app-open", dest="close_launched_app", action="store_false", help="Do not terminate the launched Codex app instance after the audit.")
    parser.set_defaults(direct_baseline=True, close_launched_app=True)
    return parser


def summarize_phase(phase: PhaseResult) -> list[str]:
    lines = [f"[{phase.name}] {'PASS' if phase.passed else 'FAIL'}"]
    if phase.name == "metadata":
        for endpoint in phase.details["endpoints"]:
            proxied = endpoint["proxied"]["inspection"]
            issues = proxied["issues"]
            warnings = proxied["warnings"]
            if issues:
                suffix = ", ".join(issues)
            elif warnings:
                suffix = "ok with warnings: " + ", ".join(warnings)
            else:
                suffix = "ok"
            lines.append(f"  - {endpoint['label']}: {suffix}")
    elif phase.name in {"cli", "app"}:
        monitor = phase.details.get("monitor", {})
        suspicious = monitor.get("suspicious_connections", [])
        lines.append(f"  - suspicious connections: {len(suspicious)}")
        for entry in suspicious[:8]:
            lines.append(
                "  - "
                + f'{entry["command"]} pid={entry["pid"]} '
                + f'{entry["protocol"]} {entry["local_host"]}:{entry["local_port"]}'
                + f' -> {entry["remote_host"]}:{entry["remote_port"]} '
                + f'[{",".join(entry["issues"])}]'
            )
        if phase.name == "cli":
            lines.append(f'  - return code: {phase.details["returncode"]}')
    return lines


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        proxy_host, proxy_port = parse_proxy_url(args.proxy_url)
    except ValueError as exc:
        parser.error(str(exc))

    if args.proxy_url_for_electron is None:
        args.proxy_url_for_electron = args.proxy_url.replace("socks5h://", "socks5://", 1)

    proxy_ips = set(resolve_host(proxy_host))
    normalized_proxy_host = normalize_ip(proxy_host)
    if normalized_proxy_host:
        proxy_ips.add(normalized_proxy_host)

    report: dict[str, Any] = {
        "generated_at": utc_now(),
        "proxy": {
            "url": args.proxy_url,
            "electron_url": args.proxy_url_for_electron,
            "host": proxy_host,
            "port": proxy_port,
            "resolved_ips": sorted(proxy_ips),
        },
        "phases": {},
    }

    phases: list[PhaseResult] = []

    if not args.skip_metadata:
        phases.append(run_metadata_phase(args))
    if not args.skip_cli:
        phases.append(run_cli_phase(args, proxy_ips, proxy_port))
    if not args.skip_app:
        phases.append(run_app_phase(args, proxy_ips, proxy_port))

    overall_pass = True
    for phase in phases:
        report["phases"][phase.name] = phase.details
        overall_pass = overall_pass and phase.passed

    report["overall_passed"] = overall_pass

    if args.output_json:
        output_path = Path(args.output_json)
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_path = Path(args.cwd) / f"proxy-audit-report-{timestamp}.json"

    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Proxy audit report written to {output_path}")
    print(f"Proxy endpoint: {proxy_host}:{proxy_port} ({', '.join(sorted(proxy_ips)) or 'unresolved'})")
    for phase in phases:
        for line in summarize_phase(phase):
            print(line)

    if overall_pass:
        print("OVERALL PASS")
        return 0

    print("OVERALL FAIL")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
