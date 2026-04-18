# Proxy Architecture

This document captures what we have established so far about routing Codex Desktop and Codex CLI through a local SOCKS proxy on macOS.

Current test proxy:

- `socks5h://127.0.0.1:9595`

Supporting tool:

- [`codex_proxy_audit.py`](./codex_proxy_audit.py)

## Scope

The goal is not just "does the app work through the proxy", but:

- does the proxy change public egress IP
- does the proxy expose public metadata such as forwarding headers
- does Codex Desktop keep all app traffic pinned to the proxy
- does Codex CLI keep all process traffic pinned to the proxy

## Codex Components

The current Codex desktop stack on this machine is multi-process:

- Electron app binary: `/Applications/Codex.app/Contents/MacOS/Codex`
- Electron network service: `Codex Helper --type=utility --utility-sub-type=network.mojom.NetworkService`
- Renderer helpers: `Codex Helper (Renderer)`
- Native sidecar: `/Applications/Codex.app/Contents/Resources/codex app-server`

This matters because proxy correctness must be evaluated across all of these processes, not just the visible Electron window.

## What We Verified

### 1. The SOCKS proxy changes egress IP

Public metadata probes showed direct traffic leaving from `45.248.151.x`, while proxied traffic left from `104.28.208.81` / `104.28.208.84`.

Established:

- the proxy is reachable
- the proxy is used successfully by curl-based traffic
- hostname resolution via `socks5h://` works as expected

### 2. The proxy itself is not yet proven to add unique public metadata

`postman-echo` reflected `x-forwarded-proto: https`, but the same header appeared in the direct baseline too.

Established:

- that specific header is not enough to claim the SOCKS proxy is leaking metadata
- the safer interpretation is that the echo service path itself adds or preserves that header

Current conclusion:

- the proxy appears acceptable for ordinary external HTTP egress tests
- we have not observed a proxy-only public metadata leak from the current `localhost:9595` SOCKS endpoint

### 3. A non-isolated Codex Desktop audit path produced false positives

When we audited the already-running Codex Desktop instance, we observed direct HTTPS connections from:

- Electron network service
- `codex app-server`

Direct destinations included:

- `104.18.37.228:443`
- `104.18.32.47:443`

This was later shown to be an artifact of the audit method:

- the original script did not pass a unique `--user-data-dir`
- it monitored all Codex processes on the machine, including the already-running session

### 4. A fresh isolated Codex Desktop launch can be proxy-pinned

Launching a second instance with explicit proxy flags and env vars produced a clean result over repeated socket samples.

Working launch pattern:

```sh
ALL_PROXY='socks5h://127.0.0.1:9595' \
HTTP_PROXY='socks5h://127.0.0.1:9595' \
HTTPS_PROXY='socks5h://127.0.0.1:9595' \
open -na /Applications/Codex.app --args \
  --user-data-dir=/tmp/codex-proxy-audit-open-profile \
  --proxy-server=socks5://127.0.0.1:9595 \
  --proxy-bypass-list='<local>'
```

Observed result for the fresh instance:

- the fresh Electron network service held only loopback connections to `127.0.0.1:9595`
- repeated `lsof` samples did not show direct remote sockets for that instance

Established:

- Codex Desktop can be launched in a proxy-contained way
- the proxy switch must be applied at process launch time
- a fresh profile and fresh instance are materially cleaner than trying to alter an existing session
- the fixed audit path now reflects the isolated launch result rather than the pre-existing session

## Codex CLI Findings

### 1. The HTTPS-to-SSH git rewrite was the direct CLI leak trigger

We ran:

```sh
codex exec --skip-git-repo-check "Reply exactly: proxy-audit-ok"
```

with:

- `ALL_PROXY=socks5h://127.0.0.1:9595`
- `HTTP_PROXY=socks5h://127.0.0.1:9595`
- `HTTPS_PROXY=socks5h://127.0.0.1:9595`

The CLI audit still observed a direct `ssh` child process:

- destination: `20.205.243.166:22`

This happened both:

- from the repo working directory
- from `/tmp` with `--skip-git-repo-check`

Established:

- Codex CLI, as exercised here, can spawn a direct `ssh` child when a global HTTPS-to-SSH rewrite is present
- the shipped Codex binary contains a curated plugin sync path that runs `git ls-remote` against `https://github.com/openai/plugins.git`
- removing `url.git@github.com:.insteadof=https://github.com/` eliminated the observed direct `ssh` socket path
- after that removal, the CLI no longer showed a network leak in the audit; the remaining failure was auth/token refresh against `chatgpt.com`

Not yet established:

- whether any other CLI path can still leak under a different configuration or plugin set

Current conclusion:

- treat Codex CLI as proxy-contained only if the HTTPS-to-SSH rewrite is removed or overridden
- if a hard guarantee is needed, still use external enforcement because other CLI paths may exist

## Current Architecture Recommendation

### Desktop

Recommended:

1. Start a fresh Codex Desktop instance.
2. Pass both proxy env vars and Chromium proxy flags.
3. Use a dedicated `--user-data-dir` if you need isolation from an existing session.
4. Audit the resulting helper processes with `lsof` or the audit script.

Recommended launch form:

```sh
ALL_PROXY='socks5h://127.0.0.1:9595' \
HTTP_PROXY='socks5h://127.0.0.1:9595' \
HTTPS_PROXY='socks5h://127.0.0.1:9595' \
open -na /Applications/Codex.app --args \
  --user-data-dir=/tmp/codex-proxy-profile \
  --proxy-server=socks5://127.0.0.1:9595 \
  --proxy-bypass-list='<local>'
```

Avoid relying on:

- changing proxy settings after Codex is already running
- assuming the visible Electron process represents the whole traffic picture

### CLI

Recommended current position:

1. Use proxy env vars as the first layer.
2. Assume this is insufficient for hard guarantees.
3. Add an external per-process enforcement layer if leaks are unacceptable.

Examples of stronger enforcement:

- a per-app proxifier
- an outbound firewall rule that only permits localhost proxy access
- a NetworkExtension-based per-app or transparent proxy

## Confidence Levels

High confidence:

- the SOCKS proxy changes egress IP
- a fresh isolated Codex Desktop launch with explicit proxy flags can be pinned to `127.0.0.1:9595`
- the original app audit path was contaminated by the existing desktop session
- the CLI path we tested spawned a direct `ssh` connection only while the HTTPS-to-SSH rewrite was present
- removing that rewrite eliminated the observed `ssh` leak in the audit

Medium confidence:

- the current SOCKS proxy does not add unique public metadata in ordinary HTTP echo tests

Low confidence / still open:

- the exact root cause of the CLI `ssh` connection
- whether every Codex Desktop feature stays proxy-contained under the fresh-launch pattern
- whether UDP, QUIC, voice, realtime, and browser-pane features are equally clean under the same launch pattern
- whether any other CLI subsystem can still bypass proxy env vars under a different setup

## Open Questions

- What exact Codex CLI subsystem is spawning the direct `ssh` child?
- Can the CLI be made clean with additional config, or does it require external containment?
- Does the fresh-launch desktop pattern remain clean during longer sessions and feature use such as voice, file upload, browser automation, and login flows?
- Is there a stable wrapper we should standardize for team use?

## Artifacts

Current supporting reports:

- `/tmp/codex-proxy-audit-metadata.json`
- `/tmp/codex-proxy-audit-cli.json`
- `/tmp/codex-proxy-audit-cli-tmp.json`
- `/tmp/codex-proxy-audit-cli-clean.json`
- `/tmp/codex-proxy-audit-cli-no-ssh-rewrite.json`
- `/tmp/codex-proxy-audit-app.json`
- `/tmp/codex-proxy-audit-app-isolated.json`

Primary audit script:

- [`scripts/codex_proxy_audit.py`](scripts/codex_proxy_audit.py)
