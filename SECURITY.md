# Security policy

## Supported versions

The connector follows the OpenTelemetry Collector's release line. Security fixes
are released in the latest minor version and, where the fix is
backwards-compatible, in the preceding minor version while it is still within
the collector's support window.

| Version | Supported |
| ------- | --------- |
| 0.7.x   | Yes       |
| 0.6.x   | No        |
| < 0.6   | No        |

## Reporting a vulnerability

**Please do not open a public issue for a security problem.** A public issue
discloses the finding before any fix exists, and this component sits in the
telemetry path between applications and backends.

Use GitHub's private vulnerability reporting:

<https://github.com/agardnerIT/logs-to-spans-otel/security/advisories/new>

That route is enabled on this repository and needs no email address. If GitHub
is unavailable to you, contact the maintainer named in
[`metadata.yaml`](metadata.yaml) directly.

Please include:

- the version (or commit) you tested,
- a minimal configuration and input that reproduces the problem,
- the impact you believe it has, and
- whether the issue is already public anywhere.

## What to expect

| Stage | Target |
| ----- | ------ |
| Acknowledgement | 3 business days |
| Initial assessment and severity | 10 business days |
| Fix or mitigation for a High/Critical finding | as soon as practical, coordinated with you |
| Credit | in the advisory and the changelog, unless you ask otherwise |

We will tell you when a fix is ready and coordinate a disclosure date with you.
Fixes are released as a tagged version; there are no per-branch backports outside
the supported versions above.

## Scope

In scope:

- the connector's log-to-span conversion, grouping, buffering and export paths;
- the shipped example configurations in this repository (they are copied into
  real deployments);
- denial of service through unbounded memory, CPU or goroutine growth;
- data leakage through the connector's own output (span names, attributes,
  internal metrics, logs).

Out of scope here: vulnerabilities in the OpenTelemetry Collector core, the
Collector Builder, or third-party exporters and receivers. Report those to their
own projects - this repository's [`SECURITY.md`](SECURITY.md) covers the
component only.

## Trust boundary, in one paragraph

Log records are **application-controlled input**. Everything the connector
derives from a record - the group key, the span name, `group.key`, the duration,
and the `trace_id_keys`/`span_id_keys` attributes used for span links - is
therefore attacker-influenced unless the log source is trusted. The connector
bounds sizes (`max_record_bytes`, `max_group_bytes`, `max_groups`,
`max_logs_per_trace`) and validates ID formats, but it cannot validate
provenance. See the README section
[Handling sensitive data and untrusted logs](README.md#handling-sensitive-data-and-untrusted-logs).
