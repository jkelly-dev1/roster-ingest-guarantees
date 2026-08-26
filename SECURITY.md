# Security Policy

## Reporting a Vulnerability

Please report suspected vulnerabilities privately, not through public issues or
pull requests.

Preferred channel: use GitHub's private vulnerability reporting for this
repository (the "Report a vulnerability" button on the Security tab). It opens a
private advisory visible only to the maintainer.

Aim is to acknowledge a report within 5 business days and to share a resolution
or mitigation plan within 30 days. Timelines may vary, as this is maintained in
personal time.

Please include enough detail to reproduce the issue: the affected file or
endpoint, the version or commit, steps to reproduce, and the impact you observed.

## Supported Versions

This is a personal learning and portfolio project. Only the latest commit on the
`main` branch is supported; there are no maintained release branches or
backports.

## Scope

These are self-contained demo projects, not production services. Nothing here
calls a hosted API and no key is ever needed to run it. The one credential-
shaped string in the repository is deliberate and is not a secret:
`stack/compose.yaml` sets a fixed MinIO root user and password, because MinIO
requires a root user to start and every port in the stack is bound to loopback
on a single machine. Do not reuse that pair anywhere reachable. The Kafka
broker and the Iceberg REST catalog run without authentication for the same
reason and on the same terms. Limitations that the README documents as
deliberate, out-of-scope seams are noted but may not be actioned.
