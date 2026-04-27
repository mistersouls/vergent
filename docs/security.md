# Security

## Threat Model

Tourillon assumes hostile networks and potentially compromised clients. The baseline objective is confidentiality, integrity, peer authenticity, and auditability.

Primary threats:
- Passive traffic interception.
- Active man-in-the-middle attacks.
- Unauthorized node enrollment.
- Replay of valid messages.

## Mandatory mTLS Requirements

- All communications MUST require mutual TLS.
- Nodes MUST validate peer certificate chains and identities before processing payloads.
- Clients MUST present trusted certificates when connecting to any node.
- Cleartext fallback MUST NOT exist.

## Certificate and Trust Management

- Trust roots MUST be explicitly configured.
- Certificate rotation SHOULD be supported without full cluster downtime.
- Revocation strategy MUST be defined (CRL, OCSP, or short-lived cert policy).
- Enrollment flows for new nodes MUST include identity verification.

## AuthN/AuthZ Boundaries

- Authentication MUST be certificate-based at transport level.
- Authorization SHOULD define role boundaries (client vs node, read vs write, admin operations).
- Sensitive admin endpoints MUST be separately scoped and audited.

## Security Hardening Baseline

- TLS versions/ciphers MUST follow modern secure defaults.
- Private keys MUST be protected at rest and never logged.
- Security-relevant events MUST be logged with correlation IDs.
- Replay protection SHOULD combine nonce/timestamp/session constraints where applicable.

## Security Acceptance Criteria

- Nodes refuse non-mTLS connections.
- Invalid or expired certificates are rejected deterministically.
- Certificate rotation procedure is tested in staging.
- Audit logs allow reconstruction of security incidents.

