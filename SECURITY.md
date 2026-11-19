# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in airflow-provider-dreamhub, please report it responsibly.

**Email:** [security@dreamhub.ai](mailto:security@dreamhub.ai)

**Do NOT file a GitHub issue for security vulnerabilities.** Public disclosure of vulnerabilities puts all users at risk.

## What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Acknowledge:** Within 48 hours of your report.
- **Fix:** Within 90 days, depending on severity and complexity.

We will coordinate disclosure with you and credit reporters who follow responsible disclosure.

## Handling your Dreamhub API token

The provider reads the token from an Airflow connection. Store it in a secrets backend rather than in DAG
code, give the Dreamhub user behind it only the permissions the pipeline needs, and rotate it before it
expires.
