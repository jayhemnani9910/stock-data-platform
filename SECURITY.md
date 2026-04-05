# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public issue.** Instead, email **jayhemnani9910@gmail.com** with:

- A description of the vulnerability
- Steps to reproduce
- Potential impact

You can expect an initial response within 48 hours.

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest on `master` | Yes |

## Security Considerations

This project handles market data and connects to external APIs. Key areas:

- **Database credentials** are managed via environment variables (`.env`) and should never be committed
- **API keys** (FRED, SEC EDGAR identity) are configured in `.env`
- **Airflow webserver** runs on port 8081 with default credentials — change these in production
- **Kafka** and **TimescaleDB** are exposed on their default ports — restrict access in production environments
