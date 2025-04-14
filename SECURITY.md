# Security Policy

## Supported Versions

Currently, security updates are provided for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability within this project, please follow these steps to report it:

1. **Do NOT disclose the vulnerability publicly**
2. Email the details to the project maintainer at [adhish.thite@elastic.co](mailto:adhish.thite@elastic.co)
3. Include as much information as possible, such as:
   - A description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact
   - Suggestions for remediation, if you have any

You should receive a response within 48 hours acknowledging receipt of your report.

## Security Best Practices for Users

When using this project, please follow these security best practices:

1. **API Keys and Credentials**: Never commit API keys, passwords, or other credentials to version control. Use environment variables or a secure vault.
2. **Least Privilege**: When configuring access to BigQuery, Elasticsearch, and OpenAI, follow the principle of least privilege.
3. **Regular Updates**: Keep dependencies updated to benefit from security patches.
4. **Input Validation**: Validate and sanitize any external data before processing.