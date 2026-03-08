# ACM — AWS Certificate Manager

In-memory ACM implementation for certificate lifecycle management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `RequestCertificate` | Request a new public certificate (supports DNS and EMAIL validation) |
| `DescribeCertificate` | Get certificate details, status, and DomainValidationOptions |
| `ListCertificates` | List all certificates |
| `DeleteCertificate` | Delete a certificate |
| `ImportCertificate` | Import a PEM-encoded certificate with private key and optional chain |
| `RenewCertificate` | Trigger renewal for an eligible certificate |
| `ExportCertificate` | Export an IMPORTED certificate with its private key |
| `GetCertificate` | Return the PEM certificate body and chain |
| `ListTagsForCertificate` | List tags on a certificate |
| `AddTagsToCertificate` | Add tags to a certificate |
| `RemoveTagsFromCertificate` | Remove tags from a certificate |

## DNS Validation Workflow

When `RequestCertificate` is called with `ValidationMethod: DNS`:

1. The certificate is created with `PENDING_VALIDATION` status.
2. `DescribeCertificate` returns `DomainValidationOptions` including synthetic CNAME `ResourceRecord` entries.
3. The mock automatically transitions the certificate to `ISSUED` after ~100 ms, simulating DNS validation.

This workflow is compatible with Terraform's `aws_acm_certificate_validation` resource and the AWS SDK's `CertificateValidatedWaiter`.

## AWS CLI Examples

```bash
# Request a certificate with DNS validation
aws --endpoint-url http://localhost:8000 acm request-certificate \
    --domain-name api.example.com \
    --validation-method DNS \
    --subject-alternative-names www.example.com

# List certificates
aws --endpoint-url http://localhost:8000 acm list-certificates

# Describe a certificate (shows DomainValidationOptions with CNAME records)
aws --endpoint-url http://localhost:8000 acm describe-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>

# Import a certificate
aws --endpoint-url http://localhost:8000 acm import-certificate \
    --certificate fileb://cert.pem \
    --private-key fileb://key.pem \
    --certificate-chain fileb://chain.pem

# Get certificate PEM body
aws --endpoint-url http://localhost:8000 acm get-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>

# Export an imported certificate
aws --endpoint-url http://localhost:8000 acm export-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id> \
    --passphrase dGVzdA==

# Renew a certificate
aws --endpoint-url http://localhost:8000 acm renew-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>

# Tag a certificate
aws --endpoint-url http://localhost:8000 acm add-tags-to-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id> \
    --tags Key=env,Value=dev

# Delete a certificate
aws --endpoint-url http://localhost:8000 acm delete-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>
```

## Notes

- `RequestCertificate` with no `ValidationMethod` issues the certificate immediately (`ISSUED`).
- `RequestCertificate` with `DNS` or `EMAIL` validation starts at `PENDING_VALIDATION` and auto-transitions to `ISSUED` after ~100 ms.
- `ExportCertificate` only works for `IMPORTED` or `PRIVATE` type certificates.
- All certificates are self-signed ECDSA P-256 (for mock purposes only).
- Private CAs (ACM PCA) are not supported.
