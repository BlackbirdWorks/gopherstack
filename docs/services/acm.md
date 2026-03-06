# ACM — AWS Certificate Manager

In-memory ACM implementation for certificate lifecycle management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `RequestCertificate` | Request a new public certificate |
| `DescribeCertificate` | Get certificate details and status |
| `ListCertificates` | List all certificates |
| `DeleteCertificate` | Delete a certificate |
| `ListTagsForCertificate` | List tags on a certificate |
| `AddTagsToCertificate` | Add tags to a certificate |
| `RemoveTagsFromCertificate` | Remove tags from a certificate |

## AWS CLI Examples

```bash
# Request a certificate
aws --endpoint-url http://localhost:8000 acm request-certificate \
    --domain-name api.example.com \
    --validation-method DNS \
    --subject-alternative-names www.example.com

# List certificates
aws --endpoint-url http://localhost:8000 acm list-certificates

# Describe a certificate
aws --endpoint-url http://localhost:8000 acm describe-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>

# Tag a certificate
aws --endpoint-url http://localhost:8000 acm add-tags-to-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id> \
    --tags Key=env,Value=dev

# Delete a certificate
aws --endpoint-url http://localhost:8000 acm delete-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>
```

## Known Limitations

- Certificates are immediately marked as `ISSUED` without any validation workflow.
- Certificate issuance, renewal, and expiry are not simulated.
- Private CAs (ACM PCA) are not supported.
- Certificates cannot be used with real TLS termination.
