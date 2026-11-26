# ACM

In-memory AWS Certificate Manager: request, import, export, renew and revoke certificates, plus ACME endpoints.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws acm request-certificate --domain-name api.example.com \
    --validation-method DNS --subject-alternative-names www.example.com
aws acm describe-certificate \
    --certificate-arn arn:aws:acm:us-east-1:000000000000:certificate/<cert-id>
aws acm import-certificate --certificate fileb://cert.pem \
    --private-key fileb://key.pem --certificate-chain fileb://chain.pem
aws acm get-certificate --certificate-arn <arn>
aws acm export-certificate --certificate-arn <arn> --passphrase dGVzdA==
aws acm add-tags-to-certificate --certificate-arn <arn> --tags Key=env,Value=dev
```

## Supported operations

| Area | Operations |
|------|------------|
| Certificates | `RequestCertificate`, `DescribeCertificate`, `ListCertificates`, `SearchCertificates`, `DeleteCertificate`, `ImportCertificate`, `GetCertificate`, `ExportCertificate`, `RenewCertificate`, `RevokeCertificate`, `UpdateCertificateOptions`, `ResendValidationEmail`, `ListCertificateDomainValidations` |
| Account | `GetAccountConfiguration`, `PutAccountConfiguration` |
| Tags | `AddTagsToCertificate`, `RemoveTagsFromCertificate`, `ListTagsForCertificate`, `TagResource`, `UntagResource`, `ListTagsForResource` |
| ACME endpoints | `Create/Update/Delete/Describe/ListAcmeEndpoint(s)` |
| ACME external account bindings | `Create/Delete/Describe/List/RevokeAcmeExternalAccountBinding`, `GetAcmeExternalAccountBindingCredentials` |
| ACME accounts | `DescribeAcmeAccount`, `ListAcmeAccounts`, `RevokeAcmeAccount` |
| ACME domain validations | `Create/Update/Delete/Describe/ListAcmeDomainValidation(s)` |

## Behaviour

- DNS and email validation: the certificate starts `PENDING_VALIDATION`. `DescribeCertificate` returns synthetic CNAME `ResourceRecord` entries, and about 100 ms later the certificate becomes `ISSUED`. This works with Terraform's `aws_acm_certificate_validation` and the SDK's `CertificateValidatedWaiter`.
- Without a `ValidationMethod` the certificate is `ISSUED` immediately.
- Generated certificates are self-signed ECDSA P-256 and are not trusted anywhere.
- `ExportCertificate` works for `IMPORTED` and `PRIVATE` certificates. `AMAZON_ISSUED` ones need `Options.Export=ENABLED`.

## Differences from AWS

> [!NOTE]
> No DNS lookup, CA or ACME server exists. Validation settles on a timer, so `FAILED` and `INACTIVE` certificates and `INVALID` ACME domain validations never occur, and ACME accounts are never populated.

- Private CAs (ACM PCA) are not part of this service.
- `TagPolicyException` is never raised: there is no Organizations tag-policy engine.
