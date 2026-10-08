package docdb

import "context"

func certificateCatalog() []Certificate {
	return []Certificate{
		{
			CertificateIdentifier: "rds-ca-2019",
			CertificateType:       "CA",
			Thumbprint:            "d404926ab3b1c6f0ad61f8d95dadf6c3eea47dbf",
			ValidFrom:             "2019-09-19T00:00:00Z",
			ValidTill:             "2024-08-22T00:00:00Z",
		},
		{
			CertificateIdentifier: "rds-ca-rsa2048-g1",
			CertificateType:       "CA",
			Thumbprint:            "cf5c7c1cf32cae39012fc84c8d9e76c25bce55fb",
			ValidFrom:             "2021-05-25T00:00:00Z",
			ValidTill:             "2061-05-25T00:00:00Z",
		},
	}
}

func certificateValidTill(id string) string {
	for _, c := range certificateCatalog() {
		if c.CertificateIdentifier == id {
			return c.ValidTill
		}
	}

	return ""
}

// DescribeCertificates returns certificate information.
func (b *InMemoryBackend) DescribeCertificates(_ context.Context, certificateID string) []Certificate {
	certs := certificateCatalog()
	if certificateID == "" {
		return certs
	}
	for _, c := range certs {
		if c.CertificateIdentifier == certificateID {
			return []Certificate{c}
		}
	}

	return []Certificate{}
}
