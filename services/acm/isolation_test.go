package acm //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func acmCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestACMRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := acmCtxRegion("us-east-1")
	ctxWest := acmCtxRegion("us-west-2")

	// 1. Request a certificate for the same domain in us-east-1.
	eastCert, err := backend.RequestCertificate(ctxEast, "example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, eastCert.ARN, "us-east-1")

	// 2. Request a certificate for the SAME domain in us-west-2.
	westCert, err := backend.RequestCertificate(ctxWest, "example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)
	assert.Contains(t, westCert.ARN, "us-west-2")

	// 3. us-east-1 sees only its own certificate.
	eastList, err := backend.ListCertificates(ctxEast, ListCertificatesParams{})
	require.NoError(t, err)
	require.Len(t, eastList.Data, 1)
	assert.Equal(t, eastCert.ARN, eastList.Data[0].ARN)

	// 4. us-west-2 sees only its own certificate.
	westList, err := backend.ListCertificates(ctxWest, ListCertificatesParams{})
	require.NoError(t, err)
	require.Len(t, westList.Data, 1)
	assert.Equal(t, westCert.ARN, westList.Data[0].ARN)

	// 5. A cert created in us-east-1 is not describable from us-west-2.
	_, err = backend.DescribeCertificate(ctxWest, eastCert.ARN)
	require.Error(t, err)

	got, err := backend.DescribeCertificate(ctxEast, eastCert.ARN)
	require.NoError(t, err)
	assert.Equal(t, eastCert.ARN, got.ARN)

	// Deleting in us-east-1 leaves us-west-2's cert intact.
	require.NoError(t, backend.DeleteCertificate(ctxEast, eastCert.ARN))

	_, err = backend.DescribeCertificate(ctxEast, eastCert.ARN)
	require.Error(t, err)

	_, err = backend.DescribeCertificate(ctxWest, westCert.ARN)
	require.NoError(t, err)
}

func TestACMAccountConfigurationRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := acmCtxRegion("us-east-1")
	ctxWest := acmCtxRegion("us-west-2")

	days := int32(30)
	require.NoError(t, backend.PutAccountConfiguration(ctxEast, "tok-east", &days))

	// us-east-1 reflects the new value; us-west-2 keeps the default.
	assert.Equal(t, int32(30), backend.GetAccountConfiguration(ctxEast).DaysBeforeExpiry)
	assert.Equal(t, defaultDaysBeforeExpiry, backend.GetAccountConfiguration(ctxWest).DaysBeforeExpiry)
}
