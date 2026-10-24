package cloudhsmv2_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hsm "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
)

func parseOneCert(t *testing.T, label, p string) *x509.Certificate {
	t.Helper()

	blk, _ := pem.Decode([]byte(p))
	require.NotNil(t, blk, label)

	c, err := x509.ParseCertificate(blk.Bytes)
	require.NoError(t, err, label)

	return c
}

func testNoHardwareCertsWithoutHsm(t *testing.T, f *fixture) {
	t.Helper()

	c := f.createCluster(t)
	f.clock.advance(time.Second)

	certs := f.describeOne(t, aws.ToString(c.ClusterId)).Certificates
	require.NotNil(t, certs)
	assert.NotEmpty(t, aws.ToString(certs.ClusterCsr))
	assert.Nil(t, certs.HsmCertificate)
	assert.Nil(t, certs.AwsHardwareCertificate)
	assert.Nil(t, certs.ManufacturerHardwareCertificate)
}

func testHardwareChain(t *testing.T, f *fixture) {
	t.Helper()

	c := f.createCluster(t)
	f.clock.advance(time.Second)

	_, err := f.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
		ClusterId: c.ClusterId, AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)

	got := f.describeOne(t, aws.ToString(c.ClusterId))
	require.NotNil(t, got.Certificates)
	assert.Equal(t, "UNINITIALIZED", string(got.State))

	certs := got.Certificates
	mfr := parseOneCert(t, "manufacturer", aws.ToString(certs.ManufacturerHardwareCertificate))
	awsHw := parseOneCert(t, "aws", aws.ToString(certs.AwsHardwareCertificate))
	hsmCert := parseOneCert(t, "hsm", aws.ToString(certs.HsmCertificate))

	require.NoError(t, mfr.CheckSignatureFrom(mfr))
	require.NoError(t, awsHw.CheckSignatureFrom(mfr))
	require.NoError(t, hsmCert.CheckSignatureFrom(awsHw))

	blk, _ := pem.Decode([]byte(aws.ToString(certs.ClusterCsr)))
	require.NotNil(t, blk)

	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	require.NoError(t, err)
	require.NoError(t, csr.CheckSignature())
	hsmPub, ok := hsmCert.PublicKey.(*ecdsa.PublicKey)
	require.True(t, ok)
	assert.True(t, hsmPub.Equal(csr.PublicKey))

	again := f.describeOne(t, aws.ToString(c.ClusterId)).Certificates
	assert.Equal(t, aws.ToString(certs.HsmCertificate), aws.ToString(again.HsmCertificate))
	assert.Equal(t, aws.ToString(certs.AwsHardwareCertificate), aws.ToString(again.AwsHardwareCertificate))

	signed, root := signCSR(t, aws.ToString(certs.ClusterCsr))
	_, err = f.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String(signed), TrustAnchor: aws.String(root),
	})
	require.NoError(t, err)

	f.clock.advance(time.Second)

	after := f.describeOne(t, aws.ToString(c.ClusterId)).Certificates
	assert.Equal(t, signed, aws.ToString(after.ClusterCertificate))
	assert.Equal(t, aws.ToString(certs.HsmCertificate), aws.ToString(after.HsmCertificate))
	assert.Nil(t, after.ClusterCsr)
}

func testHardwareCertsPersist(t *testing.T, f *fixture) {
	t.Helper()

	c := f.createCluster(t)
	f.clock.advance(time.Second)

	_, err := f.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
		ClusterId: c.ClusterId, AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)

	want := f.describeOne(t, aws.ToString(c.ClusterId)).Certificates
	require.NotNil(t, want.HsmCertificate)

	restored := hsm.NewInMemoryBackend(testAccount, testRegion)
	require.NoError(t, restored.Restore(t.Context(), f.backend.Snapshot(t.Context())))

	r := newFixtureFromBackend(t, restored)
	r.clock.t = f.clock.now()

	got := r.describeOne(t, aws.ToString(c.ClusterId)).Certificates
	require.NotNil(t, got)
	assert.Equal(t, aws.ToString(want.HsmCertificate), aws.ToString(got.HsmCertificate))
	assert.Equal(t, aws.ToString(want.AwsHardwareCertificate), aws.ToString(got.AwsHardwareCertificate))
	assert.Equal(t, aws.ToString(want.ManufacturerHardwareCertificate), aws.ToString(got.ManufacturerHardwareCertificate))

	_, err = r.client.CreateHsm(t.Context(), &sdk.CreateHsmInput{
		ClusterId: c.ClusterId, AvailabilityZone: aws.String("us-east-1b"),
	})
	require.NoError(t, err)

	signed, root := signCSR(t, aws.ToString(got.ClusterCsr))
	_, err = r.client.InitializeCluster(t.Context(), &sdk.InitializeClusterInput{
		ClusterId: c.ClusterId, SignedCert: aws.String(signed), TrustAnchor: aws.String(root),
	})
	require.NoError(t, err)
}

func TestRealClient_HardwareCertificates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testNoHardwareCertsWithoutHsm, "none_without_hsm"},
		{testHardwareChain, "chain"},
		{testHardwareCertsPersist, "persist"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}
