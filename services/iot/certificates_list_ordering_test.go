package iot_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iotsdktypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestListCertificates_AscendingOrder proves ListCertificates honors
// AscendingOrder ("results are returned in ascending order, based on the
// creation date", iot@v1.77.4 api_op_ListCertificates.go), which the handler
// previously never read at all.
func TestListCertificates_AscendingOrder(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	base := time.Now()
	b.AddCertificateInternal(iot.Certificate{CertificateID: "zulu", CreatedAt: base})
	b.AddCertificateInternal(iot.Certificate{CertificateID: "alpha", CreatedAt: base.Add(time.Second)})
	b.AddCertificateInternal(iot.Certificate{CertificateID: "mike", CreatedAt: base.Add(2 * time.Second)})

	out, err := client.ListCertificates(t.Context(), &iotsdk.ListCertificatesInput{AscendingOrder: true})
	require.NoError(t, err)
	require.Len(t, out.Certificates, 3)
	assert.Equal(t, []string{"zulu", "alpha", "mike"}, certIDs(out.Certificates))

	outDesc, err := client.ListCertificates(t.Context(), &iotsdk.ListCertificatesInput{AscendingOrder: false})
	require.NoError(t, err)
	assert.Equal(t, []string{"mike", "alpha", "zulu"}, certIDs(outDesc.Certificates))
}

// TestListCertificates_Pagination proves PageSize/Marker are honored --
// previously the handler ignored both and always returned the entire list
// in one response.
func TestListCertificates_Pagination(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	base := time.Now()
	for i := range 5 {
		b.AddCertificateInternal(iot.Certificate{
			CertificateID: certName(i), CreatedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	out, err := client.ListCertificates(t.Context(), &iotsdk.ListCertificatesInput{PageSize: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, out.Certificates, 2)
	require.NotNil(t, out.NextMarker)

	out2, err := client.ListCertificates(t.Context(), &iotsdk.ListCertificatesInput{
		PageSize: aws.Int32(2), Marker: out.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, out2.Certificates, 2)
	require.NotNil(t, out2.NextMarker)

	out3, err := client.ListCertificates(t.Context(), &iotsdk.ListCertificatesInput{
		PageSize: aws.Int32(2), Marker: out2.NextMarker,
	})
	require.NoError(t, err)
	assert.Len(t, out3.Certificates, 1)
	assert.Nil(t, out3.NextMarker)
}

// TestListCACertificates_AscendingOrderAndTemplateFilter proves
// ListCACertificates honors AscendingOrder and TemplateName -- previously
// the handler read neither, always returning every CA cert in insertion
// order.
func TestListCACertificates_AscendingOrderAndTemplateFilter(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	b.AddCACertificateInternal(iot.CACertificate{CertificateID: "zulu", CreationDate: 100})
	b.AddCACertificateInternal(iot.CACertificate{CertificateID: "alpha", CreationDate: 200})
	b.AddCACertificateInternal(iot.CACertificate{
		CertificateID: "mike", CreationDate: 300,
		RegistrationConfig: iot.RegistrationConfig{TemplateName: "jitp-template"},
	})

	out, err := client.ListCACertificates(t.Context(), &iotsdk.ListCACertificatesInput{AscendingOrder: true})
	require.NoError(t, err)
	require.Len(t, out.Certificates, 3)
	assert.Equal(t, []string{"zulu", "alpha", "mike"}, caCertIDs(out.Certificates))

	filtered, err := client.ListCACertificates(t.Context(), &iotsdk.ListCACertificatesInput{
		TemplateName: aws.String("jitp-template"),
	})
	require.NoError(t, err)
	require.Len(t, filtered.Certificates, 1)
	assert.Equal(t, "mike", aws.ToString(filtered.Certificates[0].CertificateId))
}

// TestListCertificatesByCA_AscendingOrder proves ListCertificatesByCA
// honors AscendingOrder, same class of gap as ListCertificates.
func TestListCertificatesByCA_AscendingOrder(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	base := time.Now()
	b.AddCertificateInternal(iot.Certificate{CertificateID: "zulu", CACertificateID: "ca-1", CreatedAt: base})
	b.AddCertificateInternal(iot.Certificate{
		CertificateID: "alpha", CACertificateID: "ca-1", CreatedAt: base.Add(time.Second),
	})

	out, err := client.ListCertificatesByCA(t.Context(), &iotsdk.ListCertificatesByCAInput{
		CaCertificateId: aws.String("ca-1"), AscendingOrder: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Certificates, 2)
	assert.Equal(t, []string{"zulu", "alpha"}, certIDs(out.Certificates))

	outDesc, err := client.ListCertificatesByCA(t.Context(), &iotsdk.ListCertificatesByCAInput{
		CaCertificateId: aws.String("ca-1"), AscendingOrder: false,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "zulu"}, certIDs(outDesc.Certificates))
}

// TestListCertificateProviders_AscendingOrder proves ListCertificateProviders
// honors AscendingOrder ("Returns the list of certificate providers in
// ascending alphabetical order", iot@v1.77.4
// api_op_ListCertificateProviders.go).
func TestListCertificateProviders_AscendingOrder(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)
	client := newTestIoTClient(t, h)

	for _, name := range []string{"zulu", "alpha", "mike"} {
		_, err := client.CreateCertificateProvider(t.Context(), &iotsdk.CreateCertificateProviderInput{
			CertificateProviderName: aws.String(name),
			LambdaFunctionArn:       aws.String("arn:aws:lambda:us-east-1:123456789012:function:f"),
			AccountDefaultForOperations: []iotsdktypes.CertificateProviderOperation{
				iotsdktypes.CertificateProviderOperationCreateCertificateFromCsr,
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListCertificateProviders(
		t.Context(), &iotsdk.ListCertificateProvidersInput{AscendingOrder: true},
	)
	require.NoError(t, err)
	require.Len(t, out.CertificateProviders, 3)
	assert.Equal(t, []string{"alpha", "mike", "zulu"}, certProviderNames(out.CertificateProviders))

	outDesc, err := client.ListCertificateProviders(
		t.Context(), &iotsdk.ListCertificateProvidersInput{AscendingOrder: false},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"zulu", "mike", "alpha"}, certProviderNames(outDesc.CertificateProviders))
}

func certName(i int) string {
	return string(rune('a'+i)) + "-cert"
}

func certIDs(certs []iotsdktypes.Certificate) []string {
	ids := make([]string, len(certs))
	for i, c := range certs {
		ids[i] = aws.ToString(c.CertificateId)
	}

	return ids
}

func caCertIDs(certs []iotsdktypes.CACertificate) []string {
	ids := make([]string, len(certs))
	for i, c := range certs {
		ids[i] = aws.ToString(c.CertificateId)
	}

	return ids
}

func certProviderNames(cps []iotsdktypes.CertificateProviderSummary) []string {
	names := make([]string, len(cps))
	for i, cp := range cps {
		names[i] = aws.ToString(cp.CertificateProviderName)
	}

	return names
}
