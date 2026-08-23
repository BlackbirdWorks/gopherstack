package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestUpdateServiceAttributes_RoundTrip drives UpdateServiceAttributes
// through the real SDK client, which always serializes the identifier as
// the wire key "ServiceId" (servicediscovery@v1.43.4:
// serializers.go:3040-3043), never "ServiceArn". gopherstack's decode
// struct previously only recognized "ServiceArn", so every real client
// call left the identifier empty and failed with "ServiceArn is required" —
// this test fails against that code (see hand-revert note in PARITY.md).
func TestUpdateServiceAttributes_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		identifier func(svc *sdsdk.CreateServiceOutput) string
		name       string
	}{
		{
			name: "by id",
			identifier: func(svc *sdsdk.CreateServiceOutput) string {
				return aws.ToString(svc.Service.Id)
			},
		},
		{
			name: "by arn",
			identifier: func(svc *sdsdk.CreateServiceOutput) string {
				return aws.ToString(svc.Service.Arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion)
			h := servicediscovery.NewHandler(backend)
			client := newTestServiceDiscoveryClient(t, h)

			created, err := client.CreateService(t.Context(), &sdsdk.CreateServiceInput{
				Name: aws.String("wire-attrs-svc"),
			})
			require.NoError(t, err)

			_, err = client.UpdateServiceAttributes(t.Context(), &sdsdk.UpdateServiceAttributesInput{
				ServiceId:  aws.String(tt.identifier(created)),
				Attributes: map[string]string{"env": "prod"},
			})
			require.NoError(t, err, "real SDK client always sends the ServiceId wire key")

			got, err := client.GetServiceAttributes(t.Context(), &sdsdk.GetServiceAttributesInput{
				ServiceId: created.Service.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, "prod", got.ServiceAttributes.Attributes["env"])
			assert.Equal(t, aws.ToString(created.Service.Arn), aws.ToString(got.ServiceAttributes.ServiceArn))
		})
	}
}
