package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestGetServiceAttributes_NoAttributesSet_Succeeds proves GetServiceAttributes
// on a service that never had UpdateServiceAttributes called succeeds with an
// empty map, instead of the fabricated "ServiceAttributesNotFound" code
// (which named no type anywhere in servicediscovery's pinned SDK module).
// GetServiceAttributes's own deserializeOpError (servicediscovery@v1.43.4
// deserializers.go) models only InvalidInput and ServiceNotFound.
func TestGetServiceAttributes_NoAttributesSet_Succeeds(t *testing.T) {
	t.Parallel()

	tests := map[string]struct{}{
		"never updated": {},
	}

	for name := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion)
			h := servicediscovery.NewHandler(backend)
			client := newTestServiceDiscoveryClient(t, h)

			created, err := client.CreateService(t.Context(), &sdsdk.CreateServiceInput{
				Name: aws.String("no-attrs-svc"),
			})
			require.NoError(t, err)

			got, err := client.GetServiceAttributes(t.Context(), &sdsdk.GetServiceAttributesInput{
				ServiceId: created.Service.Id,
			})
			require.NoError(t, err)
			assert.Empty(t, got.ServiceAttributes.Attributes)
			assert.Equal(t, aws.ToString(created.Service.Arn), aws.ToString(got.ServiceAttributes.ServiceArn))
		})
	}
}
