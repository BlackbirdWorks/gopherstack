package apigatewayv2_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestResourceTagsConcurrentWithUntagResource proves Get/List for APIs and VPC
// links must not hand back a Tags map UntagResource mutates in place.
func TestResourceTagsConcurrentWithUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *apigatewayv2.InMemoryBackend) (resourceARN string)
		reader func(b *apigatewayv2.InMemoryBackend)
		name   string
	}{
		{
			name: "GetAPIs races UntagResource",
			setup: func(t *testing.T, b *apigatewayv2.InMemoryBackend) string {
				t.Helper()

				api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
					Name:         "race-api",
					ProtocolType: "HTTP",
					Tags:         map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return api.APIID
			},
			reader: func(b *apigatewayv2.InMemoryBackend) {
				apis, err := b.GetAPIs()
				if err != nil {
					return
				}

				for _, api := range apis {
					for k := range api.Tags {
						_ = k
					}
				}
			},
		},
		{
			name: "GetVpcLinks races UntagResource",
			setup: func(t *testing.T, b *apigatewayv2.InMemoryBackend) string {
				t.Helper()

				vl, err := b.CreateVpcLink(apigatewayv2.CreateVpcLinkInput{
					Name:      "race-vpc-link",
					SubnetIDs: []string{"subnet-1"},
					Tags:      map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return "vpclinks/" + vl.VpcLinkID
			},
			reader: func(b *apigatewayv2.InMemoryBackend) {
				links, err := b.GetVpcLinks()
				if err != nil {
					return
				}

				for _, vl := range links {
					for k := range vl.Tags {
						_ = k
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()
			resourceARN := tt.setup(t, b)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b)
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					_ = b.TagResource(resourceARN, map[string]string{"env": "prod"})
					_ = b.UntagResource(resourceARN, []string{"env"})
				}
			}()

			wg.Wait()
		})
	}
}
