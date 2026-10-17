package transfer_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func TestWebAppGeneratedIDShapes(t *testing.T) {
	t.Parallel()

	appARN := regexp.MustCompile(`^arn:aws:sso::\d{12}:application/apl-[0-9a-f]{16}$`)
	vpce := regexp.MustCompile(`^vpce-[0-9a-f]{17}$`)

	tests := []struct {
		mut  func(b *transfer.InMemoryBackend, id string) *transfer.WebApp
		name string
	}{
		{name: "create", mut: nil},
		{name: "update_subnets", mut: func(b *transfer.InMemoryBackend, id string) *transfer.WebApp {
			w, err := b.UpdateWebApp(&transfer.UpdateWebAppInput{WebAppID: id, VpcSubnetIDs: []string{"subnet-1"}})
			require.NoError(t, err)

			return w
		}},
		{name: "update_ip_type", mut: func(b *transfer.InMemoryBackend, id string) *transfer.WebApp {
			ip := "IPV4"
			w, err := b.UpdateWebApp(&transfer.UpdateWebAppInput{WebAppID: id, VpcIPAddressType: &ip})
			require.NoError(t, err)

			return w
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			in := &transfer.CreateWebAppInput{
				IdentityCenterConfig: &transfer.WebAppIdentityCenterConfig{
					InstanceArn: "arn:aws:sso:::instance/ssoins-1234567890abcdef",
					Role:        "arn:aws:iam::123456789012:role/r",
				},
			}
			if tt.mut == nil {
				in.VpcConfig = &transfer.WebAppVpcConfig{SubnetIDs: []string{"subnet-1"}}
			}

			w, err := b.CreateWebApp(in)
			require.NoError(t, err)
			assert.Regexp(t, appARN, w.IdentityCenterConfig.ApplicationArn)

			if tt.mut != nil {
				w = tt.mut(b, w.WebAppID)
			}

			require.NotNil(t, w.VpcConfig)
			assert.Regexp(t, vpce, w.VpcConfig.VpcEndpointID)
		})
	}
}
