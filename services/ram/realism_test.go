package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func newRealismRAMClient(t *testing.T) *ramsdk.Client {
	t.Helper()

	return newTestRAMClient(t, ram.NewHandler(ram.NewInMemoryBackend("000000000000", "us-east-1")))
}

func TestCreateResourceShareDefaultsAllowExternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		allow *bool
		name  string
		want  bool
	}{
		{name: "omitted", allow: nil, want: true},
		{name: "explicit_false", allow: aws.Bool(false), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismRAMClient(t)
			out, err := client.CreateResourceShare(t.Context(), &ramsdk.CreateResourceShareInput{
				Name:                    aws.String("s"),
				AllowExternalPrincipals: tt.allow,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToBool(out.ResourceShare.AllowExternalPrincipals))
		})
	}
}

func TestRAMErrorShapes(t *testing.T) {
	t.Parallel()

	const missingShare = "arn:aws:ram:us-east-1:000000000000:resource-share/00000000-0000-0000-0000-000000000000"

	tests := []struct {
		call     func(t *testing.T, c *ramsdk.Client) error
		name     string
		wantCode string
	}{
		{
			name:     "delete_unknown_share",
			wantCode: "UnknownResourceException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.DeleteResourceShare(t.Context(), &ramsdk.DeleteResourceShareInput{
					ResourceShareArn: aws.String(missingShare),
				})

				return err
			},
		},
		{
			name:     "delete_malformed_arn",
			wantCode: "MalformedArnException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.DeleteResourceShare(t.Context(), &ramsdk.DeleteResourceShareInput{
					ResourceShareArn: aws.String("bad"),
				})

				return err
			},
		},
		{
			name:     "update_malformed_arn",
			wantCode: "MalformedArnException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.UpdateResourceShare(t.Context(), &ramsdk.UpdateResourceShareInput{
					ResourceShareArn: aws.String("bad"),
				})

				return err
			},
		},
		{
			name:     "bad_principal",
			wantCode: "InvalidParameterException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.CreateResourceShare(t.Context(), &ramsdk.CreateResourceShareInput{
					Name:       aws.String("s"),
					Principals: []string{"notanaccount"},
				})

				return err
			},
		},
		{
			name:     "bad_resource_owner",
			wantCode: "InvalidParameterException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.GetResourceShares(t.Context(), &ramsdk.GetResourceSharesInput{
					ResourceOwner: ramtypes.ResourceOwner("BOGUS"),
				})

				return err
			},
		},
		{
			name:     "bad_association_type",
			wantCode: "InvalidParameterException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.GetResourceShareAssociations(t.Context(), &ramsdk.GetResourceShareAssociationsInput{
					AssociationType: ramtypes.ResourceShareAssociationType("BAD"),
				})

				return err
			},
		},
		{
			name:     "bad_next_token",
			wantCode: "InvalidNextTokenException",
			call: func(t *testing.T, c *ramsdk.Client) error {
				t.Helper()

				_, err := c.GetResourceShares(t.Context(), &ramsdk.GetResourceSharesInput{
					ResourceOwner: ramtypes.ResourceOwnerSelf,
					NextToken:     aws.String("bogus!"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(t, newRealismRAMClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEmpty(t, apiErr.ErrorMessage())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}
