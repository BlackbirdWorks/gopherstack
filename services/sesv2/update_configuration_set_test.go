package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

func TestUpdateConfigurationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts     *sesv2types.MessageSecurityOptions
		name     string
		setName  string
		wantCode string
		wantSMME bool
	}{
		{
			name:    "smime scheme",
			setName: "cs",
			opts: &sesv2types.MessageSecurityOptions{
				SigningScheme: &sesv2types.SigningSchemeMemberSmimeScheme{
					Value: sesv2types.SmimeSigningScheme{SignatureFormat: sesv2types.SignatureFormatDetached},
				},
			},
			wantSMME: true,
		},
		{
			name:    "default scheme",
			setName: "cs",
			opts: &sesv2types.MessageSecurityOptions{
				SigningScheme: &sesv2types.SigningSchemeMemberDefaultScheme{},
			},
		},
		{
			name:     "missing set",
			setName:  "nope",
			opts:     &sesv2types.MessageSecurityOptions{SigningScheme: &sesv2types.SigningSchemeMemberDefaultScheme{}},
			wantCode: "NotFoundException",
		},
		{
			name:     "no options",
			setName:  "cs",
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			c := newSESv2SDKClient(t, h)
			ctx := t.Context()

			_, err := c.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
				ConfigurationSetName: aws.String("cs"),
			})
			require.NoError(t, err)

			_, err = c.UpdateConfigurationSet(ctx, &sesv2sdk.UpdateConfigurationSetInput{
				ConfigurationSetName:   aws.String(tt.setName),
				MessageSecurityOptions: tt.opts,
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiErrCode(t, err))

				return
			}

			require.NoError(t, err)

			check := func(cl *sesv2sdk.Client) {
				got, getErr := cl.GetConfigurationSet(ctx, &sesv2sdk.GetConfigurationSetInput{
					ConfigurationSetName: aws.String("cs"),
				})
				require.NoError(t, getErr)
				require.NotNil(t, got.MessageSecurityOptions)

				if tt.wantSMME {
					m, ok := got.MessageSecurityOptions.SigningScheme.(*sesv2types.SigningSchemeMemberSmimeScheme)
					require.True(t, ok)
					assert.Equal(t, sesv2types.SignatureFormatDetached, m.Value.SignatureFormat)

					return
				}

				_, ok := got.MessageSecurityOptions.SigningScheme.(*sesv2types.SigningSchemeMemberDefaultScheme)
				assert.True(t, ok)
			}

			check(c)

			fresh := sesv2.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(ctx, b.Snapshot(ctx)))
			check(newSESv2SDKClient(t, sesv2.NewHandler(fresh)))
		})
	}
}

func TestCreateConfigurationSetMessageSecurity(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	c := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := c.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String("cs"),
		MessageSecurityOptions: &sesv2types.MessageSecurityOptions{
			SigningScheme: &sesv2types.SigningSchemeMemberSmimeScheme{
				Value: sesv2types.SmimeSigningScheme{SignatureFormat: sesv2types.SignatureFormatDetached},
			},
		},
	})
	require.NoError(t, err)

	got, err := c.GetConfigurationSet(ctx, &sesv2sdk.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")})
	require.NoError(t, err)
	require.NotNil(t, got.MessageSecurityOptions)
	_, ok := got.MessageSecurityOptions.SigningScheme.(*sesv2types.SigningSchemeMemberSmimeScheme)
	assert.True(t, ok)
}
