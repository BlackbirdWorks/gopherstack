package outposts_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSite_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   outpostssdk.CreateSiteInput
		wantErr bool
	}{
		{name: "valid", input: outpostssdk.CreateSiteInput{Name: aws.String("site")}},
		{
			name:    "long name",
			input:   outpostssdk.CreateSiteInput{Name: aws.String(strings.Repeat("n", 1001))},
			wantErr: true,
		},
		{
			name: "long description",
			input: outpostssdk.CreateSiteInput{
				Name: aws.String("site"), Description: aws.String(strings.Repeat("d", 1001)),
			},
			wantErr: true,
		},
		{
			name: "long notes",
			input: outpostssdk.CreateSiteInput{
				Name: aws.String("site"), Notes: aws.String(strings.Repeat("n", 2001)),
			},
			wantErr: true,
		},
		{
			name: "bad rack enum",
			input: outpostssdk.CreateSiteInput{
				Name:                   aws.String("site"),
				RackPhysicalProperties: &types.RackPhysicalProperties{PowerDrawKva: types.PowerDrawKva("POWER_99_KVA")},
			},
			wantErr: true,
		},
		{
			name: "good rack enum",
			input: outpostssdk.CreateSiteInput{
				Name:                   aws.String("site"),
				RackPhysicalProperties: &types.RackPhysicalProperties{PowerDrawKva: types.PowerDrawKvaPower10Kva},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			_, err := client.CreateSite(t.Context(), &tt.input)

			if tt.wantErr {
				var apiErr *types.ValidationException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateSiteRackPhysicalProperties_EnumValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		props   outpostssdk.UpdateSiteRackPhysicalPropertiesInput
		wantErr bool
	}{
		{
			name:  "valid",
			props: outpostssdk.UpdateSiteRackPhysicalPropertiesInput{UplinkGbps: types.UplinkGbpsUplink10g},
		},
		{
			name:    "invalid",
			props:   outpostssdk.UpdateSiteRackPhysicalPropertiesInput{UplinkGbps: types.UplinkGbps("UPLINK_7G")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			siteID := createTestSite(t, client)
			tt.props.SiteId = aws.String(siteID)

			_, err := client.UpdateSiteRackPhysicalProperties(t.Context(), &tt.props)

			if tt.wantErr {
				var apiErr *types.ValidationException
				require.ErrorAs(t, err, &apiErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestGetOutpost_NotFoundMessage(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	_, err := client.GetOutpost(
		t.Context(),
		&outpostssdk.GetOutpostInput{OutpostId: aws.String("op-0123456789abcdef0")},
	)

	var apiErr *types.NotFoundException
	require.ErrorAs(t, err, &apiErr)
	assert.NotContains(t, apiErr.ErrorMessage(), "NotFoundException")
	assert.Contains(t, apiErr.ErrorMessage(), "not found")
}
