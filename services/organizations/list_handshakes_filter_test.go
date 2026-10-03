package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ListHandshakes_FilterExclusive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter     *organizationstypes.HandshakeFilter
		name       string
		wantErrors bool
	}{
		{
			name:   "action_only",
			filter: &organizationstypes.HandshakeFilter{ActionType: organizationstypes.ActionType("INVITE")},
		},
		{name: "parent_only", filter: &organizationstypes.HandshakeFilter{ParentHandshakeId: aws.String("h-12345678")}},
		{
			name: "both_rejected",
			filter: &organizationstypes.HandshakeFilter{
				ActionType: organizationstypes.ActionType("INVITE"), ParentHandshakeId: aws.String("h-12345678"),
			},
			wantErrors: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newRealClient(t)

			_, errOrg := client.ListHandshakesForOrganization(
				t.Context(),
				&organizationssdk.ListHandshakesForOrganizationInput{Filter: tt.filter},
			)
			_, errAcct := client.ListHandshakesForAccount(
				t.Context(),
				&organizationssdk.ListHandshakesForAccountInput{Filter: tt.filter},
			)

			if !tt.wantErrors {
				require.NoError(t, errOrg)
				require.NoError(t, errAcct)

				return
			}

			for _, err := range []error{errOrg, errAcct} {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
			}
		})
	}
}
