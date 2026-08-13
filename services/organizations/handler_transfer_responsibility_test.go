package organizations_test

import (
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestInviteOrganizationToTransferResponsibility_RoundTrip drives the real
// aws-sdk-go-v2 organizations client through InviteOrganizationToTransferResponsibility
// and confirms the required SourceName/StartTimestamp/Type members
// (api_op_InviteOrganizationToTransferResponsibility.go:33-56) are actually
// wired: the resulting Handshake carries the TRANSFER_RESPONSIBILITY action
// (not the pre-fix APPROVE_ALL_FEATURES bug) and its SourceName/StartTimestamp/
// Type/Notes are stored as HandshakeResource entries.
//
// This deliberately does NOT call ListOutboundResponsibilityTransfers/
// ListInboundResponsibilityTransfers through the real SDK client: those two
// ops (and DescribeResponsibilityTransfer/UpdateResponsibilityTransfer/
// TerminateResponsibilityTransfer) serialize a Handshake-shaped body under
// the correct "ResponsibilityTransfers"/"ResponsibilityTransfer" envelope
// key, but real AWS's actual element type there is types.ResponsibilityTransfer
// -- a distinct shape (ActiveHandshakeId/Arn/EndTimestamp/Id/Name/Source/
// StartTimestamp/Status/Target/Type, verified against
// awsAwsjson11_deserializeDocumentResponsibilityTransfer, deserializers.go) --
// so the real SDK client would silently decode only the two overlapping key
// names (Id, Arn) and leave every other field zero. That's a structural bug
// spanning 5 sibling operations, out of scope for this fix (see PARITY.md and
// the follow-up bd issue) -- asserting against it here would be a
// false-positive test. ListOutboundResponsibilityTransfers is instead
// verified directly against the backend below.
func TestInviteOrganizationToTransferResponsibility_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := organizations.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestOrganizationsClient(t, organizations.NewHandler(backend))

	_, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{})
	require.NoError(t, err)

	start := time.Now().Add(24 * time.Hour).Truncate(time.Second)

	out, err := client.InviteOrganizationToTransferResponsibility(
		t.Context(),
		&organizationssdk.InviteOrganizationToTransferResponsibilityInput{
			Target: &organizationstypes.HandshakeParty{
				Id:   aws.String("999999999999"),
				Type: organizationstypes.HandshakePartyTypeAccount,
			},
			SourceName:     aws.String("billing-transfer"),
			StartTimestamp: aws.Time(start),
			Type:           organizationstypes.ResponsibilityTransferTypeBilling,
			Notes:          aws.String("please take over billing"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.Handshake)

	assert.Equal(t, organizationstypes.ActionTypeTransferResponsibility, out.Handshake.Action)

	values := resourceValuesByType(out.Handshake.Resources)
	assert.Equal(t, "billing-transfer", values["RESPONSIBILITY_TRANSFER"])
	assert.Equal(t, "BILLING", values["TRANSFER_TYPE"])
	assert.NotEmpty(t, values["TRANSFER_START_TIMESTAMP"])
	assert.Equal(t, "please take over billing", values["NOTES"])

	handshakeID := aws.ToString(out.Handshake.Id)

	outbound, err := backend.ListOutboundResponsibilityTransfers()
	require.NoError(t, err)
	require.Len(t, outbound, 1)
	assert.Equal(t, handshakeID, outbound[0].ID)
	assert.Equal(t, "TRANSFER_RESPONSIBILITY", outbound[0].Action)

	inbound, err := backend.ListInboundResponsibilityTransfers()
	require.NoError(t, err)
	assert.Empty(t, inbound)
}

// resourceValuesByType flattens a Handshake's top-level Resources into a
// type->value map for assertion convenience.
func resourceValuesByType(resources []organizationstypes.HandshakeResource) map[string]string {
	out := make(map[string]string, len(resources))
	for _, r := range resources {
		out[string(r.Type)] = aws.ToString(r.Value)
	}

	return out
}

func TestHandler_InviteOrganizationToTransferResponsibility_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	validTarget := map[string]any{"Target": map[string]any{"Id": "999999999999", "Type": "ACCOUNT"}}
	validSourceName := map[string]any{"SourceName": "billing-transfer"}
	validStart := map[string]any{"StartTimestamp": float64(time.Now().Add(time.Hour).Unix())}
	validType := map[string]any{"Type": "BILLING"}

	tests := map[string]map[string]any{
		"missing target":          merge(validSourceName, validStart, validType),
		"missing source name":     merge(validTarget, validStart, validType),
		"missing start timestamp": merge(validTarget, validSourceName, validType),
		"missing type":            merge(validTarget, validSourceName, validStart),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rec := doRequest(t, h, "InviteOrganizationToTransferResponsibility", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// merge shallow-merges any number of maps into a new map, later maps
// winning on key collision.
func merge(ms ...map[string]any) map[string]any {
	out := make(map[string]any)
	for _, m := range ms {
		maps.Copy(out, m)
	}

	return out
}
