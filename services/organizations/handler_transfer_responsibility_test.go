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
// See TestResponsibilityTransfer_RoundTrip for the five sibling ops
// (DescribeResponsibilityTransfer/UpdateResponsibilityTransfer/
// TerminateResponsibilityTransfer/ListInboundResponsibilityTransfers/
// ListOutboundResponsibilityTransfers), which serialize the real
// types.ResponsibilityTransfer shape under the "ResponsibilityTransfer"/
// "ResponsibilityTransfers" envelope key rather than a Handshake.
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

	outbound, err := backend.ListOutboundResponsibilityTransfers("BILLING")
	require.NoError(t, err)
	require.Len(t, outbound, 1)
	assert.Equal(t, handshakeID, outbound[0].ActiveHandshakeID)

	inbound, err := backend.ListInboundResponsibilityTransfers("BILLING", "")
	require.NoError(t, err)
	assert.Empty(t, inbound)
}

// TestResponsibilityTransfer_RoundTrip drives the real aws-sdk-go-v2 client
// through ListOutboundResponsibilityTransfers, DescribeResponsibilityTransfer,
// UpdateResponsibilityTransfer, TerminateResponsibilityTransfer, and
// ListInboundResponsibilityTransfers, proving each decodes the real
// types.ResponsibilityTransfer shape (Name/Status/Type/Source/Target/
// ActiveHandshakeId) rather than the pre-fix Handshake-shaped body under the
// wrong "HandshakeDetails" envelope key, which the real SDK client would
// either fail to find (nil ResponsibilityTransfer) or decode as all zeros
// beyond Id/Arn.
func TestResponsibilityTransfer_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := organizations.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestOrganizationsClient(t, organizations.NewHandler(backend))

	_, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{})
	require.NoError(t, err)

	start := time.Now().Add(24 * time.Hour).Truncate(time.Second)

	_, err = client.InviteOrganizationToTransferResponsibility(
		t.Context(),
		&organizationssdk.InviteOrganizationToTransferResponsibilityInput{
			Target: &organizationstypes.HandshakeParty{
				Id:   aws.String("999999999999"),
				Type: organizationstypes.HandshakePartyTypeAccount,
			},
			SourceName:     aws.String("billing-transfer"),
			StartTimestamp: aws.Time(start),
			Type:           organizationstypes.ResponsibilityTransferTypeBilling,
		},
	)
	require.NoError(t, err)

	listOut, err := client.ListOutboundResponsibilityTransfers(
		t.Context(),
		&organizationssdk.ListOutboundResponsibilityTransfersInput{
			Type: organizationstypes.ResponsibilityTransferTypeBilling,
		},
	)
	require.NoError(t, err)
	require.Len(t, listOut.ResponsibilityTransfers, 1)

	rt := listOut.ResponsibilityTransfers[0]
	assert.Equal(t, "billing-transfer", aws.ToString(rt.Name))
	assert.Equal(t, organizationstypes.ResponsibilityTransferStatusRequested, rt.Status)
	assert.Equal(t, organizationstypes.ResponsibilityTransferTypeBilling, rt.Type)
	require.NotNil(t, rt.Source)
	assert.Equal(t, "000000000000", aws.ToString(rt.Source.ManagementAccountId))
	require.NotNil(t, rt.Target)
	assert.Equal(t, "999999999999", aws.ToString(rt.Target.ManagementAccountId))
	assert.NotEmpty(t, aws.ToString(rt.ActiveHandshakeId))
	assert.Nil(t, rt.EndTimestamp)

	transferID := aws.ToString(rt.Id)
	require.NotEmpty(t, transferID)

	descOut, err := client.DescribeResponsibilityTransfer(
		t.Context(),
		&organizationssdk.DescribeResponsibilityTransferInput{Id: aws.String(transferID)},
	)
	require.NoError(t, err)
	require.NotNil(t, descOut.ResponsibilityTransfer)
	assert.Equal(t, transferID, aws.ToString(descOut.ResponsibilityTransfer.Id))
	assert.Equal(t, "billing-transfer", aws.ToString(descOut.ResponsibilityTransfer.Name))

	updOut, err := client.UpdateResponsibilityTransfer(t.Context(), &organizationssdk.UpdateResponsibilityTransferInput{
		Id:   aws.String(transferID),
		Name: aws.String("renamed-transfer"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed-transfer", aws.ToString(updOut.ResponsibilityTransfer.Name))

	// A still-REQUESTED transfer cannot be terminated yet.
	_, err = client.TerminateResponsibilityTransfer(
		t.Context(),
		&organizationssdk.TerminateResponsibilityTransferInput{Id: aws.String(transferID)},
	)
	require.Error(t, err)

	_, err = client.AcceptHandshake(t.Context(), &organizationssdk.AcceptHandshakeInput{
		HandshakeId: descOut.ResponsibilityTransfer.ActiveHandshakeId,
	})
	require.NoError(t, err)

	termOut, err := client.TerminateResponsibilityTransfer(
		t.Context(),
		&organizationssdk.TerminateResponsibilityTransferInput{Id: aws.String(transferID)},
	)
	require.NoError(t, err)
	require.NotNil(t, termOut.ResponsibilityTransfer.EndTimestamp)

	inboundOut, err := client.ListInboundResponsibilityTransfers(
		t.Context(),
		&organizationssdk.ListInboundResponsibilityTransfersInput{
			Type: organizationstypes.ResponsibilityTransferTypeBilling,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, inboundOut.ResponsibilityTransfers)
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
	targetMissingType := map[string]any{"Target": map[string]any{"Id": "999999999999", "Type": ""}}
	validSourceName := map[string]any{"SourceName": "billing-transfer"}
	validStart := map[string]any{"StartTimestamp": float64(time.Now().Add(time.Hour).Unix())}
	validType := map[string]any{"Type": "BILLING"}

	tests := map[string]map[string]any{
		"missing target":          merge(validSourceName, validStart, validType),
		"missing target type":     merge(targetMissingType, validSourceName, validStart, validType),
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
