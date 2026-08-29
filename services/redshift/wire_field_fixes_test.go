package redshift_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestDescribeReservedNodeExchangeStatus_StatusIsLegalEnumMember drives
// DescribeReservedNodeExchangeStatus through the real aws-sdk-go-v2 client.
// ReservedNodeExchangeStatus.Status is types.ReservedNodeExchangeStatusType
// (REQUESTED/PENDING/IN_PROGRESS/RETRYING/SUCCEEDED/FAILED --
// redshift@v1.65.4 types/enums.go:468); the backend previously returned the
// bare string "Active" (borrowed from an unrelated PartnerIntegrationStatus
// constant), which is not a member of ReservedNodeExchangeStatusType, so a
// real client's waiter for an exchange request would never match any case
// and poll until timeout.
func TestDescribeReservedNodeExchangeStatus_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddReservedNodeInternal(&redshift.ReservedNode{
		ReservedNodeID: "rn-exchange",
		State:          "active",
	})
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))
	ctx := t.Context()

	out, err := client.DescribeReservedNodeExchangeStatus(ctx, &redshiftsdk.DescribeReservedNodeExchangeStatusInput{
		ReservedNodeId: aws.String("rn-exchange"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReservedNodeExchangeStatusDetails, 1)
	assert.Equal(t, types.ReservedNodeExchangeStatusTypeSucceeded, out.ReservedNodeExchangeStatusDetails[0].Status)
}
