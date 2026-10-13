package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// attachedIGW creates an internet gateway attached to the default VPC.
func attachedIGW(t *testing.T, b ec2.Backend) string {
	t.Helper()

	igw, err := b.CreateInternetGateway()
	require.NoError(t, err)
	require.NoError(t, b.AttachInternetGateway(igw.ID, "vpc-default"))

	return igw.ID
}
