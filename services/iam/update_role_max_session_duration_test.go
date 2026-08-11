package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestUpdateRole_MaxSessionDuration drives UpdateRole through the real SDK
// client (iam@v1.58.1, api_op_UpdateRole.go): the request carries
// MaxSessionDuration, but the handler only forwarded Description to the
// backend -- so a real client's value was silently discarded even though the
// backend already has UpdateRoleMaxSessionDuration wired for CreateRole's
// own optional MaxSessionDuration handling (handler_roles.go:23).
func TestUpdateRole_MaxSessionDuration(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName:                 aws.String("session-role"),
		AssumeRolePolicyDocument: aws.String("{}"),
	})
	require.NoError(t, err)

	_, err = client.UpdateRole(t.Context(), &iamsdk.UpdateRoleInput{
		RoleName:           aws.String("session-role"),
		MaxSessionDuration: aws.Int32(7200),
	})
	require.NoError(t, err)

	out, err := client.GetRole(t.Context(), &iamsdk.GetRoleInput{RoleName: aws.String("session-role")})
	require.NoError(t, err)
	assert.Equal(t, int32(7200), aws.ToInt32(out.Role.MaxSessionDuration))
}
