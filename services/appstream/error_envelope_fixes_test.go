package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestCreateEntitlement_EntitlementAlreadyExists_RealClient drives
// CreateEntitlement twice for the same name/stack through the real client.
// CreateEntitlement's own error model (appstream@v1.64.5 deserializers.go
// rpc2_deserializeOpErrorCreateEntitlement) declares
// EntitlementAlreadyExistsException, not the shared ResourceAlreadyExistsException
// every other Create* op in this service uses (gopherstack-6flj/uox6
// error-envelope sweep).
func TestCreateEntitlement_EntitlementAlreadyExists_RealClient(t *testing.T) {
	t.Parallel()

	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	h := appstream.NewHandler(backend)
	client := newTestAppStreamClient(t, h)
	ctx := t.Context()

	createStack(t, h, "dup-entitlement-stack")

	in := &appstreamsdk.CreateEntitlementInput{
		Name:          aws.String("dup-entitlement"),
		StackName:     aws.String("dup-entitlement-stack"),
		AppVisibility: types.AppVisibilityAll,
		Attributes: []types.EntitlementAttribute{
			{Name: aws.String("roles"), Value: aws.String("admin")},
		},
	}

	_, err := client.CreateEntitlement(ctx, in)
	require.NoError(t, err)

	_, err = client.CreateEntitlement(ctx, in)
	require.Error(t, err)

	var apiErr *types.EntitlementAlreadyExistsException
	require.ErrorAs(t, err, &apiErr, "expected a real EntitlementAlreadyExistsException from the SDK deserializer")
}
