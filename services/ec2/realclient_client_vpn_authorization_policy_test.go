package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const cvpnPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

func TestRealClient_ClientVpnEndpointAuthorizationPolicy(t *testing.T) {
	t.Parallel()

	newEndpoint := func(t *testing.T) (*ec2sdk.Client, string) {
		t.Helper()

		backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestEC2Client(t, ec2.NewHandler(backend))
		ep, err := backend.CreateClientVpnEndpointWithOptions(
			"10.10.0.0/22",
			"cvpn",
			nil,
			ec2.ClientVpnEndpointOptions{},
		)
		require.NoError(t, err)

		return client, ep.ClientVpnEndpointID
	}

	cases := []struct {
		run  func(t *testing.T, client *ec2sdk.Client, id string)
		name string
	}{
		{name: "create get merge delete", run: func(t *testing.T, client *ec2sdk.Client, id string) {
			t.Helper()

			ctx := t.Context()

			created, err := client.ModifyClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.ModifyClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
					PolicyDocument:      aws.String(cvpnPolicyDoc),
					Description:         aws.String("first"),
					ShadowMode:          types.ClientVpnAuthorizationPolicyShadowModeEnabled,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ClientVpnAuthorizationPolicyStatusCreating, created.Status)

			got, err := client.GetClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.GetClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, id, aws.ToString(got.ClientVpnEndpointId))
			assert.JSONEq(t, cvpnPolicyDoc, aws.ToString(got.PolicyDocument))
			assert.Equal(t, "first", aws.ToString(got.Description))
			assert.Equal(t, types.ClientVpnAuthorizationPolicyShadowModeEnabled, got.ShadowMode)
			assert.Equal(t, types.ClientVpnAuthorizationPolicyStatusActive, got.Status)

			upd, err := client.ModifyClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.ModifyClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
					Description:         aws.String("second"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ClientVpnAuthorizationPolicyStatusUpdating, upd.Status)

			merged, err := client.GetClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.GetClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "second", aws.ToString(merged.Description))
			assert.JSONEq(t, cvpnPolicyDoc, aws.ToString(merged.PolicyDocument), "unspecified values are kept")
			assert.Equal(t, types.ClientVpnAuthorizationPolicyShadowModeEnabled, merged.ShadowMode)

			del, err := client.DeleteClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.DeleteClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.ClientVpnAuthorizationPolicyStatusDeleting, del.Status)

			_, err = client.GetClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.GetClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
				},
			)
			require.ErrorContains(t, err, "InvalidClientVpnEndpointAuthorizationPolicy.NotFound")

			_, err = client.DeleteClientVpnEndpointAuthorizationPolicy(
				ctx,
				&ec2sdk.DeleteClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
				},
			)
			require.ErrorContains(t, err, "InvalidClientVpnEndpointAuthorizationPolicy.NotFound")
		}},
		{name: "unknown endpoint", run: func(t *testing.T, client *ec2sdk.Client, _ string) {
			t.Helper()

			_, err := client.GetClientVpnEndpointAuthorizationPolicy(
				t.Context(),
				&ec2sdk.GetClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String("cvpn-endpoint-0000000000000000"),
				},
			)
			require.ErrorContains(t, err, "InvalidClientVpnEndpointId.NotFound")
		}},
		{name: "invalid policy document", run: func(t *testing.T, client *ec2sdk.Client, id string) {
			t.Helper()

			_, err := client.ModifyClientVpnEndpointAuthorizationPolicy(
				t.Context(),
				&ec2sdk.ModifyClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
					PolicyDocument:      aws.String("not json"),
				},
			)
			require.ErrorContains(t, err, "InvalidParameterValue: PolicyDocument must be a JSON object")
		}},
		{name: "invalid shadow mode", run: func(t *testing.T, client *ec2sdk.Client, id string) {
			t.Helper()

			_, err := client.ModifyClientVpnEndpointAuthorizationPolicy(
				t.Context(),
				&ec2sdk.ModifyClientVpnEndpointAuthorizationPolicyInput{
					ClientVpnEndpointId: aws.String(id),
					ShadowMode:          types.ClientVpnAuthorizationPolicyShadowMode("sometimes"),
				},
			)
			require.ErrorContains(t, err, "InvalidParameterValue: ShadowMode must be enabled or disabled")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, id := newEndpoint(t)
			tc.run(t, client, id)
		})
	}
}

func TestClientVpnEndpointAuthorizationPolicy_SnapshotRestore(t *testing.T) {
	t.Parallel()

	src := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	ep, err := src.CreateClientVpnEndpointWithOptions("10.10.0.0/22", "cvpn", nil, ec2.ClientVpnEndpointOptions{})
	require.NoError(t, err)

	_, err = src.ModifyClientVpnEndpointAuthorizationPolicy(
		ep.ClientVpnEndpointID, aws.String("persisted"), aws.String(cvpnPolicyDoc), aws.String("enabled"))
	require.NoError(t, err)

	dst := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	got, err := dst.GetClientVpnEndpointAuthorizationPolicy(ep.ClientVpnEndpointID)
	require.NoError(t, err)
	assert.Equal(t, "persisted", got.Description)
	assert.Equal(t, "enabled", got.ShadowMode)
}
