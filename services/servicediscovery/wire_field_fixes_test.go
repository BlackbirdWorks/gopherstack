package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestUpdatePrivateDnsNamespace_SOATTL drives CreatePrivateDnsNamespace/
// UpdatePrivateDnsNamespace/GetNamespace through the real SDK client. Before
// the fix, UpdatePrivateDnsNamespaceInput.Namespace.Properties.DnsProperties.
// SOA.TTL (types.PrivateDnsNamespaceChange ->
// PrivateDnsNamespacePropertiesChange -> PrivateDnsPropertiesMutableChange ->
// SOAChange.TTL, servicediscovery@v1.43.4 types.go:923-975) was entirely
// absent from the wire-decode struct -- only Description was read -- so a
// real client's documented way to change a namespace's SOA TTL after
// creation was silently dropped and the original create-time value stuck
// forever.
func TestUpdatePrivateDnsNamespace_SOATTL(t *testing.T) {
	t.Parallel()

	backend := servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion)
	client := newTestServiceDiscoveryClient(t, servicediscovery.NewHandler(backend))

	createOp, err := client.CreatePrivateDnsNamespace(t.Context(), &sdsdk.CreatePrivateDnsNamespaceInput{
		Name: aws.String("soa-ttl-private.example"),
		Vpc:  aws.String("vpc-12345"),
		Properties: &sdtypes.PrivateDnsNamespaceProperties{
			DnsProperties: &sdtypes.PrivateDnsPropertiesMutable{
				SOA: &sdtypes.SOA{TTL: aws.Int64(100)},
			},
		},
	})
	require.NoError(t, err)

	nsID := waitForNamespaceID(t, client, aws.ToString(createOp.OperationId))

	before, err := client.GetNamespace(t.Context(), &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
	require.NoError(t, err)
	require.NotNil(t, before.Namespace.Properties.DnsProperties.SOA)
	require.Equal(t, int64(100), aws.ToInt64(before.Namespace.Properties.DnsProperties.SOA.TTL))

	_, err = client.UpdatePrivateDnsNamespace(t.Context(), &sdsdk.UpdatePrivateDnsNamespaceInput{
		Id: aws.String(nsID),
		Namespace: &sdtypes.PrivateDnsNamespaceChange{
			Description: aws.String("updated"),
			Properties: &sdtypes.PrivateDnsNamespacePropertiesChange{
				DnsProperties: &sdtypes.PrivateDnsPropertiesMutableChange{
					SOA: &sdtypes.SOAChange{TTL: aws.Int64(250)},
				},
			},
		},
	})
	require.NoError(t, err)

	after, err := client.GetNamespace(t.Context(), &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
	require.NoError(t, err)
	require.Equal(t, "updated", aws.ToString(after.Namespace.Description))
	require.NotNil(t, after.Namespace.Properties.DnsProperties.SOA)
	require.Equal(t, int64(250), aws.ToInt64(after.Namespace.Properties.DnsProperties.SOA.TTL))
}

// TestUpdatePublicDnsNamespace_SOATTL is TestUpdatePrivateDnsNamespace_SOATTL's
// sibling for public DNS namespaces (types.PublicDnsNamespaceChange ->
// PublicDnsNamespacePropertiesChange -> PublicDnsPropertiesMutableChange ->
// SOAChange.TTL, types.go:981-1033) -- same wire-decode gap, fixed
// separately since Create/UpdatePrivateDnsNamespace and
// Create/UpdatePublicDnsNamespace are distinct handlers.
func TestUpdatePublicDnsNamespace_SOATTL(t *testing.T) {
	t.Parallel()

	backend := servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion)
	client := newTestServiceDiscoveryClient(t, servicediscovery.NewHandler(backend))

	createOp, err := client.CreatePublicDnsNamespace(t.Context(), &sdsdk.CreatePublicDnsNamespaceInput{
		Name: aws.String("soa-ttl-public.example"),
		Properties: &sdtypes.PublicDnsNamespaceProperties{
			DnsProperties: &sdtypes.PublicDnsPropertiesMutable{
				SOA: &sdtypes.SOA{TTL: aws.Int64(60)},
			},
		},
	})
	require.NoError(t, err)

	nsID := waitForNamespaceID(t, client, aws.ToString(createOp.OperationId))

	_, err = client.UpdatePublicDnsNamespace(t.Context(), &sdsdk.UpdatePublicDnsNamespaceInput{
		Id: aws.String(nsID),
		Namespace: &sdtypes.PublicDnsNamespaceChange{
			Properties: &sdtypes.PublicDnsNamespacePropertiesChange{
				DnsProperties: &sdtypes.PublicDnsPropertiesMutableChange{
					SOA: &sdtypes.SOAChange{TTL: aws.Int64(999)},
				},
			},
		},
	})
	require.NoError(t, err)

	after, err := client.GetNamespace(t.Context(), &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
	require.NoError(t, err)
	require.NotNil(t, after.Namespace.Properties.DnsProperties.SOA)
	require.Equal(t, int64(999), aws.ToInt64(after.Namespace.Properties.DnsProperties.SOA.TTL))
}

// waitForNamespaceID resolves the namespace ID created by opID via
// GetOperation. This backend completes every operation synchronously, so a
// single poll suffices.
func waitForNamespaceID(t *testing.T, client *sdsdk.Client, opID string) string {
	t.Helper()

	op, err := client.GetOperation(t.Context(), &sdsdk.GetOperationInput{OperationId: aws.String(opID)})
	require.NoError(t, err)
	require.Equal(t, sdtypes.OperationStatusSuccess, op.Operation.Status)

	nsID, ok := op.Operation.Targets[string(sdtypes.OperationTargetTypeNamespace)]
	require.True(t, ok, "operation targets missing NAMESPACE key")

	return nsID
}
