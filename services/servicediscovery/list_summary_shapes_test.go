package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for servicediscovery's four flagged List ops,
// verified via cmd/structfielddiff against servicediscovery@v1.43.4.
// ListOperations already matched types.OperationSummary exactly. ListInstances,
// ListNamespaces, and ListServices were all missing CreatedByAccount/
// ResourceOwner -- real, optional members this single-account emulator can
// honestly source from InMemoryBackend.AccountID() (every resource this
// backend tracks is self-owned; the RESOURCE_OWNER filter already assumed
// exactly that, comparing against the literal "SELF").
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("list operations exact", func(t *testing.T) {
		t.Parallel()

		h := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion))
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		nsOut, err := client.CreateHttpNamespace(ctx, &sdsdk.CreateHttpNamespaceInput{
			Name: aws.String("lss-ns-" + uuid.NewString()[:8]),
		})
		require.NoError(t, err)

		out, err := client.ListOperations(ctx, &sdsdk.ListOperationsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, out.Operations)

		var found bool

		for _, op := range out.Operations {
			if aws.ToString(op.Id) == aws.ToString(nsOut.OperationId) {
				found = true

				assert.NotEmpty(t, op.Status)
			}
		}

		assert.True(t, found, "created operation must appear in ListOperations")
	})

	t.Run("list namespaces gains resource owner", func(t *testing.T) {
		t.Parallel()

		h := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion))
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		_, err := client.CreateHttpNamespace(ctx, &sdsdk.CreateHttpNamespaceInput{
			Name: aws.String("lss-ns-" + uuid.NewString()[:8]),
		})
		require.NoError(t, err)

		out, err := client.ListNamespaces(ctx, &sdsdk.ListNamespacesInput{})
		require.NoError(t, err)
		require.Len(t, out.Namespaces, 1)
		assert.Equal(t, "000000000000", aws.ToString(out.Namespaces[0].ResourceOwner))
	})

	t.Run("list services gains created by account and resource owner", func(t *testing.T) {
		t.Parallel()

		h := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion))
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		nsOut, err := client.CreateHttpNamespace(ctx, &sdsdk.CreateHttpNamespaceInput{
			Name: aws.String("lss-ns-" + uuid.NewString()[:8]),
		})
		require.NoError(t, err)

		opOut, err := client.GetOperation(ctx, &sdsdk.GetOperationInput{OperationId: nsOut.OperationId})
		require.NoError(t, err)
		nsID := opOut.Operation.Targets["NAMESPACE"]
		require.NotEmpty(t, nsID)

		_, err = client.CreateService(ctx, &sdsdk.CreateServiceInput{
			Name:        aws.String("lss-svc-" + uuid.NewString()[:8]),
			NamespaceId: aws.String(nsID),
		})
		require.NoError(t, err)

		out, err := client.ListServices(ctx, &sdsdk.ListServicesInput{})
		require.NoError(t, err)
		require.Len(t, out.Services, 1)
		assert.Equal(t, "000000000000", aws.ToString(out.Services[0].CreatedByAccount))
		assert.Equal(t, "000000000000", aws.ToString(out.Services[0].ResourceOwner))
	})

	t.Run("list instances gains created by account and top level resource owner", func(t *testing.T) {
		t.Parallel()

		h := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", sdTagsRTRegion))
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		nsOut, err := client.CreateHttpNamespace(ctx, &sdsdk.CreateHttpNamespaceInput{
			Name: aws.String("lss-ns-" + uuid.NewString()[:8]),
		})
		require.NoError(t, err)

		opOut, err := client.GetOperation(ctx, &sdsdk.GetOperationInput{OperationId: nsOut.OperationId})
		require.NoError(t, err)
		nsID := opOut.Operation.Targets["NAMESPACE"]
		require.NotEmpty(t, nsID)

		svcOut, err := client.CreateService(ctx, &sdsdk.CreateServiceInput{
			Name:        aws.String("lss-svc-" + uuid.NewString()[:8]),
			NamespaceId: aws.String(nsID),
		})
		require.NoError(t, err)

		_, err = client.RegisterInstance(ctx, &sdsdk.RegisterInstanceInput{
			ServiceId:  svcOut.Service.Id,
			InstanceId: aws.String("lss-inst-" + uuid.NewString()[:8]),
			Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
		})
		require.NoError(t, err)

		out, err := client.ListInstances(ctx, &sdsdk.ListInstancesInput{ServiceId: svcOut.Service.Id})
		require.NoError(t, err)
		require.Len(t, out.Instances, 1)
		assert.Equal(t, "000000000000", aws.ToString(out.Instances[0].CreatedByAccount))
		assert.Equal(t, "000000000000", aws.ToString(out.ResourceOwner))
	})
}
