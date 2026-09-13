package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestNamespaceAndService creates an HTTP namespace and a service in
// it via the real typed client, returning both IDs.
func createTestNamespaceAndService(
	t *testing.T,
	client *sdsdk.Client,
	nsName, svcName string,
) (string, string) {
	t.Helper()

	ctx := t.Context()

	ns, err := client.CreateHttpNamespace(
		ctx,
		&sdsdk.CreateHttpNamespaceInput{Name: aws.String(nsName)},
	)
	require.NoError(t, err)

	nsOp, err := client.GetOperation(ctx, &sdsdk.GetOperationInput{OperationId: ns.OperationId})
	require.NoError(t, err)
	nsID := nsOp.Operation.Targets["NAMESPACE"]
	require.NotEmpty(t, nsID)

	svc, err := client.CreateService(ctx, &sdsdk.CreateServiceInput{
		Name:        aws.String(svcName),
		NamespaceId: aws.String(nsID),
	})
	require.NoError(t, err)

	return nsID, aws.ToString(svc.Service.Id)
}

// TestServiceDiscovery_TypedSlice25 drives every remaining uncovered op
// through a real aws-sdk-go-v2 servicediscovery client: DeleteNamespace,
// DeleteService, DeleteServiceAttributes, DeregisterInstance, GetInstance,
// GetInstancesHealthStatus, ListInstances, ListNamespaces, ListOperations,
// ListServices, TagResource, UntagResource, UpdateHttpNamespace,
// UpdateInstanceCustomHealthStatus, UpdateService.
func TestServiceDiscovery_TypedSlice25(t *testing.T) {
	t.Parallel()

	t.Run(
		"instance lifecycle: GetInstance, ListInstances, health status, DeregisterInstance",
		func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestServiceDiscoveryClient(t, h)
			ctx := t.Context()

			ns, err := client.CreateHttpNamespace(
				ctx,
				&sdsdk.CreateHttpNamespaceInput{Name: aws.String("ns-inst-slice25")},
			)
			require.NoError(t, err)

			nsOp, err := client.GetOperation(
				ctx,
				&sdsdk.GetOperationInput{OperationId: ns.OperationId},
			)
			require.NoError(t, err)
			nsID := nsOp.Operation.Targets["NAMESPACE"]
			require.NotEmpty(t, nsID)

			// HealthCheckCustomConfig is required for UpdateInstanceCustomHealthStatus
			// -- real AWS returns CustomHealthNotFound otherwise.
			svc, err := client.CreateService(ctx, &sdsdk.CreateServiceInput{
				Name:                    aws.String("svc-inst-slice25"),
				NamespaceId:             aws.String(nsID),
				HealthCheckCustomConfig: &sdtypes.HealthCheckCustomConfig{},
			})
			require.NoError(t, err)
			svcID := aws.ToString(svc.Service.Id)

			_, err = client.RegisterInstance(ctx, &sdsdk.RegisterInstanceInput{
				ServiceId:  aws.String(svcID),
				InstanceId: aws.String("inst-1"),
				Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.5"},
			})
			require.NoError(t, err)

			got, err := client.GetInstance(ctx, &sdsdk.GetInstanceInput{
				ServiceId:  aws.String(svcID),
				InstanceId: aws.String("inst-1"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.Instance)
			assert.Equal(t, "inst-1", aws.ToString(got.Instance.Id))
			assert.Equal(t, "10.0.0.5", got.Instance.Attributes["AWS_INSTANCE_IPV4"])

			listOut, err := client.ListInstances(
				ctx,
				&sdsdk.ListInstancesInput{ServiceId: aws.String(svcID)},
			)
			require.NoError(t, err)
			require.Len(t, listOut.Instances, 1)
			assert.Equal(t, "inst-1", aws.ToString(listOut.Instances[0].Id))

			_, err = client.UpdateInstanceCustomHealthStatus(
				ctx,
				&sdsdk.UpdateInstanceCustomHealthStatusInput{
					ServiceId:  aws.String(svcID),
					InstanceId: aws.String("inst-1"),
					Status:     sdtypes.CustomHealthStatusUnhealthy,
				},
			)
			require.NoError(t, err)

			healthOut, err := client.GetInstancesHealthStatus(
				ctx,
				&sdsdk.GetInstancesHealthStatusInput{
					ServiceId: aws.String(svcID),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, sdtypes.HealthStatusUnhealthy, healthOut.Status["inst-1"])

			_, err = client.DeregisterInstance(ctx, &sdsdk.DeregisterInstanceInput{
				ServiceId:  aws.String(svcID),
				InstanceId: aws.String("inst-1"),
			})
			require.NoError(t, err)

			listOut, err = client.ListInstances(
				ctx,
				&sdsdk.ListInstancesInput{ServiceId: aws.String(svcID)},
			)
			require.NoError(t, err)
			assert.Empty(t, listOut.Instances)
		},
	)

	t.Run(
		"namespace lifecycle: ListNamespaces, UpdateHttpNamespace, DeleteNamespace",
		func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestServiceDiscoveryClient(t, h)
			ctx := t.Context()

			ns, err := client.CreateHttpNamespace(
				ctx,
				&sdsdk.CreateHttpNamespaceInput{Name: aws.String("ns-lifecycle-slice25")},
			)
			require.NoError(t, err)

			nsOp, err := client.GetOperation(
				ctx,
				&sdsdk.GetOperationInput{OperationId: ns.OperationId},
			)
			require.NoError(t, err)
			nsID := nsOp.Operation.Targets["NAMESPACE"]
			require.NotEmpty(t, nsID)

			listOut, err := client.ListNamespaces(ctx, &sdsdk.ListNamespacesInput{})
			require.NoError(t, err)
			var found bool
			for _, n := range listOut.Namespaces {
				if aws.ToString(n.Id) == nsID {
					found = true
				}
			}
			assert.True(t, found, "created namespace must appear in ListNamespaces")

			updOp, err := client.UpdateHttpNamespace(ctx, &sdsdk.UpdateHttpNamespaceInput{
				Id: aws.String(nsID),
				Namespace: &sdtypes.HttpNamespaceChange{
					Description: aws.String("updated via typed client"),
				},
			})
			require.NoError(t, err)

			updGetOp, err := client.GetOperation(
				ctx,
				&sdsdk.GetOperationInput{OperationId: updOp.OperationId},
			)
			require.NoError(t, err)
			assert.Equal(t, sdtypes.OperationStatusSuccess, updGetOp.Operation.Status)

			getNS, err := client.GetNamespace(ctx, &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
			require.NoError(t, err)
			assert.Equal(t, "updated via typed client", aws.ToString(getNS.Namespace.Description))

			delOp, err := client.DeleteNamespace(
				ctx,
				&sdsdk.DeleteNamespaceInput{Id: aws.String(nsID)},
			)
			require.NoError(t, err)
			require.NotNil(t, delOp.OperationId)

			_, err = client.GetNamespace(ctx, &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
			require.Error(t, err)
		},
	)

	t.Run(
		"service lifecycle: ListServices, UpdateService, DeleteServiceAttributes, DeleteService",
		func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestServiceDiscoveryClient(t, h)
			ctx := t.Context()

			_, svcID := createTestNamespaceAndService(
				t,
				client,
				"ns-svc-slice25",
				"svc-lifecycle-slice25",
			)

			listOut, err := client.ListServices(ctx, &sdsdk.ListServicesInput{})
			require.NoError(t, err)
			var found bool
			for _, s := range listOut.Services {
				if aws.ToString(s.Id) == svcID {
					found = true
				}
			}
			assert.True(t, found, "created service must appear in ListServices")

			_, err = client.UpdateService(ctx, &sdsdk.UpdateServiceInput{
				Id: aws.String(svcID),
				Service: &sdtypes.ServiceChange{
					Description: aws.String("updated service description"),
				},
			})
			require.NoError(t, err)

			got, err := client.GetService(ctx, &sdsdk.GetServiceInput{Id: aws.String(svcID)})
			require.NoError(t, err)
			assert.Equal(t, "updated service description", aws.ToString(got.Service.Description))

			_, err = client.UpdateServiceAttributes(ctx, &sdsdk.UpdateServiceAttributesInput{
				ServiceId:  aws.String(svcID),
				Attributes: map[string]string{"key1": "value1"},
			})
			require.NoError(t, err)

			_, err = client.DeleteServiceAttributes(ctx, &sdsdk.DeleteServiceAttributesInput{
				ServiceId:  aws.String(svcID),
				Attributes: []string{"key1"},
			})
			require.NoError(t, err)

			attrsOut, err := client.GetServiceAttributes(
				ctx,
				&sdsdk.GetServiceAttributesInput{ServiceId: aws.String(svcID)},
			)
			require.NoError(t, err)
			assert.NotContains(t, attrsOut.ServiceAttributes.Attributes, "key1")

			_, err = client.DeleteService(ctx, &sdsdk.DeleteServiceInput{Id: aws.String(svcID)})
			require.NoError(t, err)

			_, err = client.GetService(ctx, &sdsdk.GetServiceInput{Id: aws.String(svcID)})
			require.Error(t, err)
		},
	)

	t.Run("ListOperations", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		ns, err := client.CreateHttpNamespace(
			ctx,
			&sdsdk.CreateHttpNamespaceInput{Name: aws.String("ns-ops-slice25")},
		)
		require.NoError(t, err)
		opID := aws.ToString(ns.OperationId)
		require.NotEmpty(t, opID)

		listOut, err := client.ListOperations(ctx, &sdsdk.ListOperationsInput{})
		require.NoError(t, err)
		var found bool
		for _, op := range listOut.Operations {
			if aws.ToString(op.Id) == opID {
				found = true
				assert.Equal(t, sdtypes.OperationStatusSuccess, op.Status)
			}
		}
		assert.True(t, found, "created namespace's operation must appear in ListOperations")
	})

	t.Run("Tag and Untag a namespace resource", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestServiceDiscoveryClient(t, h)
		ctx := t.Context()

		ns, err := client.CreateHttpNamespace(
			ctx,
			&sdsdk.CreateHttpNamespaceInput{Name: aws.String("ns-tags-slice25")},
		)
		require.NoError(t, err)

		nsOp, err := client.GetOperation(ctx, &sdsdk.GetOperationInput{OperationId: ns.OperationId})
		require.NoError(t, err)
		nsID := nsOp.Operation.Targets["NAMESPACE"]
		require.NotEmpty(t, nsID)

		got, err := client.GetNamespace(ctx, &sdsdk.GetNamespaceInput{Id: aws.String(nsID)})
		require.NoError(t, err)
		nsArn := aws.ToString(got.Namespace.Arn)
		require.NotEmpty(t, nsArn)

		_, err = client.TagResource(ctx, &sdsdk.TagResourceInput{
			ResourceARN: aws.String(nsArn),
			Tags:        []sdtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		})
		require.NoError(t, err)

		tagsOut, err := client.ListTagsForResource(
			ctx,
			&sdsdk.ListTagsForResourceInput{ResourceARN: aws.String(nsArn)},
		)
		require.NoError(t, err)
		require.Len(t, tagsOut.Tags, 1)
		assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))

		_, err = client.UntagResource(ctx, &sdsdk.UntagResourceInput{
			ResourceARN: aws.String(nsArn),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		tagsOut, err = client.ListTagsForResource(
			ctx,
			&sdsdk.ListTagsForResourceInput{ResourceARN: aws.String(nsArn)},
		)
		require.NoError(t, err)
		assert.Empty(t, tagsOut.Tags)
	})
}
