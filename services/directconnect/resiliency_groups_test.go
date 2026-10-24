package directconnect_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

func newResiliencyGroup(
	t *testing.T,
	client *directconnectsdk.Client,
	name string,
) *types.ResiliencyGroup {
	t.Helper()

	out, err := client.CreateResiliencyGroup(t.Context(), &directconnectsdk.CreateResiliencyGroupInput{
		ResiliencyGroupName:     aws.String(name),
		IntendedResiliencyModel: types.ResiliencyModelHighResiliency,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ResiliencyGroup)

	return out.ResiliencyGroup
}

func waitGroupState(
	t *testing.T,
	client *directconnectsdk.Client,
	id *string,
	want types.ResiliencyGroupState,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := client.GetResiliencyGroup(t.Context(), &directconnectsdk.GetResiliencyGroupInput{
			ResiliencyGroupId: id,
		})

		return err == nil && out.ResiliencyGroup.State == want
	}, defaultAsyncWait, defaultAsyncPoll)
}

func TestResiliencyGroupLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateResiliencyGroup(ctx, &directconnectsdk.CreateResiliencyGroupInput{
		ResiliencyGroupName:     aws.String("rg-one"),
		IntendedResiliencyModel: types.ResiliencyModelMaximumResiliency,
		Tags:                    []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	rg := created.ResiliencyGroup
	require.NotNil(t, rg)
	assert.NotEmpty(t, aws.ToString(rg.ResiliencyGroupId))
	assert.Equal(t, "rg-one", aws.ToString(rg.ResiliencyGroupName))
	assert.Equal(t, rtTestAccountID, aws.ToString(rg.OwnerAccount))
	assert.Equal(t, types.ResiliencyGroupTypeManaged, rg.ResiliencyGroupType)
	assert.Equal(t, types.ResiliencyGroupStatePending, rg.State)
	assert.Contains(t, aws.ToString(rg.ResiliencyGroupArn), aws.ToString(rg.ResiliencyGroupId))
	require.Len(t, rg.Tags, 1)

	waitGroupState(t, client, rg.ResiliencyGroupId, types.ResiliencyGroupStateAvailable)

	updated, err := client.UpdateResiliencyGroup(ctx, &directconnectsdk.UpdateResiliencyGroupInput{
		ResiliencyGroupId:   rg.ResiliencyGroupId,
		ResiliencyGroupName: aws.String("rg-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "rg-renamed", aws.ToString(updated.ResiliencyGroup.ResiliencyGroupName))

	list, err := client.ListResiliencyGroups(ctx, &directconnectsdk.ListResiliencyGroupsInput{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "rg-renamed", aws.ToString(list.Items[0].ResiliencyGroupName))

	deleted, err := client.DeleteResiliencyGroup(ctx, &directconnectsdk.DeleteResiliencyGroupInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ResiliencyGroupStateDeleting, deleted.ResiliencyGroup.State)

	waitGroupState(t, client, rg.ResiliencyGroupId, types.ResiliencyGroupStateDeleted)
}

func TestResiliencyGroupListPagination(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	for _, n := range []string{"a", "b", "c"} {
		newResiliencyGroup(t, client, n)
	}

	var names []string

	var token *string

	for range 5 {
		out, err := client.ListResiliencyGroups(ctx, &directconnectsdk.ListResiliencyGroupsInput{
			MaxResults: aws.Int32(2),
			NextToken:  token,
		})
		require.NoError(t, err)

		for _, it := range out.Items {
			names = append(names, aws.ToString(it.ResiliencyGroupName))
		}

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	assert.ElementsMatch(t, []string{"a", "b", "c"}, names)
}

func TestResiliencyGroupErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(ctx context.Context, c *directconnectsdk.Client) error
		name string
	}{
		{
			name: "get missing",
			call: func(ctx context.Context, c *directconnectsdk.Client) error {
				_, err := c.GetResiliencyGroup(ctx, &directconnectsdk.GetResiliencyGroupInput{
					ResiliencyGroupId: aws.String("dxrg-missing"),
				})

				return err
			},
		},
		{
			name: "update missing",
			call: func(ctx context.Context, c *directconnectsdk.Client) error {
				_, err := c.UpdateResiliencyGroup(ctx, &directconnectsdk.UpdateResiliencyGroupInput{
					ResiliencyGroupId:   aws.String("dxrg-missing"),
					ResiliencyGroupName: aws.String("x"),
				})

				return err
			},
		},
		{
			name: "delete missing",
			call: func(ctx context.Context, c *directconnectsdk.Client) error {
				_, err := c.DeleteResiliencyGroup(ctx, &directconnectsdk.DeleteResiliencyGroupInput{
					ResiliencyGroupId: aws.String("dxrg-missing"),
				})

				return err
			},
		},
		{
			name: "create bad model",
			call: func(ctx context.Context, c *directconnectsdk.Client) error {
				_, err := c.CreateResiliencyGroup(ctx, &directconnectsdk.CreateResiliencyGroupInput{
					ResiliencyGroupName:     aws.String("x"),
					IntendedResiliencyModel: types.ResiliencyModel("bogus"),
				})

				return err
			},
		},
		{
			name: "list associations of missing group",
			call: func(ctx context.Context, c *directconnectsdk.Client) error {
				_, err := c.ListResiliencyGroupAssociations(ctx, &directconnectsdk.ListResiliencyGroupAssociationsInput{
					ResiliencyGroupId: aws.String("dxrg-missing"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			err := tt.call(t.Context(), client)
			require.Error(t, err)

			var ce *types.DirectConnectClientException

			require.ErrorAs(t, err, &ce)
		})
	}
}

func TestResiliencyGroupDuplicateTagKeys(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.CreateResiliencyGroup(t.Context(), &directconnectsdk.CreateResiliencyGroupInput{
		ResiliencyGroupName:     aws.String("dup"),
		IntendedResiliencyModel: types.ResiliencyModelBasicResiliency,
		Tags: []types.Tag{
			{Key: aws.String("k"), Value: aws.String("1")},
			{Key: aws.String("k"), Value: aws.String("2")},
		},
	})

	var de *types.DuplicateTagKeysException

	require.ErrorAs(t, err, &de)
}

func TestResiliencyGroupClientTokenIdempotent(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	in := &directconnectsdk.CreateResiliencyGroupInput{
		ResiliencyGroupName:     aws.String("idem"),
		IntendedResiliencyModel: types.ResiliencyModelBasicResiliency,
		ClientToken:             aws.String("token-1"),
	}

	first, err := client.CreateResiliencyGroup(t.Context(), in)
	require.NoError(t, err)

	second, err := client.CreateResiliencyGroup(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.ResiliencyGroup.ResiliencyGroupId),
		aws.ToString(second.ResiliencyGroup.ResiliencyGroupId))

	list, err := client.ListResiliencyGroups(t.Context(), &directconnectsdk.ListResiliencyGroupsInput{})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)
}

func TestResiliencyGroupTagging(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	rg := newResiliencyGroup(t, client, "tagged")

	_, err := client.TagResource(ctx, &directconnectsdk.TagResourceInput{
		ResourceArn: rg.ResiliencyGroupArn,
		Tags:        []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTags(ctx, &directconnectsdk.DescribeTagsInput{
		ResourceArns: []string{aws.ToString(rg.ResiliencyGroupArn)},
	})
	require.NoError(t, err)
	require.Len(t, out.ResourceTags, 1)
	require.Len(t, out.ResourceTags[0].Tags, 1)

	got, err := client.GetResiliencyGroup(ctx, &directconnectsdk.GetResiliencyGroupInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})
	require.NoError(t, err)
	require.Len(t, got.ResiliencyGroup.Tags, 1)
}

func TestResiliencyGroupAssociations(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	rg := newResiliencyGroup(t, client, "assoc")
	waitGroupState(t, client, rg.ResiliencyGroupId, types.ResiliencyGroupStateAvailable)

	conn1 := createTestConnection(t, client)
	conn2 := createTestConnection(t, client)

	conn2ARN := "arn:aws:directconnect:us-east-1:" + rtTestAccountID + ":dxcon/" + aws.ToString(conn2.ConnectionId)

	assoc, err := client.AssociateConnectionsToResiliencyGroup(ctx,
		&directconnectsdk.AssociateConnectionsToResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{aws.ToString(conn1.ConnectionId), conn2ARN},
		})
	require.NoError(t, err)
	require.Len(t, assoc.ResiliencyGroupAssociations, 2)

	for _, a := range assoc.ResiliencyGroupAssociations {
		assert.Equal(t, types.ResiliencyGroupAssociationStateAssociating, a.State)
		assert.Equal(t, aws.ToString(rg.ResiliencyGroupId), aws.ToString(a.ResiliencyGroupId))
		assert.Contains(t, aws.ToString(a.ConnectionArn), ":dxcon/dxcon-")
	}

	require.Eventually(t, func() bool {
		out, listErr := client.ListResiliencyGroupAssociations(ctx,
			&directconnectsdk.ListResiliencyGroupAssociationsInput{ResiliencyGroupId: rg.ResiliencyGroupId})
		if listErr != nil || len(out.Items) != 2 {
			return false
		}

		for _, it := range out.Items {
			if it.State != types.ResiliencyGroupAssociationStateAssociated {
				return false
			}
		}

		return true
	}, defaultAsyncWait, defaultAsyncPoll)

	page1, err := client.ListResiliencyGroupAssociations(ctx, &directconnectsdk.ListResiliencyGroupAssociationsInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
		MaxResults:        aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 1)
	require.NotNil(t, page1.NextToken)

	_, err = client.DeleteResiliencyGroup(ctx, &directconnectsdk.DeleteResiliencyGroupInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})

	var ce *types.DirectConnectClientException

	require.ErrorAs(t, err, &ce, "group with associated connections cannot be deleted")

	dis, err := client.DisassociateConnectionsFromResiliencyGroup(ctx,
		&directconnectsdk.DisassociateConnectionsFromResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{aws.ToString(conn1.ConnectionId), aws.ToString(conn2.ConnectionId)},
		})
	require.NoError(t, err)
	require.Len(t, dis.ResiliencyGroupAssociations, 2)
	assert.Equal(t, types.ResiliencyGroupAssociationStateDisassociating, dis.ResiliencyGroupAssociations[0].State)

	require.Eventually(t, func() bool {
		out, listErr := client.ListResiliencyGroupAssociations(ctx,
			&directconnectsdk.ListResiliencyGroupAssociationsInput{ResiliencyGroupId: rg.ResiliencyGroupId})

		return listErr == nil && len(out.Items) == 0
	}, defaultAsyncWait, defaultAsyncPoll)

	_, err = client.DeleteResiliencyGroup(ctx, &directconnectsdk.DeleteResiliencyGroupInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})
	require.NoError(t, err)
}

func TestResiliencyGroupAssociationAtomicErrors(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	rg := newResiliencyGroup(t, client, "atomic")
	conn := createTestConnection(t, client)
	id := aws.ToString(conn.ConnectionId)

	var ce *types.DirectConnectClientException

	_, err := client.AssociateConnectionsToResiliencyGroup(ctx,
		&directconnectsdk.AssociateConnectionsToResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{id, "dxcon-doesnotexist"},
		})
	require.ErrorAs(t, err, &ce)

	out, err := client.ListResiliencyGroupAssociations(ctx, &directconnectsdk.ListResiliencyGroupAssociationsInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Items, "failed associate must not partially apply")

	_, err = client.AssociateConnectionsToResiliencyGroup(ctx,
		&directconnectsdk.AssociateConnectionsToResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{id},
		})
	require.NoError(t, err)

	_, err = client.AssociateConnectionsToResiliencyGroup(ctx,
		&directconnectsdk.AssociateConnectionsToResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{id},
		})
	require.ErrorAs(t, err, &ce, "already associated")

	_, err = client.DisassociateConnectionsFromResiliencyGroup(ctx,
		&directconnectsdk.DisassociateConnectionsFromResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{id, "dxcon-notmember"},
		})
	require.ErrorAs(t, err, &ce)

	out, err = client.ListResiliencyGroupAssociations(ctx, &directconnectsdk.ListResiliencyGroupAssociationsInput{
		ResiliencyGroupId: rg.ResiliencyGroupId,
	})
	require.NoError(t, err)
	assert.Len(t, out.Items, 1, "failed disassociate must not partially apply")
}

func TestResiliencyGroupPersistence(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	rg := newResiliencyGroup(t, client, "persisted")
	conn := createTestConnection(t, client)

	_, err := client.AssociateConnectionsToResiliencyGroup(ctx,
		&directconnectsdk.AssociateConnectionsToResiliencyGroupInput{
			ResiliencyGroupId:     rg.ResiliencyGroupId,
			ConnectionIdentifiers: []string{aws.ToString(conn.ConnectionId)},
		})
	require.NoError(t, err)

	snap := h.Backend.Snapshot(ctx)

	restored := directconnect.NewInMemoryBackend(ctx, rtTestAccountID, rtTestRegion)
	t.Cleanup(restored.Close)
	require.NoError(t, restored.Restore(ctx, snap))

	c2 := newRoundTripClient(t, directconnect.NewHandler(restored))

	waitGroupState(t, c2, rg.ResiliencyGroupId, types.ResiliencyGroupStateAvailable)

	require.Eventually(t, func() bool {
		out, listErr := c2.ListResiliencyGroupAssociations(ctx,
			&directconnectsdk.ListResiliencyGroupAssociationsInput{ResiliencyGroupId: rg.ResiliencyGroupId})

		return listErr == nil && len(out.Items) == 1 &&
			out.Items[0].State == types.ResiliencyGroupAssociationStateAssociated
	}, defaultAsyncWait, defaultAsyncPoll)
}

func TestUpdateConnectionsBillingMode(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	c1 := createTestConnection(t, client)
	c2 := createTestConnection(t, client)
	ids := []string{aws.ToString(c1.ConnectionId), aws.ToString(c2.ConnectionId)}

	out, err := client.UpdateConnectionsBillingMode(ctx, &directconnectsdk.UpdateConnectionsBillingModeInput{
		BillingMode:   types.RequestBillingModeFlatRateTier2,
		ConnectionIds: ids,
	})
	require.NoError(t, err)
	assert.Equal(t, types.BillingModeFlatRateTier2, out.BillingMode)
	require.Len(t, out.Connections, 2)

	for _, c := range out.Connections {
		assert.Equal(t, types.BillingModeFlatRateTier2, c.BillingMode)
	}

	desc, err := client.DescribeConnections(
		ctx,
		&directconnectsdk.DescribeConnectionsInput{ConnectionId: c1.ConnectionId},
	)
	require.NoError(t, err)
	require.Len(t, desc.Connections, 1)
	assert.Equal(t, types.BillingModeFlatRateTier2, desc.Connections[0].BillingMode)

	var ce *types.DirectConnectClientException

	_, err = client.UpdateConnectionsBillingMode(ctx, &directconnectsdk.UpdateConnectionsBillingModeInput{
		BillingMode:   types.RequestBillingModePayAsYouGo,
		ConnectionIds: []string{ids[0], "dxcon-doesnotexist"},
	})
	require.ErrorAs(t, err, &ce)

	desc, err = client.DescribeConnections(
		ctx,
		&directconnectsdk.DescribeConnectionsInput{ConnectionId: c1.ConnectionId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.BillingModeFlatRateTier2, desc.Connections[0].BillingMode, "failed update is atomic")

	_, err = client.UpdateConnectionsBillingMode(ctx, &directconnectsdk.UpdateConnectionsBillingModeInput{
		BillingMode:   types.RequestBillingMode("bogus"),
		ConnectionIds: ids,
	})
	require.ErrorAs(t, err, &ce)

	_, err = client.UpdateConnectionsBillingMode(ctx, &directconnectsdk.UpdateConnectionsBillingModeInput{
		BillingMode:   types.RequestBillingModePayAsYouGo,
		ConnectionIds: []string{},
	})
	require.ErrorAs(t, err, &ce)
}
