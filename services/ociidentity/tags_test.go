package ociidentity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagNamespacesAndTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	ns, err := e.client.CreateTagNamespace(ctx, identity.CreateTagNamespaceRequest{
		CreateTagNamespaceDetails: identity.CreateTagNamespaceDetails{
			CompartmentId: new(testTenancy), Name: new("Ops"), Description: new("ops tags"),
		},
	})
	require.NoError(t, err)
	assert.False(t, *ns.IsRetired)
	assert.Equal(t, identity.TagNamespaceLifecycleStateActive, ns.LifecycleState)

	_, err = e.client.CreateTagNamespace(ctx, identity.CreateTagNamespaceRequest{
		CreateTagNamespaceDetails: identity.CreateTagNamespaceDetails{
			CompartmentId: new(testTenancy), Name: new("ops"), Description: new("dup"),
		},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	tag, err := e.client.CreateTag(ctx, identity.CreateTagRequest{
		TagNamespaceId: ns.Id,
		CreateTagDetails: identity.CreateTagDetails{
			Name: new("Env"), Description: new("environment"), IsCostTracking: new(true),
			Validator: identity.EnumTagDefinitionValidator{Values: []string{"prod", "dev"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Ops", *tag.TagNamespaceName)
	assert.True(t, *tag.IsCostTracking)

	_, err = e.client.CreateTag(ctx, identity.CreateTagRequest{
		TagNamespaceId:   ns.Id,
		CreateTagDetails: identity.CreateTagDetails{Name: new("env"), Description: new("dup")},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	got, err := e.client.GetTag(ctx, identity.GetTagRequest{TagNamespaceId: ns.Id, TagName: new("Env")})
	require.NoError(t, err)
	assert.Equal(t, *tag.Id, *got.Id)

	tl, err := e.client.ListTags(ctx, identity.ListTagsRequest{TagNamespaceId: ns.Id})
	require.NoError(t, err)
	require.Len(t, tl.Items, 1)

	d, err := e.client.CreateTagDefault(ctx, identity.CreateTagDefaultRequest{
		CreateTagDefaultDetails: identity.CreateTagDefaultDetails{
			CompartmentId: new(testTenancy), TagDefinitionId: tag.Id, Value: new("prod"), IsRequired: new(true),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Env", *d.TagDefinitionName)
	assert.True(t, *d.IsRequired)

	_, err = e.client.CreateTagDefault(ctx, identity.CreateTagDefaultRequest{
		CreateTagDefaultDetails: identity.CreateTagDefaultDetails{
			CompartmentId: new(testTenancy), TagDefinitionId: tag.Id, Value: new("dev"),
		},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	c := e.mkCompartment(t, testTenancy, "tagged")

	_, err = e.client.CreateTagDefault(ctx, identity.CreateTagDefaultRequest{
		CreateTagDefaultDetails: identity.CreateTagDefaultDetails{
			CompartmentId: c.Id, TagDefinitionId: tag.Id, Value: new("staging"),
		},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	up, err := e.client.UpdateTagDefault(ctx, identity.UpdateTagDefaultRequest{
		TagDefaultId: d.Id, UpdateTagDefaultDetails: identity.UpdateTagDefaultDetails{Value: new("dev")},
	})
	require.NoError(t, err)
	assert.Equal(t, "dev", *up.Value)

	dl, err := e.client.ListTagDefaults(ctx, identity.ListTagDefaultsRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	assert.Len(t, dl.Items, 1)

	_, err = e.client.DeleteTagNamespace(ctx, identity.DeleteTagNamespaceRequest{TagNamespaceId: ns.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	ret, err := e.client.UpdateTag(ctx, identity.UpdateTagRequest{
		TagNamespaceId: ns.Id, TagName: new("Env"), UpdateTagDetails: identity.UpdateTagDetails{IsRetired: new(true)},
	})
	require.NoError(t, err)
	assert.Equal(t, identity.TagLifecycleStateInactive, ret.LifecycleState)

	del, err := e.client.DeleteTag(ctx, identity.DeleteTagRequest{TagNamespaceId: ns.Id, TagName: new("Env")})
	require.NoError(t, err)
	require.NotNil(t, del.OpcWorkRequestId)

	wr, err := e.client.GetWorkRequest(ctx, identity.GetWorkRequestRequest{WorkRequestId: del.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, identity.WorkRequestOperationTypeTagDefinition, wr.OperationType)

	e.clock.Advance(3 * time.Second)
	e.backend.Sweep()

	_, err = e.client.GetTag(ctx, identity.GetTagRequest{TagNamespaceId: ns.Id, TagName: new("Env")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	dl, err = e.client.ListTagDefaults(ctx, identity.ListTagDefaultsRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	assert.Empty(t, dl.Items)

	_, err = e.client.UpdateTagNamespace(ctx, identity.UpdateTagNamespaceRequest{
		TagNamespaceId: ns.Id, UpdateTagNamespaceDetails: identity.UpdateTagNamespaceDetails{IsRetired: new(true)},
	})
	require.NoError(t, err)

	nl, err := e.client.ListTagNamespaces(ctx, identity.ListTagNamespacesRequest{
		CompartmentId: new(testTenancy), LifecycleState: identity.TagNamespaceLifecycleStateInactive,
	})
	require.NoError(t, err)
	require.Len(t, nl.Items, 1)

	_, err = e.client.DeleteTagNamespace(ctx, identity.DeleteTagNamespaceRequest{TagNamespaceId: ns.Id})
	require.NoError(t, err)
}
