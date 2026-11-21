package ociidentity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func (e *env) mkTag(
	t *testing.T,
	nsName, tagName string,
	validator identity.BaseTagDefinitionValidator,
) (string, string) {
	t.Helper()

	ns, err := e.client.CreateTagNamespace(t.Context(), identity.CreateTagNamespaceRequest{
		CompartmentId: new(testTenancy), Name: new(nsName), Description: new("d"),
	})
	require.NoError(t, err)

	tag, err := e.client.CreateTag(t.Context(), identity.CreateTagRequest{
		TagNamespaceId: ns.Id,
		Name:           new(tagName), Description: new("d"), Validator: validator,
	})
	require.NoError(t, err)

	return *ns.Id, *tag.Id
}

func (e *env) bulkEdit(
	t *testing.T,
	resources []identity.BulkEditResource,
	ops ...identity.BulkEditOperationDetails,
) string {
	t.Helper()

	r, err := e.client.BulkEditTags(t.Context(), identity.BulkEditTagsRequest{
		CompartmentId: new(testTenancy), Resources: resources, BulkEditOperations: ops,
	})
	require.NoError(t, err)
	require.NotNil(t, r.OpcWorkRequestId)

	return *r.OpcWorkRequestId
}

func (e *env) taggingWR(t *testing.T, id string) identity.TaggingWorkRequest {
	t.Helper()

	r, err := e.client.GetTaggingWorkRequest(t.Context(), identity.GetTaggingWorkRequestRequest{WorkRequestId: new(id)})
	require.NoError(t, err)

	return r.TaggingWorkRequest
}

func TestListBulkEditTagsResourceTypes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	r, err := e.client.ListBulkEditTagsResourceTypes(t.Context(), identity.ListBulkEditTagsResourceTypesRequest{})
	require.NoError(t, err)

	names := make([]string, 0, len(r.Items))

	for _, it := range r.Items {
		names = append(names, *it.ResourceType)
	}

	assert.ElementsMatch(t, []string{"compartment", "dynamicgroup", "group", "policy", "tagnamespace", "user"}, names)
}

func TestBulkEditTagsLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	e.mkTag(t, "Ops", "Cost", nil)

	user, group := e.mkUser(t, "alice"), e.mkGroup(t, "devs")

	wrID := e.bulkEdit(t, []identity.BulkEditResource{
		{Id: user.Id, ResourceType: new("user")},
		{Id: group.Id, ResourceType: new("GROUP")},
		{Id: new("ocid1.user.oc1..gone"), ResourceType: new("user")},
		{Id: new("ocid1.bucket.oc1..b"), ResourceType: new("bucket")},
	}, identity.BulkEditOperationDetails{
		OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
		DefinedTags:   map[string]map[string]any{"ops": {"cost": "42"}},
	})

	wr := e.taggingWR(t, wrID)
	assert.Equal(t, identity.TaggingWorkRequestOperationTypeBulkEditOfTags, wr.OperationType)
	assert.Equal(t, identity.TaggingWorkRequestStatusAccepted, wr.Status)
	assert.Nil(t, wr.TimeStarted)

	e.clock.Advance(1100 * time.Millisecond)
	wr = e.taggingWR(t, wrID)
	assert.Equal(t, identity.TaggingWorkRequestStatusInProgress, wr.Status)
	assert.NotNil(t, wr.TimeStarted)

	got, err := e.client.GetUser(ctx, identity.GetUserRequest{UserId: user.Id})
	require.NoError(t, err)
	assert.Empty(t, got.DefinedTags, "edit lands when the work request completes")

	e.clock.Advance(2 * time.Second)
	wr = e.taggingWR(t, wrID)
	assert.Equal(t, identity.TaggingWorkRequestStatusPartiallySucceeded, wr.Status)
	require.Len(t, wr.Resources, 4)
	assert.NotNil(t, wr.TimeFinished)

	got, err = e.client.GetUser(ctx, identity.GetUserRequest{UserId: user.Id})
	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]any{"Ops": {"Cost": "42"}}, got.DefinedTags)

	g, err := e.client.GetGroup(ctx, identity.GetGroupRequest{GroupId: group.Id})
	require.NoError(t, err)
	assert.Equal(t, "42", g.DefinedTags["Ops"]["Cost"])

	errs, err := e.client.ListTaggingWorkRequestErrors(ctx,
		identity.ListTaggingWorkRequestErrorsRequest{WorkRequestId: new(wrID)})
	require.NoError(t, err)
	assert.Len(t, errs.Items, 2)

	logs, err := e.client.ListTaggingWorkRequestLogs(ctx,
		identity.ListTaggingWorkRequestLogsRequest{WorkRequestId: new(wrID)})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(logs.Items), 4)

	list, err := e.client.ListTaggingWorkRequests(ctx,
		identity.ListTaggingWorkRequestsRequest{CompartmentId: new(testTenancy)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, identity.TaggingWorkRequestSummaryStatusPartiallySucceeded, list.Items[0].Status)
}

func TestBulkEditTagsOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		op     identity.BulkEditOperationDetailsOperationTypeEnum
		seed   map[string]map[string]any
		value  any
		want   map[string]map[string]any
		status identity.TaggingWorkRequestStatusEnum
	}{
		{
			"add absent", identity.BulkEditOperationDetailsOperationTypeAddWhereAbsent, nil, "new",
			map[string]map[string]any{"Ops": {"Cost": "new"}}, identity.TaggingWorkRequestStatusSucceeded,
		},
		{
			"add keeps present", identity.BulkEditOperationDetailsOperationTypeAddWhereAbsent,
			map[string]map[string]any{"Ops": {"Cost": "old"}}, "new",
			map[string]map[string]any{"Ops": {"Cost": "old"}}, identity.TaggingWorkRequestStatusSucceeded,
		},
		{
			"set skips absent", identity.BulkEditOperationDetailsOperationTypeSetWherePresent, nil, "new",
			map[string]map[string]any{}, identity.TaggingWorkRequestStatusSucceeded,
		},
		{
			"set replaces present", identity.BulkEditOperationDetailsOperationTypeSetWherePresent,
			map[string]map[string]any{"Ops": {"Cost": "old"}}, "new",
			map[string]map[string]any{"Ops": {"Cost": "new"}}, identity.TaggingWorkRequestStatusSucceeded,
		},
		{
			"remove", identity.BulkEditOperationDetailsOperationTypeRemove,
			map[string]map[string]any{"Ops": {"Cost": "old"}}, "x",
			map[string]map[string]any{}, identity.TaggingWorkRequestStatusSucceeded,
		},
		{
			"unknown tag fails", identity.BulkEditOperationDetailsOperationTypeAddOrSet, nil, "new",
			nil, identity.TaggingWorkRequestStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			tag := "Cost"

			if tt.status == identity.TaggingWorkRequestStatusFailed {
				tag = "Nope"
			}

			e.mkTag(t, "Ops", "Cost", nil)

			c, err := e.client.CreateUser(t.Context(), identity.CreateUserRequest{
				CompartmentId: new(testTenancy), Name: new("u"), Description: new("d"), DefinedTags: tt.seed,
			})
			require.NoError(t, err)

			wrID := e.bulkEdit(t, []identity.BulkEditResource{{Id: c.Id, ResourceType: new("user")}},
				identity.BulkEditOperationDetails{
					OperationType: tt.op, DefinedTags: map[string]map[string]any{"Ops": {tag: tt.value}},
				})
			e.clock.Advance(3 * time.Second)
			assert.Equal(t, tt.status, e.taggingWR(t, wrID).Status)

			if tt.want == nil {
				return
			}

			got, err := e.client.GetUser(t.Context(), identity.GetUserRequest{UserId: c.Id})
			require.NoError(t, err)

			if len(tt.want) == 0 {
				assert.Empty(t, got.DefinedTags)
			} else {
				assert.Equal(t, tt.want, got.DefinedTags)
			}
		})
	}
}

func TestBulkEditTagsRetiredAndEnum(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.mkTag(t, "Ops", "Tier", identity.EnumTagDefinitionValidator{Values: []string{"gold", "silver"}})
	user := e.mkUser(t, "bob")

	ok := e.bulkEdit(t, []identity.BulkEditResource{{Id: user.Id, ResourceType: new("user")}},
		identity.BulkEditOperationDetails{
			OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
			DefinedTags:   map[string]map[string]any{"Ops": {"Tier": "gold"}},
		})
	bad := e.bulkEdit(t, []identity.BulkEditResource{{Id: user.Id, ResourceType: new("user")}},
		identity.BulkEditOperationDetails{
			OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
			DefinedTags:   map[string]map[string]any{"Ops": {"Tier": "bronze"}},
		})

	e.clock.Advance(3 * time.Second)
	assert.Equal(t, identity.TaggingWorkRequestStatusSucceeded, e.taggingWR(t, ok).Status)
	assert.Equal(t, identity.TaggingWorkRequestStatusFailed, e.taggingWR(t, bad).Status)

	got, err := e.client.GetUser(t.Context(), identity.GetUserRequest{UserId: user.Id})
	require.NoError(t, err)
	assert.Equal(t, "gold", got.DefinedTags["Ops"]["Tier"])
}

func TestBulkEditTagsValidation(t *testing.T) {
	t.Parallel()

	res := []identity.BulkEditResource{{Id: new("ocid1.user.oc1..x"), ResourceType: new("user")}}
	op := identity.BulkEditOperationDetails{
		OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
		DefinedTags:   map[string]map[string]any{"a": {"b": "c"}},
	}

	tests := []struct {
		name    string
		details identity.BulkEditTagsDetails
		status  int
	}{
		{"no resources", identity.BulkEditTagsDetails{
			CompartmentId: new(
				testTenancy,
			), Resources: []identity.BulkEditResource{}, BulkEditOperations: []identity.BulkEditOperationDetails{op},
		}, http.StatusBadRequest},
		{"no operations", identity.BulkEditTagsDetails{
			CompartmentId: new(testTenancy), Resources: res, BulkEditOperations: []identity.BulkEditOperationDetails{},
		}, http.StatusBadRequest},
		{"unknown compartment", identity.BulkEditTagsDetails{
			CompartmentId: new("ocid1.compartment.oc1..nope"), Resources: res,
			BulkEditOperations: []identity.BulkEditOperationDetails{op},
		}, http.StatusNotFound},
		{"bad operation type", identity.BulkEditTagsDetails{
			CompartmentId: new(testTenancy), Resources: res,
			BulkEditOperations: []identity.BulkEditOperationDetails{{
				OperationType: "NOPE", DefinedTags: op.DefinedTags,
			}},
		}, http.StatusBadRequest},
		{"empty defined tags", identity.BulkEditTagsDetails{
			CompartmentId: new(testTenancy), Resources: res,
			BulkEditOperations: []identity.BulkEditOperationDetails{{
				OperationType: identity.BulkEditOperationDetailsOperationTypeRemove,
				DefinedTags:   map[string]map[string]any{},
			}},
		}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.client.BulkEditTags(t.Context(), identity.BulkEditTagsRequest{BulkEditTagsDetails: tt.details})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestBulkDeleteTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	nsID, tagID := e.mkTag(t, "Ops", "Cost", nil)
	_, doomed := e.mkTag(t, "Other", "Gone", nil)
	user := e.mkUser(t, "carol")

	e.bulkEdit(t, []identity.BulkEditResource{{Id: user.Id, ResourceType: new("user")}},
		identity.BulkEditOperationDetails{
			OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
			DefinedTags:   map[string]map[string]any{"Ops": {"Cost": "1"}, "Other": {"Gone": "x"}},
		})
	e.clock.Advance(3 * time.Second)

	_, err := e.client.BulkDeleteTags(ctx, identity.BulkDeleteTagsRequest{
		TagDefinitionIds: []string{"ocid1.tagdefinition.oc1..nope"},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.BulkDeleteTags(ctx, identity.BulkDeleteTagsRequest{
		TagDefinitionIds: []string{},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	r, err := e.client.BulkDeleteTags(ctx, identity.BulkDeleteTagsRequest{
		TagDefinitionIds: []string{doomed, doomed},
		OpcRetryToken:    new("tok-1"),
	})
	require.NoError(t, err)

	replay, err := e.client.BulkDeleteTags(ctx, identity.BulkDeleteTagsRequest{
		TagDefinitionIds: []string{doomed},
		OpcRetryToken:    new("tok-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, *r.OpcWorkRequestId, *replay.OpcWorkRequestId)

	again, err := e.client.BulkDeleteTags(ctx, identity.BulkDeleteTagsRequest{
		TagDefinitionIds: []string{tagID, doomed},
	})
	require.NoError(t, err)

	wr := e.taggingWR(t, *r.OpcWorkRequestId)
	assert.Equal(t, identity.TaggingWorkRequestOperationTypeBulkDeleteTagDefinition, wr.OperationType)
	assert.Equal(t, identity.TaggingWorkRequestStatusAccepted, wr.Status)

	e.clock.Advance(3 * time.Second)
	wr = e.taggingWR(t, *r.OpcWorkRequestId)
	assert.Equal(t, identity.TaggingWorkRequestStatusSucceeded, wr.Status)
	require.Len(t, wr.Resources, 1)
	assert.Equal(t, identity.WorkRequestResourceActionTypeDeleted, wr.Resources[0].ActionType)

	got, err := e.client.GetUser(ctx, identity.GetUserRequest{UserId: user.Id})
	require.NoError(t, err)
	assert.Empty(t, got.DefinedTags, "both deleted tags are stripped from the user")

	wr = e.taggingWR(t, *again.OpcWorkRequestId)
	assert.Equal(t, identity.TaggingWorkRequestStatusPartiallySucceeded, wr.Status)

	errs, err := e.client.ListTaggingWorkRequestErrors(ctx,
		identity.ListTaggingWorkRequestErrorsRequest{WorkRequestId: again.OpcWorkRequestId})
	require.NoError(t, err)
	require.Len(t, errs.Items, 1)
	assert.Equal(t, "IncorrectState", *errs.Items[0].Code)

	_, err = e.client.GetTag(ctx, identity.GetTagRequest{TagNamespaceId: new(nsID), TagName: new("Cost")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestBulkEditRetryTokenReplays(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	req := identity.BulkEditTagsRequest{
		OpcRetryToken: new("same"),
		CompartmentId: new(testTenancy),
		Resources:     []identity.BulkEditResource{{Id: new("ocid1.user.oc1..x"), ResourceType: new("user")}},
		BulkEditOperations: []identity.BulkEditOperationDetails{{
			OperationType: identity.BulkEditOperationDetailsOperationTypeRemove,
			DefinedTags:   map[string]map[string]any{"a": {"b": "c"}},
		}},
	}

	r1, err := e.client.BulkEditTags(t.Context(), req)
	require.NoError(t, err)

	r2, err := e.client.BulkEditTags(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, *r1.OpcWorkRequestId, *r2.OpcWorkRequestId)
}

func TestBulkWorkRequestSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.mkTag(t, "Ops", "Cost", nil)
	user := e.mkUser(t, "dave")

	wrID := e.bulkEdit(t, []identity.BulkEditResource{{Id: user.Id, ResourceType: new("user")}},
		identity.BulkEditOperationDetails{
			OperationType: identity.BulkEditOperationDetailsOperationTypeAddOrSet,
			DefinedTags:   map[string]map[string]any{"Ops": {"Cost": "9"}},
		})

	fresh := oci.NewInMemoryBackend()
	fresh.SetClock(e.clock.Now)
	require.NoError(t, fresh.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	e.clock.Advance(3 * time.Second)
	fresh.Sweep()

	wr, err := fresh.GetTaggingWorkRequest(wrID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", wr.Status)

	u, _, err := fresh.GetUser(*user.Id)
	require.NoError(t, err)
	assert.Equal(t, "9", u.DefinedTags["Ops"]["Cost"])
}
