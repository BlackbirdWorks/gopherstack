package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateUserPopulatesTimestampsAndActor verifies that DescribeUser returns
// CreatedAt/UpdatedAt as AWS JSON-protocol epoch-seconds numbers (not the Go
// default RFC3339 string, and not simply absent -- a prior gap left these
// fields unmodeled entirely) plus non-empty CreatedBy/UpdatedBy, matching the
// real DescribeUserOutput shape.
func TestCreateUserPopulatesTimestampsAndActor(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "timestamp.user",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	descRec := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)

	createdAt, ok := resp["CreatedAt"].(float64)
	require.True(t, ok, "CreatedAt must be a JSON number (epoch seconds), got %T", resp["CreatedAt"])
	assert.Positive(t, createdAt)

	updatedAt, ok := resp["UpdatedAt"].(float64)
	require.True(t, ok, "UpdatedAt must be a JSON number (epoch seconds), got %T", resp["UpdatedAt"])
	assert.Positive(t, updatedAt)
	assert.InDelta(t, createdAt, updatedAt, 0, "CreatedAt and UpdatedAt must match immediately after create")

	createdBy, _ := resp["CreatedBy"].(string)
	updatedBy, _ := resp["UpdatedBy"].(string)
	assert.NotEmpty(t, createdBy)
	assert.Equal(t, createdBy, updatedBy)
}

// TestUpdateUserRefreshesUpdatedAt verifies UpdateUser advances UpdatedAt
// while leaving CreatedAt untouched.
func TestUpdateUserRefreshesUpdatedAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "refresh.user",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	before := parseResponse(t, doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	}))
	beforeCreatedAt := before["CreatedAt"].(float64)

	updRec := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "displayName", "AttributeValue": "Refreshed"},
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	after := parseResponse(t, doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	}))

	assert.InDelta(t, beforeCreatedAt, after["CreatedAt"].(float64), 0, "CreatedAt must not change on update")
	assert.GreaterOrEqual(t, after["UpdatedAt"].(float64), beforeCreatedAt, "UpdatedAt must not regress")
	assert.NotEmpty(t, after["UpdatedBy"])
}

// TestListUsersIncludesTimestamps verifies ListUsers items also carry the
// CreatedAt/UpdatedAt/CreatedBy/UpdatedBy fields (not only DescribeUser).
func TestListUsersIncludesTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "list.timestamp.user",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users, ok := parseResponse(t, listRec)["Users"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, users)

	first := users[0].(map[string]any)
	assert.IsType(t, float64(0), first["CreatedAt"])
	assert.IsType(t, float64(0), first["UpdatedAt"])
	assert.NotEmpty(t, first["CreatedBy"])
}

// TestCreateGroupPopulatesTimestampsAndActor mirrors
// TestCreateUserPopulatesTimestampsAndActor for groups.
func TestCreateGroupPopulatesTimestampsAndActor(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "timestamp-group",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	groupID := parseResponse(t, createRec)["GroupId"].(string)

	descRec := doRequest(t, h, "DescribeGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)

	createdAt, ok := resp["CreatedAt"].(float64)
	require.True(t, ok)
	assert.Positive(t, createdAt)
	assert.InDelta(t, createdAt, resp["UpdatedAt"].(float64), 0)
	assert.NotEmpty(t, resp["CreatedBy"])
	assert.Equal(t, resp["CreatedBy"], resp["UpdatedBy"])
}

// TestUpdateGroupRefreshesUpdatedAt mirrors TestUpdateUserRefreshesUpdatedAt
// for groups.
func TestUpdateGroupRefreshesUpdatedAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "refresh-group",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	groupID := parseResponse(t, createRec)["GroupId"].(string)

	before := parseResponse(t, doRequest(t, h, "DescribeGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
	}))
	beforeCreatedAt := before["CreatedAt"].(float64)

	updRec := doRequest(t, h, "UpdateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"Operations": []map[string]any{
			{"AttributePath": "description", "AttributeValue": "refreshed desc"},
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	after := parseResponse(t, doRequest(t, h, "DescribeGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
	}))

	assert.InDelta(t, beforeCreatedAt, after["CreatedAt"].(float64), 0)
	assert.GreaterOrEqual(t, after["UpdatedAt"].(float64), beforeCreatedAt)
}

// TestCreateGroupMembershipPopulatesTimestamps verifies
// DescribeGroupMembership returns CreatedAt/CreatedBy/UpdatedAt/UpdatedBy,
// and that -- since real AWS has no UpdateGroupMembership API -- CreatedAt
// equals UpdatedAt for the resource's whole lifetime.
func TestCreateGroupMembershipPopulatesTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	userRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "membership.timestamp.user",
	})
	require.Equal(t, http.StatusOK, userRec.Code)
	userID := parseResponse(t, userRec)["UserId"].(string)

	groupRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "membership-timestamp-group",
	})
	require.Equal(t, http.StatusOK, groupRec.Code)
	groupID := parseResponse(t, groupRec)["GroupId"].(string)

	membershipRec := doRequest(t, h, "CreateGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
		"MemberId":        map[string]string{"UserId": userID},
	})
	require.Equal(t, http.StatusOK, membershipRec.Code)
	membershipID := parseResponse(t, membershipRec)["MembershipId"].(string)

	descRec := doRequest(t, h, "DescribeGroupMembership", map[string]any{
		"IdentityStoreId": testStoreID,
		"MembershipId":    membershipID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)

	createdAt, ok := resp["CreatedAt"].(float64)
	require.True(t, ok)
	assert.Positive(t, createdAt)
	assert.InDelta(t, createdAt, resp["UpdatedAt"].(float64), 0)
	assert.NotEmpty(t, resp["CreatedBy"])
	assert.Equal(t, resp["CreatedBy"], resp["UpdatedBy"])
}

// TestUpdateUserOperationsBound verifies UpdateUser enforces the real
// AttributeOperations shape's min:1/max:100 constraint on Operations, which
// was previously entirely unvalidated (any Operations length, including
// zero, was accepted).
func TestUpdateUserOperationsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		opsCount   int
		wantStatus int
	}{
		{name: "empty_operations_rejected", opsCount: 0, wantStatus: http.StatusBadRequest},
		{name: "single_operation_accepted", opsCount: 1, wantStatus: http.StatusOK},
		{name: "max_100_operations_accepted", opsCount: 100, wantStatus: http.StatusOK},
		{name: "over_100_operations_rejected", opsCount: 101, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "bound.user." + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			userID := parseResponse(t, createRec)["UserId"].(string)

			ops := make([]map[string]any, tt.opsCount)
			for i := range ops {
				ops[i] = map[string]any{
					"AttributePath":  "title",
					"AttributeValue": fmt.Sprintf("title-%d", i),
				}
			}

			rec := doRequest(t, h, "UpdateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserId":          userID,
				"Operations":      ops,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestUpdateGroupOperationsBound mirrors TestUpdateUserOperationsBound for
// UpdateGroup.
func TestUpdateGroupOperationsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		opsCount   int
		wantStatus int
	}{
		{name: "empty_operations_rejected", opsCount: 0, wantStatus: http.StatusBadRequest},
		{name: "single_operation_accepted", opsCount: 1, wantStatus: http.StatusOK},
		{name: "max_100_operations_accepted", opsCount: 100, wantStatus: http.StatusOK},
		{name: "over_100_operations_rejected", opsCount: 101, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createRec := doRequest(t, h, "CreateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"DisplayName":     "bound-group-" + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			groupID := parseResponse(t, createRec)["GroupId"].(string)

			ops := make([]map[string]any, tt.opsCount)
			for i := range ops {
				ops[i] = map[string]any{
					"AttributePath":  "description",
					"AttributeValue": fmt.Sprintf("desc-%d", i),
				}
			}

			rec := doRequest(t, h, "UpdateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"GroupId":         groupID,
				"Operations":      ops,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestCreateGroupRejectsExternalIds verifies the real CreateGroup wire shape
// -- ExternalIds is not settable at creation time (no ExternalIds member on
// CreateGroupRequest in the real smithy model). A prior gopherstack revision
// accepted and applied this field anyway; sending it now must simply be
// ignored (unknown-field-tolerant JSON decoding), never surfaced on the
// created group.
func TestCreateGroupRejectsExternalIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "no-external-ids-at-create",
		"ExternalIds": []map[string]string{
			{"Issuer": "https://sso.example.com", "Id": "should-be-ignored"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	groupID := parseResponse(t, createRec)["GroupId"].(string)

	descRec := doRequest(t, h, "DescribeGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"GroupId":         groupID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	assert.Nil(t, resp["ExternalIds"], "ExternalIds sent on CreateGroup must not be applied")

	lookupRec := doRequest(t, h, "GetGroupId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"ExternalId": map[string]string{
				"Issuer": "https://sso.example.com",
				"Id":     "should-be-ignored",
			},
		},
	})
	assert.Equal(t, http.StatusNotFound, lookupRec.Code)
}
