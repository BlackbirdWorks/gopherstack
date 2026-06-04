package detective_test

// Batch-2 AWS-accuracy tests for Detective (go-itbf).
//
// Covers accuracy gaps not exercised by the batch-1 audit:
//
//  1. Graph ARN format: arn:aws:detective:{region}:{account}:graph:{32hexchars}
//  2. ListGraphs CreatedTime: ISO 8601 timestamp present in every graph entry.
//  3. ListGraphs pagination: NextToken present when truncated, absent when all fit.
//  4. ListMembers pagination: same behavior.
//  5. MemberDetail MasterId: deprecated alias always equals AdministratorId.
//  6. MemberDetail fields: all required fields present after CreateMembers.
//  7. DeleteMembers all-unprocessed: AccountIds is [] not null.
//  8. CreateMembers UnprocessedAccounts: always [] not null even when none fail.
//  9. TagResource empty Tags body: returns 200.
// 10. UntagResource non-existent key: silent no-op (200).
// 11. ListTagsForResource no-tags graph: Tags is {} not null.
// 12. CreateGraph tags visible via ListTagsForResource.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Graph ARN format
// ---------------------------------------------------------------------------

var reDetectiveARN = regexp.MustCompile(`^arn:aws:detective:[a-z0-9-]+:\d{12}:graph:[0-9a-f]{32}$`)

func TestBatch2_GraphARN_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["GraphArn"].(string)

	require.NotEmpty(t, arn, "CreateGraph must return a GraphArn")
	assert.True(t, reDetectiveARN.MatchString(arn),
		"GraphArn must match arn:aws:detective:{region}:{account}:graph:{32hexchars}, got %q", arn)
}

// ---------------------------------------------------------------------------
// ListGraphs CreatedTime present
// ---------------------------------------------------------------------------

func TestBatch2_ListGraphs_CreatedTime_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/graph", map[string]any{})

	rec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		GraphList []struct {
			Arn         string `json:"Arn"`
			CreatedTime string `json:"CreatedTime"`
		} `json:"GraphList"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.GraphList, 1)
	assert.NotEmpty(t, resp.GraphList[0].CreatedTime, "ListGraphs must include CreatedTime")
	assert.Contains(t, resp.GraphList[0].CreatedTime, "T",
		"CreatedTime must be ISO 8601, got %q", resp.GraphList[0].CreatedTime)
}

// ---------------------------------------------------------------------------
// ListGraphs pagination
// ---------------------------------------------------------------------------

func TestBatch2_ListGraphs_Pagination_NextToken_Absent_When_All_Fit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/graph", map[string]any{})

	rec := doRequest(t, h, http.MethodPost, "/graphs/list", map[string]any{"MaxResults": 200})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasToken := resp["NextToken"]
	assert.False(t, hasToken, "NextToken must be absent when all results fit on one page")
}

// ---------------------------------------------------------------------------
// ListMembers pagination
// ---------------------------------------------------------------------------

func TestBatch2_ListMembers_Pagination_NextToken_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	accounts := make([]any, 3)
	for i := range 3 {
		accounts[i] = map[string]any{
			"AccountId":    fmt.Sprintf("%012d", i+1),
			"EmailAddress": fmt.Sprintf("member%d@example.com", i+1),
		}
	}

	createMem := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": accounts,
	})
	require.Equal(t, http.StatusOK, createMem.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphArn,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page1))
	memberList1, _ := page1["MemberDetails"].([]any)
	assert.Len(t, memberList1, 2, "MaxResults=2 must return exactly 2 members")
	nextToken, hasToken := page1["NextToken"].(string)
	assert.True(t, hasToken && nextToken != "", "NextToken must be present when more results remain")

	rec3 := doRequest(t, h, http.MethodPost, "/graph/members/list", map[string]any{
		"GraphArn":   graphArn,
		"MaxResults": 200,
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var all map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &all))
	_, hasAllToken := all["NextToken"]
	assert.False(t, hasAllToken, "NextToken must be absent when all members fit")
}

// ---------------------------------------------------------------------------
// MemberDetail: MasterId field (deprecated alias for AdministratorId)
// ---------------------------------------------------------------------------

func TestBatch2_MemberDetail_MasterId_Equals_AdministratorId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "111111111111", "EmailAddress": "member@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var membersResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &membersResp))
	members, _ := membersResp["Members"].([]any)
	require.Len(t, members, 1)

	m := members[0].(map[string]any)
	masterID, hasMaster := m["MasterId"]
	adminID := m["AdministratorId"]

	assert.True(t, hasMaster, "MemberDetail must include MasterId (deprecated alias for AdministratorId)")
	assert.Equal(t, adminID, masterID, "MasterId must equal AdministratorId")
}

// ---------------------------------------------------------------------------
// MemberDetail: all required fields present
// ---------------------------------------------------------------------------

func TestBatch2_MemberDetail_AllRequiredFields_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "222222222222", "EmailAddress": "user@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var membersResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &membersResp))
	members, _ := membersResp["Members"].([]any)
	require.Len(t, members, 1)

	m := members[0].(map[string]any)
	for _, field := range []string{
		"AccountId", "AdministratorId", "EmailAddress",
		"GraphArn", "InvitedTime", "MasterId", "Status", "UpdatedTime",
	} {
		_, ok := m[field]
		assert.True(t, ok, "MemberDetail must include %q field", field)
	}

	assert.Equal(t, "INVITED", m["Status"], "initial member status must be INVITED")
}

// ---------------------------------------------------------------------------
// DeleteMembers: AccountIds is [] not null when all accounts unprocessed
// ---------------------------------------------------------------------------

func TestBatch2_DeleteMembers_AllUnprocessed_AccountIds_Empty_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members/removal", map[string]any{
		"GraphArn":   graphArn,
		"AccountIds": []string{"999999999999"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	accountIDs, hasKey := resp["AccountIds"]
	assert.True(t, hasKey, "DeleteMembers response must include AccountIds")

	ids, ok := accountIDs.([]any)
	assert.True(t, ok, "AccountIds must be an array (not null), got %T: %v", accountIDs, accountIDs)

	if ok {
		assert.Empty(t, ids, "AccountIds must be empty when all accounts are unprocessed")
	}

	unprocessed, _ := resp["UnprocessedAccounts"].([]any)
	assert.Len(t, unprocessed, 1, "unknown account must appear in UnprocessedAccounts")
}

// ---------------------------------------------------------------------------
// CreateMembers: UnprocessedAccounts always [] not null when none fail
// ---------------------------------------------------------------------------

func TestBatch2_CreateMembers_UnprocessedAccounts_Empty_Not_Null(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": []any{
			map[string]any{"AccountId": "333333333333", "EmailAddress": "ok@example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	unprocessed, hasKey := resp["UnprocessedAccounts"]
	assert.True(t, hasKey, "CreateMembers must always include UnprocessedAccounts")
	_, ok := unprocessed.([]any)
	assert.True(t, ok, "UnprocessedAccounts must be an array (not null), got %T: %v", unprocessed, unprocessed)
}

// ---------------------------------------------------------------------------
// TagResource: empty Tags body succeeds
// ---------------------------------------------------------------------------

func TestBatch2_TagResource_EmptyTags_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{
		"Tags": map[string]string{},
	})
	assert.Equal(t, http.StatusOK, rec2.Code, "TagResource with empty Tags must succeed")
}

// ---------------------------------------------------------------------------
// UntagResource: non-existent key is silent no-op
// ---------------------------------------------------------------------------

func TestBatch2_UntagResource_NonExistentKey_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	req := doRequest(t, h, http.MethodDelete, "/tags/"+graphArn+"?tagKeys=does-not-exist", nil)
	assert.Equal(t, http.StatusOK, req.Code, "UntagResource with non-existent key must succeed (no-op)")
}

// ---------------------------------------------------------------------------
// ListTagsForResource: Tags is {} not null when no tags set
// ---------------------------------------------------------------------------

func TestBatch2_ListTagsForResource_NoTags_Returns_EmptyObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	tags, hasKey := resp["Tags"]
	assert.True(t, hasKey, "ListTagsForResource must include Tags field")
	_, ok := tags.(map[string]any)
	assert.True(t, ok, "Tags must be an object (not null), got %T: %v", tags, tags)
}

// ---------------------------------------------------------------------------
// CreateGraph with tags: visible via ListTagsForResource
// ---------------------------------------------------------------------------

func TestBatch2_CreateGraph_Tags_VisibleInListTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{
		"Tags": map[string]string{"env": "prod", "team": "security"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+graphArn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))
	tags, _ := tagsResp["Tags"].(map[string]any)

	assert.Equal(t, "prod", tags["env"], "tags set at graph creation must be visible via ListTagsForResource")
	assert.Equal(t, "security", tags["team"])
}
