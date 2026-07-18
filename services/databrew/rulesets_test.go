package databrew_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ---- Ruleset backend ----

func TestCreateRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rules := []databrew.Rule{
		{Name: "rule1", CheckExpression: "ROWCOUNT > 0"},
	}
	rs, err := b.CreateRuleset(
		context.Background(),
		"my-ruleset",
		"desc",
		"arn:aws:glue:us-east-1:123456789012:table/db/tbl",
		rules,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-ruleset", rs.Name)
	assert.Equal(t, "desc", rs.Description)
	assert.NotEmpty(t, rs.Arn)
	assert.Len(t, rs.Rules, 1)
	assert.Equal(t, "test", rs.Tags["env"])
}

func TestCreateRuleset_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(context.Background(), "", "desc", "arn:x", nil, nil)
	require.Error(t, err)
}

func TestCreateRuleset_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(context.Background(), "rs", "desc", "arn:x", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRuleset(context.Background(), "rs", "desc", "arn:x", nil, nil)
	require.Error(t, err)
}

func TestDescribeRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(
		context.Background(),
		"rs1",
		"desc",
		"arn:x",
		[]databrew.Rule{{Name: "r1", CheckExpression: "x > 0"}},
		nil,
	)
	require.NoError(t, err)
	rs, err := b.DescribeRuleset(context.Background(), "rs1")
	require.NoError(t, err)
	assert.Equal(t, "rs1", rs.Name)
	assert.Len(t, rs.Rules, 1)
}

func TestDescribeRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRuleset(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListRulesets(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(context.Background(), "rs1", "", "arn:x", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRuleset(context.Background(), "rs2", "", "arn:y", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRulesets(context.Background(), 100, "", "")
	assert.Len(t, list, 2)
}

func TestListRulesets_Pagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	for i := range 5 {
		name := []string{"rs-a", "rs-b", "rs-c", "rs-d", "rs-e"}[i]
		_, err := b.CreateRuleset(context.Background(), name, "", "arn:x", nil, nil)
		require.NoError(t, err)
	}
	page1, next := b.ListRulesets(context.Background(), 2, "", "")
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next)
	page2, next2 := b.ListRulesets(context.Background(), 2, next, "")
	assert.NotEmpty(t, page2)
	_ = next2
}

func TestUpdateRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(context.Background(), "upd-rs", "old", "arn:x", nil, nil)
	require.NoError(t, err)
	rules := []databrew.Rule{{Name: "new-rule", CheckExpression: "ROWCOUNT > 10"}}
	err = b.UpdateRuleset(context.Background(), "upd-rs", "new desc", rules)
	require.NoError(t, err)
	rs, err := b.DescribeRuleset(context.Background(), "upd-rs")
	require.NoError(t, err)
	assert.Equal(t, "new desc", rs.Description)
	assert.Len(t, rs.Rules, 1)
}

func TestUpdateRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateRuleset(context.Background(), "no-such", "", nil)
	require.Error(t, err)
}

func TestDeleteRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset(context.Background(), "del-rs", "", "arn:x", nil, nil)
	require.NoError(t, err)
	err = b.DeleteRuleset(context.Background(), "del-rs")
	require.NoError(t, err)
	_, err = b.DescribeRuleset(context.Background(), "del-rs")
	require.Error(t, err)
}

func TestDeleteRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteRuleset(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Ruleset handler ----

func TestHandlerCreateRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name":      "my-ruleset",
		"TargetArn": "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
		"Rules":     []map[string]any{{"Name": "r1", "CheckExpression": "ROWCOUNT > 0"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-ruleset", resp["Name"])
}

func TestHandlerDescribeRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "rs1", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets/rs1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeRuleset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListRulesets(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Rulesets"])
}

func TestHandlerUpdateRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "upd-rs", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/rulesets/upd-rs", map[string]any{
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "del-rs", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/rulesets/del-rs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRuleset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/rulesets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
