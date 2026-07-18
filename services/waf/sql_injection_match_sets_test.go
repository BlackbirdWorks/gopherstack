package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

func TestWAF_SqlInjectionMatchSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateSqlInjectionMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-sqli",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	simsMap := createResp["SqlInjectionMatchSet"].(map[string]any)
	id := simsMap["SqlInjectionMatchSetId"].(string)
	require.NotEmpty(t, id)

	// Update
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"SqlInjectionMatchTuple": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "QUERY_STRING"},
					"TextTransformation": "URL_DECODE",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetSqlInjectionMatchSet", map[string]any{"SqlInjectionMatchSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sims := resp["SqlInjectionMatchSet"].(map[string]any)
	tuples := sims["SqlInjectionMatchTuples"].([]any)
	require.Len(t, tuples, 1)
	tuple := tuples[0].(map[string]any)
	assert.Equal(t, "URL_DECODE", tuple["TextTransformation"])
	fm := tuple["FieldToMatch"].(map[string]any)
	assert.Equal(t, "QUERY_STRING", fm["Type"])

	// List
	rec = wafDo(t, h, "ListSqlInjectionMatchSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["SqlInjectionMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSqlInjectionMatchSet", map[string]any{
		"ChangeToken":            token,
		"SqlInjectionMatchSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.SqlInjectionMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_SqlInjectionMatchSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetSqlInjectionMatchSet", map[string]any{"SqlInjectionMatchSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
