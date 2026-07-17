package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

func TestWAF_SizeConstraintSet_CreateGetUpdateDeleteList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateSizeConstraintSet", map[string]any{
		"ChangeToken": token,
		"Name":        "my-scs",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	scsMap := createResp["SizeConstraintSet"].(map[string]any)
	id := scsMap["SizeConstraintSetId"].(string)
	require.NotEmpty(t, id)

	// Update
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "UpdateSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": id,
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"SizeConstraint": map[string]any{
					"FieldToMatch":       map[string]any{"Type": "BODY"},
					"TextTransformation": "NONE",
					"ComparisonOperator": "GT",
					"Size":               8192,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = wafDo(t, h, "GetSizeConstraintSet", map[string]any{"SizeConstraintSetId": id})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	scs := resp["SizeConstraintSet"].(map[string]any)
	constraints := scs["SizeConstraints"].([]any)
	require.Len(t, constraints, 1)
	c := constraints[0].(map[string]any)
	assert.Equal(t, "GT", c["ComparisonOperator"])
	assert.EqualValues(t, 8192, c["Size"])

	// List
	rec = wafDo(t, h, "ListSizeConstraintSets", map[string]any{"Limit": 100})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	sets := listResp["SizeConstraintSets"].([]any)
	assert.Len(t, sets, 1)

	// Delete
	token = wafGetToken(t, h)
	rec = wafDo(t, h, "DeleteSizeConstraintSet", map[string]any{
		"ChangeToken":         token,
		"SizeConstraintSetId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, waf.SizeConstraintSetCount(h.Backend.(*waf.InMemoryBackend)))
}

func TestWAF_SizeConstraintSet_NotFound(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	rec := wafDo(t, h, "GetSizeConstraintSet", map[string]any{"SizeConstraintSetId": "nope"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
