package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListReleaseLabels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	listRec := doEMRRequest(t, h, "ListReleaseLabels", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ReleaseLabels []string `json:"ReleaseLabels"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.ReleaseLabels)
	assert.Contains(t, out.ReleaseLabels, "emr-7.3.0")
}

func TestDescribeReleaseLabel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "DescribeReleaseLabel", map[string]any{"ReleaseLabel": "emr-7.3.0"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ReleaseLabel string `json:"ReleaseLabel"`
		Applications []struct {
			Name string `json:"Name"`
		} `json:"Applications"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "emr-7.3.0", out.ReleaseLabel)
	assert.NotEmpty(t, out.Applications)
}

func TestListSupportedInstanceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "ListSupportedInstanceTypes", map[string]any{"ReleaseLabel": "emr-7.3.0"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SupportedInstanceTypes []struct {
			Type string `json:"Type"`
			VCPU int    `json:"VCPU"`
		} `json:"SupportedInstanceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.SupportedInstanceTypes)
}
