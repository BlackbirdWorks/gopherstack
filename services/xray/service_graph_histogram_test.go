package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetServiceGraph_Histograms(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	put := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-hg-001", "a1", "", "frontend", now-10, now-8, false, false, false),
			segJSON("1-hg-001", "b1", "a1", "backend", now-9, now-8, false, false, false),
			segJSON("1-hg-002", "a2", "", "frontend", now-6, now-4, false, false, false),
			segJSON("1-hg-002", "b2", "a2", "backend", now-5, now-3.5, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, put.Code)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{"StartTime": now - 20, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Services []struct {
			Name                  string           `json:"Name"`
			ResponseTimeHistogram []map[string]any `json:"ResponseTimeHistogram"`
			DurationHistogram     []map[string]any `json:"DurationHistogram"`
			Edges                 []struct {
				ResponseTimeHistogram []map[string]any `json:"ResponseTimeHistogram"`
			} `json:"Edges"`
		} `json:"Services"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	byName := map[string]int{}
	for i, s := range resp.Services {
		byName[s.Name] = i
	}

	front := resp.Services[byName["frontend"]]
	assert.Equal(t, []map[string]any{{"Value": 2.0, "Count": 2.0}}, front.ResponseTimeHistogram)
	assert.Equal(t, front.ResponseTimeHistogram, front.DurationHistogram)

	back := resp.Services[byName["backend"]]
	assert.Equal(
		t,
		[]map[string]any{{"Value": 1.0, "Count": 1.0}, {"Value": 1.5, "Count": 1.0}},
		back.ResponseTimeHistogram,
	)

	require.Len(t, front.Edges, 1)
	assert.Equal(t, back.ResponseTimeHistogram, front.Edges[0].ResponseTimeHistogram)
}
