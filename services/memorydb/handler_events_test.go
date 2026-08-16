package memorydb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestHandler_DescribeEvents_WithData tests DescribeEvents with actual events.
func TestHandler_DescribeEvents_WithData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name: "filter by source name",
			body: map[string]any{
				"SourceName": "my-cluster",
			},
			wantCount: 1,
		},
		{
			name: "filter by source type",
			body: map[string]any{
				"SourceType": "cluster",
			},
			wantCount: 2,
		},
		{
			name:      "all events",
			body:      map[string]any{},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

			b.AddEvent(&memorydb.ExportedEvent{
				SourceName: "my-cluster",
				SourceType: "cluster",
				Message:    "event 1",
			})
			b.AddEvent(&memorydb.ExportedEvent{
				SourceName: "other-cluster",
				SourceType: "cluster",
				Message:    "event 2",
			})

			events, err := b.DescribeEvents(context.Background(), &memorydb.ExportedDescribeEventsRequest{
				SourceName: func() string {
					if v, ok := tt.body["SourceName"].(string); ok {
						return v
					}

					return ""
				}(),
				SourceType: func() string {
					if v, ok := tt.body["SourceType"].(string); ok {
						return v
					}

					return ""
				}(),
			})
			require.NoError(t, err)
			assert.Len(t, events, tt.wantCount)
		})
	}
}

func TestWireEpoch_DescribeEvents_DateIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "epoch-cluster",
		"NodeType":    "db.t4g.small",
	})

	rec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-cluster",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, _ := resp["Events"].([]any)
	require.NotEmpty(t, events, "expected at least one event for the created cluster")

	ev, _ := events[0].(map[string]any)
	dateVal, ok := ev["Date"]
	require.True(t, ok, "Date field missing from event")

	_, isNumber := dateVal.(float64)
	assert.True(t, isNumber, "Event.Date must serialize as a JSON number (epoch seconds), got %T: %v", dateVal, dateVal)
}

func TestWireEpoch_DescribeEvents_StartTimeAcceptsEpochNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "epoch-filter-cluster",
		"NodeType":    "db.t4g.small",
	})

	// Real clients send StartTime as a raw JSON number, not a quoted string.
	rec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-filter-cluster",
		"StartTime":  0,
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"DescribeEvents must accept an epoch-seconds StartTime the way a real SDK client sends it")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, _ := resp["Events"].([]any)
	assert.NotEmpty(t, events, "StartTime=0 (epoch start) should not filter out the cluster-creation event")

	// A StartTime far in the future must filter every event out.
	rec = doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "epoch-filter-cluster",
		"StartTime":  4102444800, // 2100-01-01T00:00:00Z
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp = nil
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	events, _ = resp["Events"].([]any)
	assert.Empty(t, events, "a future StartTime should filter out all past events")
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns empty events list",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter by source name returns empty when no match",
			body: map[string]any{
				"SourceName": "my-cluster",
				"SourceType": "cluster",
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			assert.Len(t, events, tt.wantCount)
		})
	}
}

func TestHandler_Events_Generated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupFn     func(h *memorydb.Handler)
		wantSrcName string
		wantSrcType string
	}{
		{
			name: "CreateCluster emits event",
			setupFn: func(h *memorydb.Handler) {
				doCreateCluster(t, h, minimalClusterBody("evt-cluster"))
			},
			wantSrcName: "evt-cluster",
			wantSrcType: "cluster",
		},
		{
			name: "CreateACL emits event",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "evt-acl"})
			},
			wantSrcName: "evt-acl",
			wantSrcType: "acl",
		},
		{
			name: "CreateUser emits event",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "evt-user",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			wantSrcName: "evt-user",
			wantSrcType: "user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceName": tt.wantSrcName,
				"SourceType": tt.wantSrcType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events, _ := resp["Events"].([]any)
			assert.NotEmpty(t, events, "expected events for %s", tt.wantSrcName)
		})
	}
}

// -- Optimization: ListClusters clone fix ----------------------------------------

func TestHandler_Events_SourceTypeFiltering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupBody      map[string]any
		name           string
		srcType        string
		setupOp        string
		wantEventCount int
	}{
		{
			name:    "cluster events filterable by source type",
			srcType: "cluster",
			setupOp: "CreateCluster",
			setupBody: map[string]any{
				"ClusterName": "evt-src-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantEventCount: 1,
		},
		{
			name:    "acl events filterable by source type",
			srcType: "acl",
			setupOp: "CreateACL",
			setupBody: map[string]any{
				"ACLName": "evt-src-acl",
			},
			wantEventCount: 1,
		},
		{
			name:    "user events filterable by source type",
			srcType: "user",
			setupOp: "CreateUser",
			setupBody: map[string]any{
				"UserName":           "evt-src-user",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
			},
			wantEventCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, tt.setupOp, tt.setupBody)
			require.Equal(t, http.StatusOK, rec.Code, "setup: %s", rec.Body)

			evRec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceType": tt.srcType,
			})
			require.Equal(t, http.StatusOK, evRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(evRec.Body.Bytes(), &resp))
			events, _ := resp["Events"].([]any)
			assert.GreaterOrEqual(t, len(events), tt.wantEventCount)
		})
	}
}

// -- Pagination: MaxResults respected (finding 24) -------------------------------

// TestHandler_DescribeEvents_TimeFilters tests event filtering with time parameters.
func TestHandler_DescribeEvents_TimeFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "filter by duration",
			body:       map[string]any{"Duration": 60},
			wantStatus: http.StatusOK,
		},
		{
			name:       "all events no filter",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a cluster to generate events
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "event-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Events_AutoPopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(*memorydb.Handler)
		name       string
		wantSrc    string
		wantType   string
		wantMinMsg string
	}{
		{
			name: "create cluster emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			wantSrc:    "evt-cluster",
			wantType:   "cluster",
			wantMinMsg: "created",
		},
		{
			name: "delete cluster emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": "evt-cluster"})
			},
			wantSrc:    "evt-cluster",
			wantType:   "cluster",
			wantMinMsg: "deleted",
		},
		{
			name: "create snapshot emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "evt-cluster",
					"SnapshotName": "evt-snap",
				})
			},
			wantSrc:    "evt-snap",
			wantType:   "snapshot",
			wantMinMsg: "created",
		},
		{
			name: "create ACL emits event",
			ops: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "evt-acl"})
			},
			wantSrc:    "evt-acl",
			wantType:   "acl",
			wantMinMsg: "created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.ops(h)

			rec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceName": tt.wantSrc,
				"SourceType": tt.wantType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			require.NotEmpty(t, events, "expected at least one event for source %q type %q", tt.wantSrc, tt.wantType)

			found := false
			for _, e := range events {
				ev := e.(map[string]any)
				if strings.Contains(strings.ToLower(ev["Message"].(string)), tt.wantMinMsg) {
					found = true

					break
				}
			}
			assert.True(t, found, "expected event with message containing %q", tt.wantMinMsg)
		})
	}
}

// -- DescribeEvents Duration (Gap 14) ------------------------------------------

func TestHandler_DescribeEvents_Duration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*memorydb.Handler)
		body         map[string]any
		name         string
		wantMinCount int
	}{
		{
			name: "duration set returns recent events",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "dur-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:         map[string]any{"Duration": 60},
			wantMinCount: 1,
		},
		{
			name: "no filter returns all events",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "dur-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:         map[string]any{},
			wantMinCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, "DescribeEvents", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			assert.GreaterOrEqual(t, len(events), tt.wantMinCount)
		})
	}
}

// -- ServiceUpdates fixtures (Gap 11) ------------------------------------------
