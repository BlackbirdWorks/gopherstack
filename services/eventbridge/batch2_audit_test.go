package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// ---------------------------------------------------------------------------
// Issue 1: CreateEventBus Tags silently dropped
//
// AWS CreateEventBus accepts a Tags map that is stored on the event bus ARN.
// The handler previously lacked a Tags field in createEventBusInput, so tags
// supplied at creation time were silently dropped and never stored.
// Fix: add Tags map to createEventBusInput and call setTags(bus.Arn, input.Tags).
// ---------------------------------------------------------------------------

func TestBatch2_CreateEventBus_TagsPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]any
		wantTags map[string]string
		name     string
		busName  string
	}{
		{
			name:    "tags supplied at creation are stored",
			busName: "tagged-bus",
			tags:    map[string]any{"env": "prod", "team": "platform"},
			wantTags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
		},
		{
			name:     "no tags is still valid",
			busName:  "untagged-bus",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			body := map[string]any{"Name": tt.busName}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}
			rec := auditMakeRequest(t, h, e, "CreateEventBus", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			busArn := createResp["EventBusArn"]
			require.NotEmpty(t, busArn)

			rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{"ResourceARN": busArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

			got := make(map[string]string, len(tagsResp.Tags))
			for _, kv := range tagsResp.Tags {
				got[kv.Key] = kv.Value
			}
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue 2: PutRule Tags silently dropped
//
// AWS PutRule accepts a Tags map for new rules. Previously PutRuleInput lacked
// a Tags field so tags were silently dropped.
// Fix: add Tags map to PutRuleInput and call setTags(rule.Arn, input.Tags) in
// the handler after a successful PutRule call.
// ---------------------------------------------------------------------------

func TestBatch2_PutRule_TagsPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]any
		wantTags map[string]string
		name     string
		ruleName string
	}{
		{
			name:     "tags at rule creation are stored",
			ruleName: "tagged-rule",
			tags:     map[string]any{"owner": "alice", "cost-center": "eng"},
			wantTags: map[string]string{
				"owner":       "alice",
				"cost-center": "eng",
			},
		},
		{
			name:     "rule with no tags has empty tag set",
			ruleName: "bare-rule",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			body := map[string]any{
				"Name":         tt.ruleName,
				"EventPattern": `{"source":["test"]}`,
				"State":        "ENABLED",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}
			rec := auditMakeRequest(t, h, e, "PutRule", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			ruleArn := putResp["RuleArn"]
			require.NotEmpty(t, ruleArn)

			rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{"ResourceARN": ruleArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

			got := make(map[string]string, len(tagsResp.Tags))
			for _, kv := range tagsResp.Tags {
				got[kv.Key] = kv.Value
			}
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue 3: StartReplay ReplayStartTime serialized as RFC3339 string
//
// AWS JSON protocol (application/x-amz-json-1.1) requires timestamps to be
// serialized as Unix epoch seconds (float64), not as RFC3339 strings.
// The StartReplay response struct previously used `time.Time` for
// ReplayStartTime, which Go's JSON encoder serializes as an RFC3339 string.
// Fix: change the field type to float64 and convert via timeToEpochSeconds.
// ---------------------------------------------------------------------------

func TestBatch2_StartReplay_ReplayStartTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		replayName string
	}{
		{
			name:       "ReplayStartTime is a JSON number not a string",
			replayName: "replay-epoch-test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now()
			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			b.AddArchiveInternal(&eventbridge.Archive{
				ArchiveName:    "batch2-arc",
				ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/batch2-arc",
				EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
				State:          "ACTIVE",
			})
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "StartReplay", map[string]any{
				"ReplayName":     tt.replayName,
				"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/batch2-arc",
				"EventStartTime": now.Add(-2 * time.Hour).UTC().Format(time.RFC3339),
				"EventEndTime":   now.Add(-time.Hour).UTC().Format(time.RFC3339),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			replayStartRaw, ok := raw["ReplayStartTime"]
			require.True(t, ok, "ReplayStartTime must be present in StartReplay response")

			var f float64
			err := json.Unmarshal(replayStartRaw, &f)
			require.NoError(
				t,
				err,
				"ReplayStartTime must be a JSON number (epoch seconds), got: %s",
				string(replayStartRaw),
			)
			assert.Greater(t, f, float64(0), "ReplayStartTime epoch value must be positive")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue 4: CreatePipe/UpdatePipe timestamps serialized as RFC3339 strings
//
// CreatePipe returns CreationTime and UpdatePipe returns LastModifiedTime.
// Both were typed as time.Time in the anonymous response structs, causing
// Go's JSON encoder to emit RFC3339 strings. AWS emits epoch float seconds.
// Fix: change both fields to float64 and convert via timeToEpochSeconds.
// ---------------------------------------------------------------------------

func TestBatch2_CreatePipe_CreationTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeName  string
		sourceArn string
		targetArn string
	}{
		{
			name:      "CreationTime in CreatePipe response is a JSON number",
			pipeName:  "pipe-ts-test",
			sourceArn: "arn:aws:sqs:us-east-1:123456789012:src-q",
			targetArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
				"Name":      tt.pipeName,
				"SourceArn": tt.sourceArn,
				"TargetArn": tt.targetArn,
				"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			ctRaw, ok := raw["CreationTime"]
			require.True(t, ok, "CreationTime must be present in CreatePipe response")

			var f float64
			err := json.Unmarshal(ctRaw, &f)
			require.NoError(t, err, "CreationTime must be a JSON number (epoch seconds), got: %s", string(ctRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}

func TestBatch2_UpdatePipe_LastModifiedTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
	}{
		{
			name:     "LastModifiedTime in UpdatePipe response is a JSON number",
			pipeName: "pipe-update-ts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
				"Name":      tt.pipeName,
				"SourceArn": "arn:aws:sqs:us-east-1:123456789012:src-q",
				"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
				"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
			})

			rec := auditMakeRequest(t, h, e, "UpdatePipe", map[string]any{
				"Name":        tt.pipeName,
				"Description": "updated",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			lmtRaw, ok := raw["LastModifiedTime"]
			require.True(t, ok, "LastModifiedTime must be present in UpdatePipe response")

			var f float64
			err := json.Unmarshal(lmtRaw, &f)
			require.NoError(t, err, "LastModifiedTime must be a JSON number (epoch seconds), got: %s", string(lmtRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}
