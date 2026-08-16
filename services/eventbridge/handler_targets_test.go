package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestHandler_ListTargetsByRule_RuleNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     string
		wantCode int
	}{
		{
			// ListTargetsByRule with a nonexistent rule returns empty list (200), not 404
			name:     "list targets for nonexistent rule returns empty list",
			action:   "ListTargetsByRule",
			body:     `{"Rule":"nonexistent","EventBusName":"default"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutTargetsListAndRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		ruleName           string
		targets            string
		removeIDs          string
		wantRemainingID    string
		wantInitialCount   int
		wantRemainingCount int
	}{
		{
			name:     "put two targets then remove one leaves the other",
			ruleName: "rule-t",
			targets: `[{"Id":"t1","Arn":"arn:aws:lambda:us-east-1:123:function:fn"},` +
				`{"Id":"t2","Arn":"arn:aws:sqs:us-east-1:123:q"}]`,
			wantInitialCount:   2,
			removeIDs:          `["t1"]`,
			wantRemainingID:    "t2",
			wantRemainingCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(
				t,
				handler,
				e,
				"PutRule",
				`{"Name":"`+tt.ruleName+`","ScheduleExpression":"rate(1 minute)"}`,
			)

			rec := makeRequestWithHandler(t, handler, e, "PutTargets",
				`{"Rule":"`+tt.ruleName+`","Targets":`+tt.targets+`}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var putResp struct {
				FailedEntries    []eventbridge.FailedEntry `json:"FailedEntries"`
				FailedEntryCount int                       `json:"FailedEntryCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.Equal(t, 0, putResp.FailedEntryCount)

			rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"`+tt.ruleName+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Targets []eventbridge.Target `json:"Targets"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Targets, tt.wantInitialCount)

			rec = makeRequestWithHandler(t, handler, e, "RemoveTargets",
				`{"Rule":"`+tt.ruleName+`","Ids":`+tt.removeIDs+`}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = makeRequestWithHandler(t, handler, e, "ListTargetsByRule", `{"Rule":"`+tt.ruleName+`"}`)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Targets, tt.wantRemainingCount)
			assert.Equal(t, tt.wantRemainingID, listResp.Targets[0].ID)
		})
	}
}
