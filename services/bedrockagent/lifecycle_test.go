package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func TestLifecycle_TransitionalStatesSettleOverTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
		b.SetLifecycleDelay(10 * time.Second)

		agent, err := b.CreateAgent(ctx, bedrockagent.AgentConfig{AgentName: "a"})
		require.NoError(t, err)

		prep, err := b.PrepareAgent(ctx, agent.AgentID)
		require.NoError(t, err)
		assert.Equal(t, "PREPARING", prep.AgentStatus)

		kb, err := b.CreateKnowledgeBase(ctx, bedrockagent.KnowledgeBaseConfig{Name: "kb"})
		require.NoError(t, err)
		assert.Equal(t, "CREATING", kb.Status)

		ds, err := b.CreateDataSource(ctx, kb.KnowledgeBaseID, bedrockagent.DataSourceConfig{Name: "ds"})
		require.NoError(t, err)

		job, err := b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "", "")
		require.NoError(t, err)
		assert.Equal(t, "STARTING", job.Status)

		_, err = b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "", "")
		require.ErrorIs(t, err, bedrockagent.ErrAlreadyExists)

		time.Sleep(6 * time.Second)

		got, err := b.GetIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, job.IngestionJobID)
		require.NoError(t, err)
		assert.Equal(t, "IN_PROGRESS", got.Status)

		time.Sleep(5 * time.Second)

		gotAgent, err := b.GetAgent(ctx, agent.AgentID)
		require.NoError(t, err)
		assert.Equal(t, "PREPARED", gotAgent.AgentStatus)

		gotKB, err := b.GetKnowledgeBase(ctx, kb.KnowledgeBaseID)
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", gotKB.Status)

		got, err = b.GetIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, job.IngestionJobID)
		require.NoError(t, err)
		assert.Equal(t, "COMPLETE", got.Status)

		_, err = b.StartIngestionJob(ctx, kb.KnowledgeBaseID, ds.DataSourceID, "", "")
		require.NoError(t, err)
	})
}

func TestRequestRealism_ErrorsAndPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantType string
		wantCode int
	}{
		{
			name:     "not found",
			method:   http.MethodGet,
			path:     "/agents/nope/",
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "max zero",
			method:   http.MethodPost,
			path:     "/knowledgebases/?maxResults=0",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "max over",
			method:   http.MethodPost,
			path:     "/agents/?maxResults=1001",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
		{
			name:     "bad token",
			method:   http.MethodPost,
			path:     "/knowledgebases/?nextToken=zz!",
			wantCode: http.StatusBadRequest,
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := setupHandler(t)
			rec := doRequest(t, h, e, tt.method, tt.path, nil)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"))

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotContains(t, out["message"], "Exception")
		})
	}
}
