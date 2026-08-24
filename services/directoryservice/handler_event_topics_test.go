package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeEventTopics_Filtering(t *testing.T) {
	t.Parallel()

	t.Run("filter by topic name", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "topic-a"},
		)
		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "topic-b"},
		)

		rec := doRequest(t, h, "DescribeEventTopics", map[string]any{
			"DirectoryId": dirID,
			"TopicNames":  []string{"topic-a"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		body := respBody(t, rec)
		topics, _ := body["EventTopics"].([]any)
		require.Len(t, topics, 1)
		assert.Equal(t, "topic-a", topics[0].(map[string]any)["TopicName"])
	})

	t.Run("duplicate topic registration succeeds, no already-exists code is modeled", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "my-topic"},
		)
		rec := doRequest(
			t,
			h,
			"RegisterEventTopic",
			map[string]any{"DirectoryId": dirID, "TopicName": "my-topic"},
		)
		assert.Equal(t, http.StatusOK, rec.Code)

		listRec := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": dirID})
		body := respBody(t, listRec)
		topics, _ := body["EventTopics"].([]any)
		assert.Len(t, topics, 1, "re-registration must refresh, not duplicate, the topic entry")
	})
}

// --- DomainController lifecycle ---

func TestEventTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "register describe deregister cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Register
			rec1 := doRequest(t, h, "RegisterEventTopic", map[string]any{
				"DirectoryId": dirID,
				"TopicName":   "my-topic",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			topics, _ := r2["EventTopics"].([]any)
			require.Len(t, topics, 1)
			topic := topics[0].(map[string]any)
			assert.Equal(t, "my-topic", topic["TopicName"])

			// Deregister
			rec3 := doRequest(t, h, "DeregisterEventTopic", map[string]any{
				"DirectoryId": dirID,
				"TopicName":   "my-topic",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			// Describe after deregister
			rec4 := doRequest(t, h, "DescribeEventTopics", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)
			var r4 map[string]any
			require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &r4))
			topics2, _ := r4["EventTopics"].([]any)
			assert.Empty(t, topics2)

			_ = tc
		})
	}
}
