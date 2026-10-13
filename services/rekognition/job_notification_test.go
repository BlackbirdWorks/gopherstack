package rekognition_test

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

type recordingNotifier struct {
	topics   []string
	messages []string
	mu       sync.Mutex
}

func (r *recordingNotifier) Publish(topic, msg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.topics = append(r.topics, topic)
	r.messages = append(r.messages, msg)

	return nil
}

func TestStartJobNotificationChannel(t *testing.T) {
	t.Parallel()

	const topic = "arn:aws:sns:us-east-1:000000000000:rek"

	tests := []struct {
		channel    map[string]any
		name       string
		action     string
		getAction  string
		wantAPI    string
		wantStatus int
		wantNotify bool
	}{
		{
			name: "label_detection", action: "StartLabelDetection", getAction: "GetLabelDetection",
			wantAPI:    "StartLabelDetection",
			channel:    map[string]any{"SNSTopicArn": topic, "RoleArn": "arn:aws:iam::000000000000:role/r"},
			wantStatus: http.StatusOK, wantNotify: true,
		},
		{
			name: "face_search", action: "StartFaceSearch", getAction: "GetFaceSearch",
			wantAPI:    "StartFaceSearch",
			channel:    map[string]any{"SNSTopicArn": topic, "RoleArn": "arn:aws:iam::000000000000:role/r"},
			wantStatus: http.StatusOK, wantNotify: true,
		},
		{
			name: "no_channel", action: "StartTextDetection", getAction: "GetTextDetection",
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_role", action: "StartLabelDetection",
			channel:    map[string]any{"SNSTopicArn": topic},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			n := &recordingNotifier{}

			bk, ok := h.Backend.(*rekognition.InMemoryBackend)
			require.True(t, ok)
			bk.SetJobNotifier(n)

			body := map[string]any{
				"Video":        map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "v.mp4"}},
				"JobTag":       "tag1",
				"CollectionId": "c",
			}
			if tt.channel != nil {
				body["NotificationChannel"] = tt.channel
			}

			if tt.action == "StartFaceSearch" {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "c"})
			}

			rec := doRequest(t, h, tt.action, body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				assert.Empty(t, n.messages)

				return
			}

			var start map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &start))

			if !tt.wantNotify {
				assert.Empty(t, n.messages)

				return
			}

			require.Len(t, n.messages, 1)
			assert.Equal(t, topic, n.topics[0])

			var msg map[string]any
			require.NoError(t, json.Unmarshal([]byte(n.messages[0]), &msg))
			assert.Equal(t, start["JobId"], msg["JobId"])
			assert.Equal(t, "SUCCEEDED", msg["Status"])
			assert.Equal(t, tt.wantAPI, msg["API"])
			assert.Equal(t, "tag1", msg["JobTag"])
			assert.Equal(t, map[string]any{"S3ObjectName": "v.mp4", "S3Bucket": "b"}, msg["Video"])

			get := doRequest(t, h, tt.getAction, map[string]any{"JobId": start["JobId"]})
			require.Equal(t, http.StatusOK, get.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(get.Body.Bytes(), &got))
			assert.Equal(t, "SUCCEEDED", got["JobStatus"])
		})
	}
}
