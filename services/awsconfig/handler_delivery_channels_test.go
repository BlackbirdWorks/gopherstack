package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigHandler_DeleteDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
						"snsTopicARN":  "",
					},
				})
			},
			body:     map[string]any{"DeliveryChannelName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"DeliveryChannelName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteDeliveryChannel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestDeliveryChannelS3KeyPrefix verifies S3KeyPrefix is stored and returned.
func TestDeliveryChannelS3KeyPrefix(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-bucket",
			"s3KeyPrefix":  "config/",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeDeliveryChannels", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"config/"`)
}

// TestDeliveryChannelSnapshotFrequency verifies snapshot delivery properties roundtrip.
func TestDeliveryChannelSnapshotFrequency(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-bucket",
			"configSnapshotDeliveryProperties": map[string]any{
				"deliveryFrequency": "TwentyFour_Hours",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeDeliveryChannels", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TwentyFour_Hours")
}

func TestAWSConfigHandler_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"DeliveryChannel": map[string]any{
					"name":         "default",
					"s3BucketName": "my-bucket",
					"snsTopicARN":  "arn:aws:sns:us-east-1:000000000000:my-topic",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *awsconfig.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "with_channel",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
						"snsTopicARN":  "",
					},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeliveryChannels"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeDeliveryChannels", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_PutDeliveryChannel_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "empty_name_returns_400",
			body: map[string]any{
				"DeliveryChannel": map[string]any{"name": "", "s3BucketName": "my-bucket"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_bucket_returns_400",
			body: map[string]any{
				"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": ""},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeDeliveryChannels_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "filter_one_channel",
			body:      map[string]any{"DeliveryChannelNames": []string{"ch-a"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutDeliveryChannel("ch-a", "bucket-a", "", "", nil))
			require.NoError(t, h.Backend.PutDeliveryChannel("ch-b", "bucket-b", "", "", nil))

			rec := doAWSConfigRequest(t, h, "DescribeDeliveryChannels", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var channels []any
			require.NoError(t, json.Unmarshal(out["DeliveryChannels"], &channels))
			assert.Len(t, channels, tt.wantCount)
		})
	}
}
