package kinesis_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartEncryption_ARNSupport verifies encryption ops accept StreamARN.
func TestStartEncryption_ARNSupport(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "arn-enc-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "arn-enc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			StreamARN string `json:"StreamARN"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	rec = doRequest(t, h, "StartStreamEncryption", map[string]any{
		"StreamARN":      descResp.StreamDescription.StreamARN,
		"EncryptionType": "KMS",
		"KeyId":          "arn-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "StopStreamEncryption", map[string]any{
		"StreamARN":      descResp.StreamDescription.StreamARN,
		"EncryptionType": "KMS",
		"KeyId":          "arn-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStopEncryption_ResetsFields verifies StopEncryption clears type/key.
func TestStopEncryption_ResetsFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "stop-enc-stream", "ShardCount": 1})
	doRequest(t, h, "StartStreamEncryption", map[string]any{
		"StreamName": "stop-enc-stream", "EncryptionType": "KMS", "KeyId": "k",
	})
	doRequest(t, h, "StopStreamEncryption", map[string]any{
		"StreamName": "stop-enc-stream", "EncryptionType": "KMS", "KeyId": "k",
	})

	rec := doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "stop-enc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamDescription struct {
			EncryptionType string `json:"EncryptionType"`
		} `json:"StreamDescription"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "NONE", resp.StreamDescription.EncryptionType)
}

// TestStreamEncryption verifies StartStreamEncryption and StopStreamEncryption.
func TestStreamEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		streamName    string
		encType       string
		keyID         string
		wantStartCode int
		wantStopCode  int
	}{
		{
			name:          "kms_encryption_roundtrip",
			streamName:    "enc-stream-kms",
			encType:       "KMS",
			keyID:         "arn:aws:kms:us-east-1:123456789012:key/test-key-id",
			wantStartCode: http.StatusOK,
			wantStopCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateStream", map[string]any{
				"StreamName": tt.streamName,
				"ShardCount": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Start encryption.
			rec = doRequest(t, h, "StartStreamEncryption", map[string]any{
				"StreamName":     tt.streamName,
				"EncryptionType": tt.encType,
				"KeyId":          tt.keyID,
			})
			assert.Equal(t, tt.wantStartCode, rec.Code)

			// Stop encryption.
			rec = doRequest(t, h, "StopStreamEncryption", map[string]any{
				"StreamName":     tt.streamName,
				"EncryptionType": tt.encType,
				"KeyId":          tt.keyID,
			})
			assert.Equal(t, tt.wantStopCode, rec.Code)
		})
	}
}

// TestStreamEncryption_Errors verifies error cases for encryption operations.
func TestStreamEncryption_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		action      string
		wantErrType string
		wantCode    int
	}{
		{
			name:        "start_stream_not_found",
			action:      "StartStreamEncryption",
			body:        map[string]any{"StreamName": "no-such", "EncryptionType": "KMS", "KeyId": "key-id"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "stop_stream_not_found",
			action:      "StopStreamEncryption",
			body:        map[string]any{"StreamName": "no-such", "EncryptionType": "KMS", "KeyId": "key-id"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:   "start_invalid_encryption_type",
			action: "StartStreamEncryption",
			body: map[string]any{
				"StreamName":     "enc-err-stream",
				"EncryptionType": "INVALID",
				"KeyId":          "key-id",
			},
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidArgumentException",
		},
	}

	h := newTestHandler(t)
	setup := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "enc-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, setup.Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}

				require.NoError(t, json.Unmarshal(got.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}

func TestStreamEncryption_StartStop(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "enc-stream"}))

	// Initially no encryption.
	descOut, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "NONE", descOut.EncryptionType)
	assert.Empty(t, descOut.KeyID)

	// Start encryption.
	require.NoError(t, bk.StartStreamEncryption(context.Background(), &kinesis.StartStreamEncryptionInput{
		StreamName:     "enc-stream",
		EncryptionType: "KMS",
		KeyID:          "alias/aws/kinesis",
	}))

	descOut, err = bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "KMS", descOut.EncryptionType)
	assert.Equal(t, "alias/aws/kinesis", descOut.KeyID)

	// Stop encryption.
	require.NoError(t, bk.StopStreamEncryption(context.Background(), &kinesis.StopStreamEncryptionInput{
		StreamName:     "enc-stream",
		EncryptionType: "KMS",
		KeyID:          "alias/aws/kinesis",
	}))

	descOut, err = bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "NONE", descOut.EncryptionType)
	assert.Empty(t, descOut.KeyID)
}
