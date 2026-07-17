package firehose_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestFirehoseHandler_StartDeliveryStreamEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *firehose.Handler)
		body         map[string]any
		name         string
		streamName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success_default_key_type",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			body:     map[string]any{"DeliveryStreamName": "my-stream"},
			wantCode: http.StatusOK,
		},
		{
			name:       "success_customer_managed_cmk",
			streamName: "encrypted-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(
					t, h, "CreateDeliveryStream",
					map[string]any{"DeliveryStreamName": "encrypted-stream"},
				)
			},
			body: map[string]any{
				"DeliveryStreamName": "encrypted-stream",
				"DeliveryStreamEncryptionConfigurationInput": map[string]any{
					"KeyType": "CUSTOMER_MANAGED_CMK",
					"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/test-key-id",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			body:       map[string]any{"DeliveryStreamName": "nonexistent"},
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestStartEncryption_CustomerManagedCMK_MissingKeyARN verifies CUSTOMER_MANAGED_CMK
// without a KeyARN is rejected.
func TestStartEncryption_CustomerManagedCMK_MissingKeyARN(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "KeyARN")
}

// TestStartEncryption_CustomerManagedCMK_WithKeyARN verifies CUSTOMER_MANAGED_CMK with a
// KeyARN is accepted.
func TestStartEncryption_CustomerManagedCMK_WithKeyARN(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
			"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/my-key",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStartEncryption_Rejected_OnKinesisSource verifies SSE is rejected on a
// KinesisStreamAsSource stream.
func TestStartEncryption_Rejected_OnKinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-enc-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "ks-enc-stream",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestStartEncryption_StatusAppearsInDescribe verifies the encryption status and key
// type appear in DescribeDeliveryStream.
func TestStartEncryption_StatusAppearsInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-desc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-desc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "AWS_OWNED_CMK",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "enc-desc-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "EncryptionConfiguration")
	assert.Contains(t, body, "ENABLED")
	assert.Contains(t, body, "AWS_OWNED_CMK")
}

// TestDescribeDeliveryStream_ReturnsEncryption verifies the
// DeliveryStreamEncryptionConfiguration field appears in Describe once enabled.
func TestDescribeDeliveryStream_ReturnsEncryption(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "enc-stream"})
	doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "enc-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"DeliveryStreamEncryptionConfiguration"`)
	assert.Contains(t, rec.Body.String(), `"ENABLED"`)
}

func TestFirehoseHandler_StopDeliveryStreamEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *firehose.Handler)
		name       string
		streamName string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
				doFirehoseRequest(
					t,
					h,
					"StartDeliveryStreamEncryption",
					map[string]any{"DeliveryStreamName": "my-stream"},
				)
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "stop_without_start_succeeds",
			streamName: "plain-stream",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "plain-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption", map[string]any{
				"DeliveryStreamName": tt.streamName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestStopDeliveryStreamEncryption_DisablesEncryption verifies StopDeliveryStreamEncryption
// transitions the stream's encryption status to DISABLED.
func TestStopDeliveryStreamEncryption_DisablesEncryption(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stop-stream")

	doFirehoseRequest(t, h, "StartDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})

	rec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), "DISABLED")
}

func TestStopDeliveryStreamEncryption_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehoseHandler_EncryptionRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	// Create a stream.
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "enc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Enable encryption with a customer managed key.
	rec = doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
			"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/abc123",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Disable encryption.
	rec = doFirehoseRequest(t, h, "StopDeliveryStreamEncryption", map[string]any{"DeliveryStreamName": "enc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestEncryptionStartStopLifecycle verifies that StartDeliveryStreamEncryption
// transitions the stream to ENABLED and StopDeliveryStreamEncryption to DISABLED, for
// both AWS_OWNED_CMK and CUSTOMER_MANAGED_CMK key types.
func TestEncryptionStartStopLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		startBody   map[string]any
		wantKeyType string
	}{
		{
			name: "aws_owned_cmk",
			startBody: map[string]any{
				"DeliveryStreamName": "enc-stream-aws",
				"DeliveryStreamEncryptionConfigurationInput": map[string]any{
					"KeyType": "AWS_OWNED_CMK",
				},
			},
			wantKeyType: "AWS_OWNED_CMK",
		},
		{
			name: "customer_managed_cmk",
			startBody: map[string]any{
				"DeliveryStreamName": "enc-stream-cmk",
				"DeliveryStreamEncryptionConfigurationInput": map[string]any{
					"KeyType": "CUSTOMER_MANAGED_CMK",
					"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/audit-key",
				},
			},
			wantKeyType: "CUSTOMER_MANAGED_CMK",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			streamName := tc.startBody["DeliveryStreamName"].(string)
			auditCreateStream(t, h, streamName, nil)

			// Start encryption.
			startRec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", tc.startBody)
			require.Equal(t, http.StatusOK, startRec.Code)

			// Describe: DeliveryStreamEncryptionConfiguration must show ENABLED.
			desc := auditDescribe(t, h, streamName)
			enc, ok := desc["DeliveryStreamEncryptionConfiguration"].(map[string]any)
			require.True(t, ok, "DeliveryStreamEncryptionConfiguration must be present")
			assert.Equal(t, "ENABLED", enc["Status"])
			assert.Equal(t, tc.wantKeyType, enc["KeyType"])

			// Stop encryption.
			stopRec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
				map[string]any{"DeliveryStreamName": streamName})
			require.Equal(t, http.StatusOK, stopRec.Code)

			// Describe: status must be DISABLED.
			desc2 := auditDescribe(t, h, streamName)
			enc2, ok := desc2["DeliveryStreamEncryptionConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "DISABLED", enc2["Status"])
		})
	}
}

// TestEncryption_CustomerManagedCMK_RequiresKeyARN verifies that CUSTOMER_MANAGED_CMK
// without a KeyARN is rejected, end-to-end via the handler.
func TestEncryption_CustomerManagedCMK_RequiresKeyARN(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "cmk-noarn", nil)

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "cmk-noarn",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
			// KeyARN intentionally omitted.
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
