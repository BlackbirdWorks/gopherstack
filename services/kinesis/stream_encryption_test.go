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
		"KeyId":          "alias/arn-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "StopStreamEncryption", map[string]any{
		"StreamARN":      descResp.StreamDescription.StreamARN,
		"EncryptionType": "KMS",
		"KeyId":          "alias/arn-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStopEncryption_ResetsFields verifies StopEncryption clears type/key.
func TestStopEncryption_ResetsFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "stop-enc-stream", "ShardCount": 1})
	doRequest(t, h, "StartStreamEncryption", map[string]any{
		"StreamName": "stop-enc-stream", "EncryptionType": "KMS", "KeyId": "alias/k",
	})
	doRequest(t, h, "StopStreamEncryption", map[string]any{
		"StreamName": "stop-enc-stream", "EncryptionType": "KMS", "KeyId": "alias/k",
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
			keyID:         "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
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

// TestStartStreamEncryption_KeyIDFormat verifies StartStreamEncryption's
// required KeyId is validated against the shapes AWS documents (UUID, key
// ARN, alias ARN, "alias/..." name) even with no KMSKeyValidator wired.
func TestStartStreamEncryption_KeyIDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyID   string
		wantErr bool
	}{
		{name: "uuid", keyID: "12345678-1234-1234-1234-123456789012", wantErr: false},
		{
			name:    "key_arn",
			keyID:   "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
			wantErr: false,
		},
		{name: "alias_arn", keyID: "arn:aws:kms:us-east-1:123456789012:alias/MyAliasName", wantErr: false},
		{name: "alias_name", keyID: "alias/MyAliasName", wantErr: false},
		{name: "kinesis_owned_alias", keyID: "alias/aws/kinesis", wantErr: false},
		{name: "empty", keyID: "", wantErr: true},
		{name: "garbage", keyID: "not-a-real-key-id", wantErr: true},
		{name: "bare_arn_without_resource", keyID: "arn:aws:kms:us-east-1:123456789012:", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			streamName := "kmsfmt-" + tt.name
			require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: streamName,
			}))

			err := bk.StartStreamEncryption(context.Background(), &kinesis.StartStreamEncryptionInput{
				StreamName:     streamName,
				EncryptionType: "KMS",
				KeyID:          tt.keyID,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, kinesis.ErrInvalidArgument)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// fakeKMSValidator is a test double for kinesis.KMSKeyValidator that returns
// a scripted error (or nil) for a specific keyID, letting tests exercise the
// StartStreamEncryption -> KMS cross-service validation path without a real
// KMS backend (mirrors how cli.go's wireKinesisKMS wires the real one).
type fakeKMSValidator struct {
	err   error
	keyID string
}

func (f *fakeKMSValidator) ValidateKMSKey(_ context.Context, keyID string) error {
	if keyID == f.keyID {
		return f.err
	}

	return nil
}

// TestStartStreamEncryption_KMSValidator verifies that when a KMSKeyValidator
// is wired (WithKMSValidator), StartStreamEncryption surfaces the exact
// KMS-specific exceptions the real aws-sdk-go-v2/service/kinesis error model
// declares for StartStreamEncryption (KMSNotFoundException/
// KMSDisabledException/KMSInvalidStateException).
func TestStartStreamEncryption_KMSValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		validatorErr error
		wantErr      error
		name         string
	}{
		{name: "key_not_found", validatorErr: kinesis.ErrKMSNotFound, wantErr: kinesis.ErrKMSNotFound},
		{name: "key_disabled", validatorErr: kinesis.ErrKMSDisabled, wantErr: kinesis.ErrKMSDisabled},
		{name: "key_invalid_state", validatorErr: kinesis.ErrKMSInvalidState, wantErr: kinesis.ErrKMSInvalidState},
		{name: "key_valid", validatorErr: nil, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			streamName := "kmsval-" + tt.name
			require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: streamName,
			}))

			bk.WithKMSValidator(&fakeKMSValidator{keyID: "alias/scripted-key", err: tt.validatorErr})

			err := bk.StartStreamEncryption(context.Background(), &kinesis.StartStreamEncryptionInput{
				StreamName:     streamName,
				EncryptionType: "KMS",
				KeyID:          "alias/scripted-key",
			})

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestStartStreamEncryption_StreamNotFoundBeforeKeyFormat verifies stream
// existence is checked before KeyId format/KMS validation, so a nonexistent
// stream always surfaces ResourceNotFoundException regardless of KeyId shape.
func TestStartStreamEncryption_StreamNotFoundBeforeKeyFormat(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	err := bk.StartStreamEncryption(context.Background(), &kinesis.StartStreamEncryptionInput{
		StreamName:     "does-not-exist",
		EncryptionType: "KMS",
		KeyID:          "totally-malformed",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, kinesis.ErrStreamNotFound)
}
