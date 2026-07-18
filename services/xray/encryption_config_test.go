package xray_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestPutEncryptionConfigValidation verifies type validation.
func TestPutEncryptionConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		encType string
		keyID   string
		wantErr bool
	}{
		{name: "NONE valid", encType: "NONE", keyID: "", wantErr: false},
		{name: "KMS with key valid", encType: "KMS", keyID: "arn:aws:kms:us-east-1:123:key/abc", wantErr: false},
		{name: "invalid type", encType: "INVALID", keyID: "", wantErr: true},
		{name: "KMS without key invalid", encType: "KMS", keyID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.PutEncryptionConfig(tt.encType, tt.keyID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
