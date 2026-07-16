package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExportKeyPair tests key pair export.
func TestExportKeyPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyName string
		create  bool
		wantErr bool
	}{
		{
			name:    "export_existing_key",
			keyName: "my-key",
			create:  true,
			wantErr: false,
		},
		{
			name:    "export_missing_key",
			keyName: "nonexistent-key",
			create:  false,
			wantErr: true,
		},
		{
			name:    "empty_key_name",
			keyName: "",
			create:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			if tt.create {
				_, err := b.CreateKeyPair(tt.keyName)
				require.NoError(t, err)
			}

			pubKey, err := b.ExportKeyPair(tt.keyName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, pubKey)
			assert.Contains(t, pubKey, "ssh-rsa")
		})
	}
}

// TestDescribeInstanceTypeOfferings verifies the instance type list.

// TestHandlerExportKeyPair verifies key pair export handler.
func TestHandlerExportKeyPair(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a key pair first.
	rec := postForm(t, h, "Action=CreateKeyPair&Version=2016-11-15&KeyName=test-export-key")
	require.Equal(t, 200, rec.Code)

	rec = postForm(t, h, "Action=ExportKeyPair&Version=2016-11-15&KeyName=test-export-key")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "ExportKeyPairResponse")
	assert.Contains(t, rec.Body.String(), "ssh-rsa")
}
