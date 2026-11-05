package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

func TestLambdaWebKeys_CheckKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		usage   string
		disable bool
		missing bool
		wantErr bool
	}{
		{name: "symmetric enabled"},
		{name: "disabled", disable: true, wantErr: true},
		{name: "missing", missing: true, wantErr: true},
		{name: "signing key", usage: "SIGN_VERIFY", spec: "RSA_2048", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bk := kmsbackend.NewInMemoryBackend()
			ctx := t.Context()

			out, err := bk.CreateKey(ctx, &kmsbackend.CreateKeyInput{KeyUsage: tc.usage, KeySpec: tc.spec})
			require.NoError(t, err)

			arn := out.KeyMetadata.Arn
			if tc.missing {
				arn = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-0000-0000-000000000000"
			}

			if tc.disable {
				require.NoError(t, bk.DisableKey(ctx, &kmsbackend.DisableKeyInput{KeyID: out.KeyMetadata.KeyID}))
			}

			err = lambdaWebKeys{backend: bk}.CheckKey(ctx, "us-east-1", arn)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}
