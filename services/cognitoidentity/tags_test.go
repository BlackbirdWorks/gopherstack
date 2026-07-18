package cognitoidentity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestInMemoryBackend_TagResource_Validation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.TagResource(context.Background(), "", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidentity.ErrInvalidParameter)
}

func TestInMemoryBackend_UntagResource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget   error
		name        string
		resourceARN string
		tagKeys     []string
	}{
		{
			name:        "empty_arn",
			resourceARN: "",
			tagKeys:     []string{"k"},
			errTarget:   cognitoidentity.ErrInvalidParameter,
		},
		{
			name:        "empty_tag_keys",
			resourceARN: "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/pool",
			tagKeys:     []string{},
			errTarget:   cognitoidentity.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.UntagResource(context.Background(), tt.resourceARN, tt.tagKeys)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errTarget)
		})
	}
}
