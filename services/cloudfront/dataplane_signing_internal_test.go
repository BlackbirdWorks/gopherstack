package cloudfront

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCannedPolicyDoc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		want     string
	}{
		{
			name:     "plain",
			resource: "https://d1.cloudfront.net/a.txt",
			want: `{"Statement":[{"Resource":"https://d1.cloudfront.net/a.txt",` +
				`"Condition":{"DateLessThan":{"AWS:EpochTime":42}}}]}`,
		},
		{
			name:     "quote and backslash",
			resource: `https://d1.cloudfront.net/a"b\c`,
			want: `{"Statement":[{"Resource":"https://d1.cloudfront.net/a\"b\\c",` +
				`"Condition":{"DateLessThan":{"AWS:EpochTime":42}}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc, err := cannedPolicyDoc(tt.resource, 42)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(doc))
			assert.True(t, json.Valid(doc))
		})
	}
}
