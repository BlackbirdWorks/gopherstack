package memorydb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeEngineVersions_DefaultOnly pins DefaultOnly ("only the default version of
// the specified engine"): one version per engine, never one overall.
func TestDescribeEngineVersions_DefaultOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine *string
		want   map[string]int
		name   string
	}{
		{name: "all_engines", want: map[string]int{"valkey": 1, "redis": 1}},
		{name: "redis_only", engine: aws.String("redis"), want: map[string]int{"redis": 1}},
		{name: "valkey_only", engine: aws.String("valkey"), want: map[string]int{"valkey": 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newMemorydbSDKClient(t, newTestHandler(t))
			out, err := client.DescribeEngineVersions(t.Context(), &memorydbsdk.DescribeEngineVersionsInput{
				DefaultOnly: true, Engine: tc.engine,
			})
			require.NoError(t, err)

			got := map[string]int{}
			for _, v := range out.EngineVersions {
				got[aws.ToString(v.Engine)]++
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
