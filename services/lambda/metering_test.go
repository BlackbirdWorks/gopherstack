package lambda_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestInvocationObserverReportsBillingDimensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantArch string
		arch     []string
	}{
		{name: "default x86", wantArch: "x86_64"},
		{name: "arm", arch: []string{"arm64"}, wantArch: "arm64"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, b)

			require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "bill-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:latest",
				MemorySize: 256, Architectures: tc.arch, Tags: map[string]string{"Team": "core"},
			}))

			var got []lambda.InvocationUsage

			b.SetInvocationObserver(func(u lambda.InvocationUsage) { got = append(got, u) })
			lambda.EmitInvocationMetricsForTest(b, "bill-fn", "", "", 1500, false)

			require.Len(t, got, 1)
			assert.Equal(t, "bill-fn", got[0].FunctionName)
			assert.Equal(t, 256, got[0].MemoryMB)
			assert.Equal(t, tc.wantArch, got[0].Architecture)
			assert.Equal(t, 1500*time.Millisecond, got[0].Duration)
			assert.Equal(t, "core", got[0].Tags["Team"])
			assert.Equal(t, "us-east-1", got[0].Region)

			b.SetInvocationObserver(nil)
			lambda.EmitInvocationMetricsForTest(b, "bill-fn", "", "", 10, false)
			assert.Len(t, got, 1)
		})
	}
}
