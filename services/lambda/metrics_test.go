package lambda_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

type pointRecorder struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *pointRecorder) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func (r *pointRecorder) named(name string) []cwmetric.Point {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []cwmetric.Point

	for _, p := range r.points {
		if p.Name == name {
			out = append(out, p)
		}
	}

	return out
}

func dimNames(p cwmetric.Point) []string {
	out := make([]string, len(p.Dimensions))
	for i, d := range p.Dimensions {
		out[i] = d.Name
	}

	return out
}

func TestInvoke_PublishesLambdaMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		reserved     *int
		wantMetric   string
		wantAbsent   []string
		wantErrCount int
	}{
		{name: "throttled", reserved: new(int), wantMetric: "Throttles", wantAbsent: []string{"Invocations"}},
		{name: "runtime_unavailable", wantMetric: "Invocations", wantErrCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, b)

			rec := &pointRecorder{}
			b.SetMetricEmitter(rec)

			require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "metric-fn", PackageType: lambda.PackageTypeImage, ImageURI: "x:latest",
			}))

			if tt.reserved != nil {
				_, err := b.PutFunctionConcurrency("metric-fn", *tt.reserved)
				require.NoError(t, err)
			}

			_, _, _, _, err := b.InvokeFunctionWithQualifier(
				t.Context(), "metric-fn", "", "", "", lambda.InvocationTypeRequestResponse, []byte(`{}`),
			)
			require.Error(t, err)

			points := rec.named(tt.wantMetric)
			require.Len(t, points, 1)
			assert.Equal(t, "AWS/Lambda", points[0].Namespace)
			assert.Equal(t, "us-east-1", points[0].Region)
			assert.Equal(t, []string{"FunctionName"}, dimNames(points[0]))
			assert.InDelta(t, 1, points[0].Value, 0)
			assert.Len(t, rec.named("Errors"), tt.wantErrCount)

			for _, absent := range tt.wantAbsent {
				assert.Empty(t, rec.named(absent))
			}
		})
	}
}

func TestEmitInvocationMetrics_Dimensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		qualifier string
		want      [][]string
		failed    bool
	}{
		{name: "unqualified", want: [][]string{{"FunctionName"}}},
		{name: "latest", qualifier: "$LATEST", want: [][]string{{"FunctionName"}}},
		{
			name: "version", qualifier: "3",
			want: [][]string{{"FunctionName"}, {"FunctionName", "Resource"}},
		},
		{
			name:      "alias",
			qualifier: "prod",
			failed:    true,
			want: [][]string{
				{"FunctionName"},
				{"FunctionName", "Resource"},
				{"FunctionName", "Resource", "ExecutedVersion"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, b)

			rec := &pointRecorder{}
			b.SetMetricEmitter(rec)

			lambda.EmitInvocationMetricsForTest(b, "m-fn", tt.qualifier, "3", 250, tt.failed)

			invocations := rec.named("Invocations")
			got := make([][]string, 0, len(invocations))

			for _, p := range invocations {
				got = append(got, dimNames(p))
			}

			assert.Equal(t, tt.want, got)

			durations := rec.named("Duration")
			require.NotEmpty(t, durations)
			assert.Equal(t, "Milliseconds", durations[0].Unit)
			assert.InDelta(t, 250, durations[0].Value, 0.001)

			wantErrors := 0
			if tt.failed {
				wantErrors = len(tt.want)
			}

			assert.Len(t, rec.named("Errors"), wantErrors)
		})
	}
}
