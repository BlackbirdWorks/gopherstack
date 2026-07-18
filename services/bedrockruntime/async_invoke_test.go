package bedrockruntime_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

func TestBackend_AsyncInvoke_CRUD(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// Initially empty.
	invocations := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Empty(t, invocations)

	// Start an invocation.
	inv, err := b.StartAsyncInvoke(
		"anthropic.claude-v2",
		"s3://bucket/prefix/",
		"token-1",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	require.NotNil(t, inv)
	assert.NotEmpty(t, inv.InvocationArn)
	assert.NotEmpty(t, inv.ModelArn)
	assert.Equal(t, "s3://bucket/prefix/", inv.OutputS3URI)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)
	assert.NotZero(t, inv.SubmitTime)
	require.NotNil(t, inv.ClientRequestToken)
	assert.Equal(t, "token-1", *inv.ClientRequestToken)
	assert.Equal(t, map[string]string{"env": "test"}, inv.Tags)

	// GetAsyncInvoke returns the invocation.
	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, inv.InvocationArn, got.InvocationArn)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, got.Status)

	// ListAsyncInvokes returns it.
	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	require.Len(t, list, 1)
	assert.Equal(t, inv.InvocationArn, list[0].InvocationArn)

	// GetAsyncInvoke returns error for unknown ARN.
	_, err = b.GetAsyncInvoke("arn:aws:bedrock:us-east-1:123456789012:async-invoke/nonexistent")
	require.Error(t, err)
}

func TestBackend_AsyncInvoke_MultipleInvocations(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv1, err := b.StartAsyncInvoke("model-1", "s3://bucket/1/", "", nil)
	require.NoError(t, err)
	inv2, err := b.StartAsyncInvoke("model-2", "s3://bucket/2/", "", nil)
	require.NoError(t, err)
	inv3, err := b.StartAsyncInvoke("model-3", "s3://bucket/3/", "", nil)
	require.NoError(t, err)

	assert.NotEqual(t, inv1.InvocationArn, inv2.InvocationArn)
	assert.NotEqual(t, inv2.InvocationArn, inv3.InvocationArn)

	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Len(t, list, 3)
}

func TestBackend_AsyncInvoke_NoClientToken(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)
	require.NotNil(t, inv)
	assert.Nil(t, inv.ClientRequestToken)
}

func TestBackend_StartAsyncInvoke_Idempotency(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// First invocation with token.
	first, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "my-token", nil)
	require.NoError(t, err)

	// Second call with same token should return the same ARN.
	second, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "my-token", nil)
	require.NoError(t, err)
	assert.Equal(t, first.InvocationArn, second.InvocationArn)

	// Only one invocation stored.
	list := b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	assert.Len(t, list, 1)

	// Different token creates a new invocation.
	third, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "other-token", nil)
	require.NoError(t, err)
	assert.NotEqual(t, first.InvocationArn, third.InvocationArn)

	// Empty token always creates a new invocation.
	a, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "", nil)
	require.NoError(t, err)
	aa, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, a.InvocationArn, aa.InvocationArn)
}

func TestBackend_StartAsyncInvoke_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		s3URI   string
		wantErr bool
	}{
		{
			name:    "missing modelId returns error",
			modelID: "",
			s3URI:   "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "missing s3Uri returns error",
			modelID: "anthropic.claude-v2",
			s3URI:   "",
			wantErr: true,
		},
		{
			name:    "valid params succeeds",
			modelID: "anthropic.claude-v2",
			s3URI:   "s3://bucket/",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.StartAsyncInvoke(tt.modelID, tt.s3URI, "", nil)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_StartAsyncInvoke_S3URIValidation(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.StartAsyncInvoke("anthropic.claude-v2", "not-s3://bucket/prefix", "", nil)
	require.Error(t, err, "non-s3:// URI must be rejected")

	_, err = b.StartAsyncInvoke("anthropic.claude-v2", "s3:///no-bucket", "", nil)
	require.Error(t, err, "empty bucket name must be rejected")

	_, valid := b.StartAsyncInvoke("anthropic.claude-v2", "s3://valid-bucket/prefix", "", nil)
	require.NoError(t, valid, "valid s3:// URI must be accepted")
}

func TestGetAsyncInvoke_EndTimeAbsentForInProgress(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)

	// Status is InProgress immediately after creation.
	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, got.Status)
	assert.Nil(t, got.EndTime, "endTime must be nil while InProgress")
}

func TestJanitor_AdvancesAsyncInvoke(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	inv, err := b.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/prefix/", "", nil)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)

	// Directly force-advance; janitor does this after completion delay.
	b.AdvanceAsyncInvokesForTest(time.Duration(0))

	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusCompleted, got.Status)
	assert.NotNil(t, got.EndTime)
}
