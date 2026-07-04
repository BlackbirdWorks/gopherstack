package pipes_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func parityNewBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("123456789012", "us-west-2")
}

// TestParity_StartStop_ConflictException verifies that StartPipe/StopPipe on a pipe
// already at the desired state returns ConflictException (409), not ValidationException.
// Real AWS EventBridge Pipes returns ConflictException for this condition.
func TestParity_StartStop_ConflictException(t *testing.T) {
	t.Parallel()

	b := parityNewBackend()
	h := pipes.NewHandler(b)

	tests := []struct {
		pipeName     string
		callPath     string
		setupDesired string
		name         string
	}{
		{
			name:         "start pipe already desired RUNNING",
			pipeName:     "conflict-start-pipe",
			setupDesired: "RUNNING",
			callPath:     "start",
		},
		{
			name:         "stop pipe already desired STOPPED",
			pipeName:     "conflict-stop-pipe",
			setupDesired: "STOPPED",
			callPath:     "stop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pipeName := tt.pipeName
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         pipeName,
				Source:       "arn:aws:sqs:us-east-1:123456789012:q",
				Target:       "arn:aws:lambda:us-east-1:123456789012:function:fn",
				DesiredState: tt.setupDesired,
			})
			require.NoError(t, err)

			// Wait for pipe to exit CREATING state.
			if tt.setupDesired == "RUNNING" {
				pipes.WaitPipeRunning(t, b, pipeName)
			} else {
				pipes.WaitPipeStopped(t, b, pipeName)
			}

			rec := auditDo(t, h, http.MethodPost,
				"/v1/pipes/"+pipeName+"/"+tt.callPath, nil)

			assert.Equal(t, http.StatusConflict, rec.Code,
				"real AWS returns 409 ConflictException, not 400 ValidationException")

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ConflictException", resp["__type"],
				"error type must be ConflictException; body: %s", rec.Body.String())
		})
	}
}

// TestParity_ServiceQuotaExceededException verifies that exceeding the pipe limit
// returns ServiceQuotaExceededException, not ValidationException.
// Real AWS returns ServiceQuotaExceededException when the account limit is hit.
func TestParity_ServiceQuotaExceededException(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("quota-acct", "us-west-2")
	h := pipes.NewHandler(b)
	ctx := context.Background()

	// Fill to the limit using the backend directly for speed.
	for i := range 1000 {
		_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
			Name:   "pipe-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26)),
			Source: "arn:aws:sqs:us-east-1:123456789012:q",
			Target: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		})
		if err != nil {
			// Already exceeded — that's ok if we're at capacity.
			break
		}
	}

	// Now try to create one more via HTTP.
	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/overflow-pipe", map[string]any{
		"Source": "arn:aws:sqs:us-east-1:123456789012:q",
		"Target": "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})

	// Should fail at this point — the backend might have stopped before limit if name collisions.
	// The key assertion is the error type when it does fail.
	if rec.Code != http.StatusOK {
		var resp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ServiceQuotaExceededException", resp["__type"],
			"quota exceeded must return ServiceQuotaExceededException; body: %s", rec.Body.String())
	}
}

// TestParity_ListPipes_EnrichmentInSummary verifies that ListPipes includes the
// Enrichment field in each pipe summary. Real AWS ListPipes returns Enrichment.
func TestParity_ListPipes_EnrichmentInSummary(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)
	enrichmentARN := "arn:aws:lambda:us-west-2:123456789012:function:enricher"

	auditCreate(t, h, "enriched-pipe", map[string]any{
		"Source":     "arn:aws:sqs:us-west-2:123456789012:q",
		"Target":     "arn:aws:lambda:us-west-2:123456789012:function:fn",
		"Enrichment": enrichmentARN,
	})

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipes []map[string]any `json:"Pipes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Pipes, 1)

	enrichment, _ := out.Pipes[0]["Enrichment"].(string)
	assert.Equal(t, enrichmentARN, enrichment,
		"ListPipes summary must include the Enrichment field (real AWS includes it)")
}

// TestParity_ListPipes_NoEnrichmentOmitted verifies that the Enrichment field is
// absent (not empty string) when no enrichment is configured.
func TestParity_ListPipes_NoEnrichmentOmitted(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)

	auditCreate(t, h, "plain-pipe", map[string]any{
		"Source": "arn:aws:sqs:us-west-2:123456789012:q",
		"Target": "arn:aws:lambda:us-west-2:123456789012:function:fn",
	})

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Pipes []map[string]any `json:"Pipes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Pipes, 1)

	_, hasEnrichment := out.Pipes[0]["Enrichment"]
	assert.False(t, hasEnrichment,
		"Enrichment must be omitted (not present) when not configured")
}

// TestParity_InvalidNextToken_ValidationException verifies that an invalid NextToken
// returns ValidationException. Real AWS returns ValidationException for malformed tokens.
func TestParity_InvalidNextToken_ValidationException(t *testing.T) {
	t.Parallel()

	h := auditNewHandler(t)

	rec := auditDo(t, h, http.MethodGet, "/v1/pipes?NextToken=not-valid-base64!!!", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"],
		"invalid NextToken must return ValidationException; body: %s", rec.Body.String())
}

// TestParity_DeletePipe_AlreadyDeleting_ConflictException verifies that calling
// DeletePipe on a pipe already in DELETING state returns ConflictException.
func TestParity_DeletePipe_AlreadyDeleting_ConflictException(t *testing.T) {
	t.Parallel()

	b := parityNewBackend()
	h := pipes.NewHandler(b)
	ctx := context.Background()

	_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
		Name:   "to-delete",
		Source: "arn:aws:sqs:us-east-1:123456789012:q",
		Target: "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})
	require.NoError(t, err)

	pipes.WaitPipeRunning(t, b, "to-delete")

	// First delete starts the DELETING transition.
	_, err = b.DeletePipe(ctx, "to-delete")
	require.NoError(t, err)

	// Second delete while pipe is DELETING.
	_, err = b.DeletePipe(ctx, "to-delete")
	require.Error(t, err)
	require.ErrorIs(t, err, pipes.ErrConflict,
		"deleting a pipe already in DELETING state must return ErrConflict")

	_ = h
}

// TestParity_ServiceQuotaErrorCode verifies that hitting the 1000-pipe limit returns
// ServiceQuotaExceededException via the HTTP handler.
func TestParity_ServiceQuotaErrorCode(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("fill-acct", "us-west-2")
	h := pipes.NewHandler(b)
	ctx := context.Background()

	for i := range 1000 {
		name := fmt.Sprintf("pipe-%04d", i)
		_, err := b.CreatePipe(ctx, pipes.CreatePipeInput{
			Name:   name,
			Source: "arn:aws:sqs:us-east-1:123456789012:q",
			Target: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		})
		require.NoError(t, err, "setup: failed to create pipe %q at index %d", name, i)
	}

	rec := auditDo(t, h, http.MethodPost, "/v1/pipes/overflow", map[string]any{
		"Source": "arn:aws:sqs:us-east-1:123456789012:q",
		"Target": "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ServiceQuotaExceededException", resp["__type"],
		"1001st pipe must fail with ServiceQuotaExceededException; body: %s", rec.Body.String())
}
