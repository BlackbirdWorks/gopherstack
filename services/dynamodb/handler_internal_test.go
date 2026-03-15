package dynamodb

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_ClassifyError_Mapping(t *testing.T) {
	t.Parallel()
	h := NewHandler(NewInMemoryDB())

	tests := []struct {
		err            error
		name           string
		wantType       string
		wantStatusCode int
	}{
		{
			name:           "InternalServerError",
			err:            NewInternalServerError("boom"),
			wantStatusCode: http.StatusInternalServerError,
			wantType:       "InternalServerError",
		},
		{
			name:           "ValidationException",
			err:            NewValidationException("bad"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ValidationException",
		},
		{
			name:           "ResourceNotFoundException",
			err:            NewResourceNotFoundException("missing"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ResourceNotFoundException",
		},
		{
			name:           "ConditionalCheckFailedException",
			err:            NewConditionalCheckFailedException("fail"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ConditionalCheckFailedException",
		},
		{
			name:           "TransactionCanceledException",
			err:            NewTransactionCanceledException("cancel", nil),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "TransactionCanceledException",
		},
		{
			name:           "ItemCollectionSizeLimitExceededException",
			err:            NewItemCollectionSizeLimitExceededException("too big"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ItemCollectionSizeLimitExceededException",
		},
		{
			name:           "LimitExceededException",
			err:            NewLimitExceededException("limit"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "LimitExceededException",
		},
		{
			name:           "ResourceInUseException",
			err:            NewResourceInUseException("in use"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ResourceInUseException",
		},
		{
			name:           "ProvisionedThroughputExceededException",
			err:            NewProvisionedThroughputExceededException("throughput"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ProvisionedThroughputExceededException",
		},
		{
			name:           "ThrottlingException",
			err:            NewThrottlingException("throttle"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ThrottlingException",
		},
		{
			name:           "RequestLimitExceeded",
			err:            NewRequestLimitExceeded("request limit"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "RequestLimitExceeded",
		},
		{
			name:           "TransactionConflictException",
			err:            NewTransactionConflictException("conflict"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "TransactionConflictException",
		},
		{
			name:           "ReplicatedWriteConflictException",
			err:            NewReplicatedWriteConflictException("replicated conflict"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "ReplicatedWriteConflictException",
		},
		{
			name:           "PolicyNotFoundException",
			err:            NewPolicyNotFoundException("missing policy"),
			wantStatusCode: http.StatusBadRequest,
			wantType:       "PolicyNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			status, wireErr := h.classifyError(tt.err)
			assert.Equal(t, tt.wantStatusCode, status)
			assert.Contains(t, wireErr.Type, tt.wantType)
		})
	}
}
