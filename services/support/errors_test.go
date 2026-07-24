package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

// TestSupport_ErrValidationSentinel verifies ErrValidation is exported.
func TestSupport_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	assert.Error(t, support.ErrValidation)
}

// TestSupport_HandleError_ErrValidation verifies ErrValidation -> 400.
func TestSupport_HandleError_ErrValidation(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)

	// Missing attachmentId triggers ErrValidation.
	rec := doSupportRequest(t, h, "DescribeAttachment", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSupport_HandleError_ErrUnknown verifies unknown actions -> 400.
func TestSupport_HandleError_ErrUnknown(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSupport_HandleError_WireType verifies every error response carries a
// "__type" field, not just "message". A real AWS SDK client's error
// deserializer (awsAwsjson11_deserializeOpError<Op> in
// aws-sdk-go-v2/service/support/deserializers.go) resolves the exception via
// resolveProtocolErrorType, which checks the X-Amzn-ErrorType header and then
// the body's "__type" field; without either, every error is indistinguishable
// "UnknownError" to a real client. It also locks in that Support's JSON-RPC
// (awsjson1.1) protocol uses HTTP 400 for every client-fault exception --
// including "*NotFound"-named ones, which have no httpError override in the
// service model -- and 500 only for the fault:true InternalServerError shape.
func TestSupport_HandleError_WireType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:     "CaseIdNotFound",
			action:   "ResolveCase",
			body:     map[string]any{"caseId": "case-missing"},
			wantType: "CaseIdNotFound",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "AttachmentIdNotFound",
			action:   "DescribeAttachment",
			body:     map[string]any{"attachmentId": "att-missing"},
			wantType: "AttachmentIdNotFound",
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "AttachmentSetIdNotFound",
			action: "AddAttachmentsToSet",
			body: map[string]any{
				"attachmentSetId": "set-missing",
				"attachments":     []map[string]any{{"fileName": "f.txt", "data": []byte("d")}},
			},
			wantType: "AttachmentSetIdNotFound",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ValidationException",
			action:   "CreateCase",
			body:     map[string]any{"serviceCode": "amazon-s3"},
			wantType: "ValidationException",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "UnknownAction",
			action:   "TotallyMadeUpAction",
			body:     map[string]any{},
			wantType: "ValidationException",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, tt.action, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			resp := decodeSupportResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
			assert.NotEmpty(t, resp["message"])
		})
	}
}
