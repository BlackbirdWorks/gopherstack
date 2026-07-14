package cloudformation_test

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2Audit_StackInstances_OperationId verifies that
// CreateStackInstances, DeleteStackInstances, and UpdateStackInstances all
// return an OperationId in their result, matching AWS CloudFormation behaviour.
// Previously these handlers returned empty responses with no OperationId.
func TestBatch2Audit_StackInstances_OperationId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraFields url.Values
		name        string
		action      string
		resultTag   string
	}{
		{
			name:      "create_instances_returns_operation_id",
			action:    "CreateStackInstances",
			resultTag: "CreateStackInstancesResult",
			extraFields: url.Values{
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			},
		},
		{
			name:      "delete_instances_returns_operation_id",
			action:    "DeleteStackInstances",
			resultTag: "DeleteStackInstancesResult",
			extraFields: url.Values{
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			},
		},
		{
			name:      "update_instances_returns_operation_id",
			action:    "UpdateStackInstances",
			resultTag: "UpdateStackInstancesResult",
			extraFields: url.Values{
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			// Create the stack set.
			rec := postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"opid-test-set"},
				"TemplateBody": {`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			// Ensure we have an instance to delete/update.
			if tt.action != "CreateStackInstances" {
				seedRec := postForm(t, h, url.Values{
					"Action":            {"CreateStackInstances"},
					"StackSetName":      {"opid-test-set"},
					"Accounts.member.1": {"111111111111"},
					"Regions.member.1":  {"us-east-1"},
				}.Encode())
				require.Equal(t, http.StatusOK, seedRec.Code)
			}

			fields := url.Values{
				"Action":       {tt.action},
				"StackSetName": {"opid-test-set"},
			}
			maps.Copy(fields, tt.extraFields)

			rec = postForm(t, h, fields.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, tt.resultTag,
				"response must contain result wrapper element")
			opID := extractField(body, "OperationId")
			assert.NotEmpty(t, opID,
				"OperationId must be present in %s response", tt.action)
		})
	}
}

// TestBatch2Audit_DeleteStackSet_NotEmpty verifies that DeleteStackSet returns
// StackSetNotEmptyException when the stack set still has instances, matching
// AWS CloudFormation behaviour. Previously the backend silently deleted all
// instances alongside the stack set.
func TestBatch2Audit_DeleteStackSet_NotEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantErrorCode string
		createInst    bool
		wantOK        bool
	}{
		{
			name:       "delete_empty_set_succeeds",
			createInst: false,
			wantOK:     true,
		},
		{
			name:          "delete_non_empty_set_fails",
			createInst:    true,
			wantOK:        false,
			wantErrorCode: "StackSetNotEmptyException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"del-test-set"},
				"TemplateBody": {`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.createInst {
				instRec := postForm(t, h, url.Values{
					"Action":            {"CreateStackInstances"},
					"StackSetName":      {"del-test-set"},
					"Accounts.member.1": {"111111111111"},
					"Regions.member.1":  {"us-east-1"},
				}.Encode())
				require.Equal(t, http.StatusOK, instRec.Code)
			}

			rec = postForm(t, h, url.Values{
				"Action":       {"DeleteStackSet"},
				"StackSetName": {"del-test-set"},
			}.Encode())

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code,
					"DeleteStackSet on empty set must succeed")
			} else {
				assert.NotEqual(t, http.StatusOK, rec.Code,
					"DeleteStackSet on non-empty set must fail")
				assert.Contains(t, rec.Body.String(), tt.wantErrorCode,
					"error code must be StackSetNotEmptyException")
			}
		})
	}
}

// TestBatch2Audit_DeleteStackSet_Idempotent verifies that DeleteStackSet on a
// StackSet name that was never created (or already deleted) is a silent
// no-op, not a StackSetNotFoundException. Confirmed against the SDK's
// generated DeleteStackSet error deserializer, whose modeled error set is
// exactly {OperationInProgressException, StackSetNotEmptyException} — no
// "not found" case — mirroring the already-established DeleteStack
// idempotency fix in this codebase.
func TestBatch2Audit_DeleteStackSet_Idempotent(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":       {"DeleteStackSet"},
		"StackSetName": {"never-existed-set"},
	}.Encode())

	assert.Equal(t, http.StatusOK, rec.Code,
		"DeleteStackSet on a nonexistent StackSet must succeed idempotently")
	assert.NotContains(t, rec.Body.String(), "StackSetNotFoundException")
}

// TestBatch2Audit_UnsuffixedNotFoundCodes verifies that GeneratedTemplate and
// ResourceScan not-found errors use the exact (unsuffixed) wire code the SDK
// models — ErrorCode() on GeneratedTemplateNotFoundException/
// ResourceScanNotFoundException returns "GeneratedTemplateNotFound" /
// "ResourceScanNotFound" with no "Exception" suffix (same bug class as the
// already-fixed ChangeSetNotFound). Sending the "...Exception"-suffixed code
// this codebase previously emitted means aws-sdk-go-v2 clients never
// recognize it as the typed exception and fall back to a generic APIError.
func TestBatch2Audit_UnsuffixedNotFoundCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		idParam  string
		wantCode string
	}{
		{
			name:     "DescribeGeneratedTemplate",
			action:   "DescribeGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "GetGeneratedTemplate",
			action:   "GetGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "UpdateGeneratedTemplate",
			action:   "UpdateGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "DeleteGeneratedTemplate",
			action:   "DeleteGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "DescribeResourceScan",
			action:   "DescribeResourceScan",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
		{
			name:     "ListResourceScanResources",
			action:   "ListResourceScanResources",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
		{
			name:     "ListResourceScanRelatedResources",
			action:   "ListResourceScanRelatedResources",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, url.Values{
				"Action":   {tt.action},
				tt.idParam: {"does-not-exist"},
			}.Encode())

			assert.NotEqual(t, http.StatusOK, rec.Code, "%s on an unknown ID must fail", tt.action)
			assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantCode+"</Code>",
				"%s must use the exact unsuffixed SDK-modeled error code", tt.action)
		})
	}
}

// TestBatch2Audit_DescribeStackSetOperation_Action verifies that
// DescribeStackSetOperation returns the Action field in its response, matching
// AWS CloudFormation behaviour. Previously only OperationId and Status were
// returned; the Action (e.g. CREATE_INSTANCES, DETECT_DRIFT) was omitted.
func TestBatch2Audit_DescribeStackSetOperation_Action(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		triggerAction string
		triggerFields url.Values
		wantAction    string
	}{
		{
			name:          "detect_drift_action_present",
			triggerAction: "DetectStackSetDrift",
			triggerFields: url.Values{},
			wantAction:    "DETECT_DRIFT",
		},
		{
			name:          "create_instances_action_present",
			triggerAction: "CreateStackInstances",
			triggerFields: url.Values{
				"Accounts.member.1": {"111111111111"},
				"Regions.member.1":  {"us-east-1"},
			},
			wantAction: "CREATE_INSTANCES",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"action-test-set"},
				"TemplateBody": {`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			fields := url.Values{
				"Action":       {tt.triggerAction},
				"StackSetName": {"action-test-set"},
			}
			maps.Copy(fields, tt.triggerFields)
			triggerRec := postForm(t, h, fields.Encode())
			require.Equal(t, http.StatusOK, triggerRec.Code)

			opID := extractField(triggerRec.Body.String(), "OperationId")
			require.NotEmpty(t, opID, "OperationId must be present in trigger response")

			rec = postForm(t, h, url.Values{
				"Action":       {"DescribeStackSetOperation"},
				"StackSetName": {"action-test-set"},
				"OperationId":  {opID},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			action := extractField(body, "Action")
			assert.Equal(t, tt.wantAction, action,
				"DescribeStackSetOperation must return Action field")
		})
	}
}

// TestBatch2Audit_UpdateStackSet_Description verifies that UpdateStackSet
// updates the Description field when supplied, matching AWS CloudFormation
// behaviour. Previously Description was silently ignored.
func TestBatch2Audit_UpdateStackSet_Description(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		updateDesc      string
		wantDescription string
	}{
		{
			name:            "description_updated",
			updateDesc:      "my updated description",
			wantDescription: "my updated description",
		},
		{
			name:            "empty_description_not_cleared",
			updateDesc:      "",
			wantDescription: "initial description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, url.Values{
				"Action":       {"CreateStackSet"},
				"StackSetName": {"desc-test-set"},
				"Description":  {"initial description"},
				"TemplateBody": {`{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			rec = postForm(t, h, url.Values{
				"Action":       {"UpdateStackSet"},
				"StackSetName": {"desc-test-set"},
				"Description":  {tt.updateDesc},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			rec = postForm(t, h, url.Values{
				"Action":       {"DescribeStackSet"},
				"StackSetName": {"desc-test-set"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			desc := extractField(rec.Body.String(), "Description")
			assert.Equal(t, tt.wantDescription, desc,
				"DescribeStackSet Description must reflect UpdateStackSet result")
		})
	}
}
