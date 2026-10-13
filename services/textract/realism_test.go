package textract_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	loc := map[string]any{"S3Object": map[string]any{"Bucket": "bkt", "Name": "doc.pdf"}}

	startBody := func(extra map[string]any) map[string]any {
		m := map[string]any{"DocumentLocation": loc, "FeatureTypes": []string{"TABLES"}}
		maps.Copy(m, extra)

		return m
	}

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantMsg  string
	}{
		{
			name: "detect_text_empty_document", action: "DetectDocumentText", body: map[string]any{},
			wantType: "InvalidParameterException", wantMsg: "Document must specify Bytes or S3Object",
		},
		{
			name: "analyze_document_empty_document", action: "AnalyzeDocument",
			body:     map[string]any{"FeatureTypes": []string{"TABLES"}},
			wantType: "InvalidParameterException", wantMsg: "Document must specify Bytes or S3Object",
		},
		{
			name: "job_tag_pattern", action: "StartDocumentAnalysis",
			body:     startBody(map[string]any{"JobTag": "bad tag!"}),
			wantType: "InvalidParameterException", wantMsg: "JobTag must match",
		},
		{
			name: "client_token_pattern", action: "StartDocumentTextDetection",
			body:     startBody(map[string]any{"ClientRequestToken": "bad tok!"}),
			wantType: "InvalidParameterException", wantMsg: "ClientRequestToken must match",
		},
		{
			name: "notification_topic_arn", action: "StartExpenseAnalysis",
			body: startBody(map[string]any{"NotificationChannel": map[string]any{
				"SNSTopicArn": "bad", "RoleArn": "arn:aws:iam::123456789012:role/r",
			}}),
			wantType: "InvalidParameterException", wantMsg: "SNSTopicArn",
		},
		{
			name: "notification_role_arn", action: "StartLendingAnalysis",
			body: startBody(map[string]any{"NotificationChannel": map[string]any{
				"SNSTopicArn": "arn:aws:sns:us-east-1:123456789012:t", "RoleArn": "bad",
			}}),
			wantType: "InvalidParameterException", wantMsg: "RoleArn",
		},
		{
			name: "get_analysis_garbage_token", action: "GetDocumentAnalysis",
			body:     map[string]any{"JobId": "x", "NextToken": "!!!"},
			wantType: "InvalidParameterException", wantMsg: "NextToken is invalid",
		},
		{
			name: "list_adapters_garbage_token", action: "ListAdapters",
			body:     map[string]any{"NextToken": "!!!"},
			wantType: "ValidationException", wantMsg: "NextToken is invalid",
		},
		{
			name: "adapter_name_pattern", action: "CreateAdapter",
			body:     map[string]any{"AdapterName": "bad name!", "FeatureTypes": []string{"QUERIES"}},
			wantType: "ValidationException", wantMsg: "AdapterName must match",
		},
		{
			name: "job_not_found_has_no_code_prefix", action: "GetDocumentAnalysis",
			body:     map[string]any{"JobId": "nope"},
			wantType: "InvalidJobIdException", wantMsg: "job nope not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTextractRequest(t, newTestHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
			assert.Contains(t, resp["message"], tt.wantMsg)
			assert.NotContains(t, resp["message"], "Exception: ")
		})
	}
}
