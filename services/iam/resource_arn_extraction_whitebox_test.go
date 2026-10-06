package iam

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractResourceARN_JSONTargetAndLambdaForms(t *testing.T) {
	t.Parallel()

	const (
		acct   = "123456789012"
		region = "us-east-1"
	)

	tests := []struct {
		name   string
		target string
		body   string
		path   string
		want   string
	}{
		{
			name: "kms_alias", target: "TrentService.Encrypt", body: `{"KeyId":"alias/app"}`,
			want: "arn:aws:kms:us-east-1:123456789012:alias/app",
		},
		{
			name: "kms_key_id", target: "TrentService.Encrypt", body: `{"KeyId":"abc"}`,
			want: "arn:aws:kms:us-east-1:123456789012:key/abc",
		},
		{
			name: "dynamodb_table_arn", target: "DynamoDB_20120810.GetItem",
			body: `{"TableName":"arn:aws:dynamodb:us-east-1:123456789012:table/t"}`,
			want: "arn:aws:dynamodb:us-east-1:123456789012:table/t",
		},
		{
			name: "dynamodb_table_name", target: "DynamoDB_20120810.GetItem", body: `{"TableName":"t"}`,
			want: "arn:aws:dynamodb:us-east-1:123456789012:table/t",
		},
		{
			name: "lambda_arn_path",
			path: "/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:fn/invocations",
			want: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		},
		{
			name: "lambda_name_path", path: "/2015-03-31/functions/fn/invocations",
			want: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := tt.path
			if path == "" {
				path = "/"
			}

			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(tt.body))
			if tt.target != "" {
				r.Header.Set("X-Amz-Target", tt.target)
			}

			assert.Equal(t, tt.want, extractResourceARN(r, acct, region))
		})
	}
}
