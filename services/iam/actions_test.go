package iam_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestExtractIAMAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		method      string
		path        string
		headers     map[string]string
		body        string
		contentType string
		want        string
	}{
		{
			name:   "dynamodb_get_item",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "DynamoDB_20120810.GetItem",
			},
			want: "dynamodb:GetItem",
		},
		{
			name:   "dynamodb_put_item",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "DynamoDB_20120810.PutItem",
			},
			want: "dynamodb:PutItem",
		},
		{
			name:   "kms_create_key",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "TrentService.CreateKey",
			},
			want: "kms:CreateKey",
		},
		{
			name:   "secretsmanager_get_secret",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "secretsmanager.GetSecretValue",
			},
			want: "secretsmanager:GetSecretValue",
		},
		{
			name:   "logs_create_log_group",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "Logs_20140328.CreateLogGroup",
			},
			want: "logs:CreateLogGroup",
		},
		{
			name:   "eventbridge_put_rule",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "AmazonEventBridge.PutRule",
			},
			want: "events:PutRule",
		},
		{
			name:   "states_create_state_machine",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "AmazonStates.CreateStateMachine",
			},
			want: "states:CreateStateMachine",
		},
		{
			name:   "kinesis_create_stream",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "Kinesis_20130901.CreateStream",
			},
			want: "kinesis:CreateStream",
		},
		{
			name:   "ssm_get_parameter",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "AmazonSSM.GetParameter",
			},
			want: "ssm:GetParameter",
		},
		{
			name:   "sqs_send_message",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "AmazonSQS.SendMessage",
			},
			want: "sqs:SendMessage",
		},
		{
			name:        "iam_create_user",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateUser&Version=2010-05-08&UserName=alice",
			want:        "iam:CreateUser",
		},
		{
			name:        "sts_get_caller_identity",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=GetCallerIdentity&Version=2011-06-15",
			want:        "sts:GetCallerIdentity",
		},
		{
			name:        "sns_publish",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=Publish&Version=2010-03-31&TopicArn=arn:aws:sns:us-east-1:000000000000:MyTopic&Message=hello",
			want:        "sns:Publish",
		},
		{
			name:        "ec2_describe_instances",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=DescribeInstances&Version=2016-11-15",
			want:        "ec2:DescribeInstances",
		},
		{
			name:        "cloudwatch_put_metric_data",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=PutMetricData&Version=2010-08-01",
			want:        "cloudwatch:PutMetricData",
		},
		{
			name:   "s3_get_object",
			method: http.MethodGet,
			path:   "/my-bucket/my-key",
			want:   "s3:GetObject",
		},
		{
			name:   "s3_put_object",
			method: http.MethodPut,
			path:   "/my-bucket/my-key",
			want:   "s3:PutObject",
		},
		{
			name:   "s3_delete_object",
			method: http.MethodDelete,
			path:   "/my-bucket/my-key",
			want:   "s3:DeleteObject",
		},
		{
			name:   "s3_list_bucket",
			method: http.MethodGet,
			path:   "/my-bucket",
			want:   "s3:ListBucket",
		},
		{
			name:   "s3_list_all_buckets",
			method: http.MethodGet,
			path:   "/",
			want:   "s3:ListAllMyBuckets",
		},
		{
			name:   "s3_create_bucket",
			method: http.MethodPut,
			path:   "/my-bucket",
			want:   "s3:CreateBucket",
		},
		{
			name:   "unknown_target",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "UnknownService.SomeOperation",
			},
			want: "",
		},
		{
			name:   "target_without_dot",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "NoDot",
			},
			want: "",
		},
		// APIGateway target-based requests.
		{
			name:   "apigateway_create_rest_api",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "APIGateway.CreateRestApi",
			},
			want: "apigateway:CreateRestApi",
		},
		// Lambda target-based requests (AWSLambda prefix).
		{
			name:   "lambda_invoke_target",
			method: http.MethodPost,
			path:   "/",
			headers: map[string]string{
				"X-Amz-Target": "AWSLambda.InvokeFunction",
			},
			want: "lambda:InvokeFunction",
		},
		// Redshift form-encoded requests.
		{
			name:        "redshift_describe_clusters",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=DescribeClusters&Version=2012-12-01",
			want:        "redshift:DescribeClusters",
		},
		// CloudFormation form-encoded requests.
		{
			name:        "cloudformation_create_stack",
			method:      http.MethodPost,
			path:        "/",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateStack&Version=2010-05-15&StackName=my-stack",
			want:        "cloudformation:CreateStack",
		},
		// Lambda REST paths must NOT fall through to S3 detection.
		{
			name:   "lambda_rest_path_not_s3",
			method: http.MethodPost,
			path:   "/2015-03-31/functions/my-func/invocations",
			want:   "",
		},
		// Route53 REST paths must NOT fall through to S3 detection.
		{
			name:   "route53_rest_path_not_s3",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, body)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			got := iam.ExtractIAMAction(req)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractAccessKeyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authorization string
		want          string
	}{
		{
			name: "valid_sigv4",
			authorization: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request," +
				" SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123",
			want: "AKIAIOSFODNN7EXAMPLE",
		},
		{
			name:          "empty_header",
			authorization: "",
			want:          "",
		},
		{
			name:          "no_credential",
			authorization: "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc123",
			want:          "",
		},
		{
			name:          "short_credential",
			authorization: "AWS4-HMAC-SHA256 Credential=AKID",
			want:          "AKID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}

			got := iam.ExtractAccessKeyID(req)
			assert.Equal(t, tt.want, got)
		})
	}
}
