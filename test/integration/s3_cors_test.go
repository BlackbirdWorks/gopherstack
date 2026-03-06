package integration_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_CORS verifies CORS rule evaluation on OPTIONS preflight requests.
func TestIntegration_S3_CORS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup         func(t *testing.T, client *s3.Client, bucket string)
		name          string
		origin        string
		method        string
		reqHeaders    string
		wantAllow     string
		wantCode      int
	}{
		{
			name: "matching origin and method returns 200 with allow headers",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketCors(t.Context(), &s3.PutBucketCorsInput{
					Bucket: aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{
						CORSRules: []types.CORSRule{
							{
								AllowedOrigins: []string{"https://app.example"},
								AllowedMethods: []string{"PUT"},
							},
						},
					},
				})
				require.NoError(t, err)
			},
			origin:    "https://app.example",
			method:    "PUT",
			wantCode:  http.StatusOK,
			wantAllow: "https://app.example",
		},
		{
			name: "non-matching origin returns 403",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketCors(t.Context(), &s3.PutBucketCorsInput{
					Bucket: aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{
						CORSRules: []types.CORSRule{
							{
								AllowedOrigins: []string{"https://trusted.example"},
								AllowedMethods: []string{"GET"},
							},
						},
					},
				})
				require.NoError(t, err)
			},
			origin:   "https://evil.example",
			method:   "GET",
			wantCode: http.StatusForbidden,
		},
		{
			name: "non-matching method returns 403",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketCors(t.Context(), &s3.PutBucketCorsInput{
					Bucket: aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{
						CORSRules: []types.CORSRule{
							{
								AllowedOrigins: []string{"https://app.example"},
								AllowedMethods: []string{"GET"},
							},
						},
					},
				})
				require.NoError(t, err)
			},
			origin:   "https://app.example",
			method:   "DELETE",
			wantCode: http.StatusForbidden,
		},
		{
			name: "wildcard origin allows any origin",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketCors(t.Context(), &s3.PutBucketCorsInput{
					Bucket: aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{
						CORSRules: []types.CORSRule{
							{
								AllowedOrigins: []string{"*"},
								AllowedMethods: []string{"GET"},
							},
						},
					},
				})
				require.NoError(t, err)
			},
			origin:    "https://any-origin.example",
			method:    "GET",
			wantCode:  http.StatusOK,
			wantAllow: "https://any-origin.example",
		},
		{
			name: "no CORS config returns 403",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No CORS configuration stored — preflight should be rejected.
			},
			origin:   "https://app.example",
			method:   "GET",
			wantCode: http.StatusForbidden,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "cors-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			// Send an OPTIONS preflight directly via HTTP so we can control all headers.
			reqURL := fmt.Sprintf("%s/%s", endpoint, bucket)
			req, err := http.NewRequestWithContext(ctx, http.MethodOptions, reqURL, nil)
			require.NoError(t, err)

			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			if tt.method != "" {
				req.Header.Set("Access-Control-Request-Method", tt.method)
			}

			if tt.reqHeaders != "" {
				req.Header.Set("Access-Control-Request-Headers", tt.reqHeaders)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantAllow != "" {
				assert.Equal(t, tt.wantAllow, resp.Header.Get("Access-Control-Allow-Origin"))
			}
		})
	}
}
