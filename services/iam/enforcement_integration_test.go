package iam_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

func setupEnforcementTestServer(
	t *testing.T,
	backend *mockEnforcementBackend,
) *httptest.Server {
	t.Helper()

	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			meta := awsmeta.FromRequest(req, "us-east-1")
			ctx := awsmeta.Set(req.Context(), meta)
			c.SetRequest(req.WithContext(ctx))

			return next(c)
		}
	})

	cfg := iam.EnforcementConfig{
		Global: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, true, 0),
	}

	e.Use(iam.EnforcementMiddleware(backend, cfg))

	// Mock S3 handler
	e.PUT("/:bucket/:key", func(c *echo.Context) error {
		return c.String(http.StatusOK, "uploaded")
	})
	e.GET("/:bucket/:key", func(c *echo.Context) error {
		return c.String(http.StatusOK, "content")
	})

	// Mock DynamoDB handler (JSON-RPC)
	e.POST("/", func(c *echo.Context) error {
		target := c.Request().Header.Get("X-Amz-Target")
		if target == "DynamoDB_20120810.PutItem" {
			return c.JSON(http.StatusOK, map[string]any{})
		}
		if target == "DynamoDB_20120810.GetItem" {
			return c.JSON(http.StatusOK, map[string]any{
				"Item": map[string]any{
					"id": map[string]any{"S": "123"},
				},
			})
		}

		return c.JSON(http.StatusOK, map[string]any{})
	})

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func createTestS3Client(
	ctx context.Context,
	endpoint, accessKey, secretKey, sessionToken string,
) (*s3sdk.Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		),
	)
	if err != nil {
		return nil, err
	}

	return s3sdk.NewFromConfig(cfg, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}

func createTestDynamoDBClient(
	ctx context.Context,
	endpoint, accessKey, secretKey, sessionToken string,
) (*dynamodbsdk.Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		),
	)
	if err != nil {
		return nil, err
	}

	return dynamodbsdk.NewFromConfig(cfg, func(o *dynamodbsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	}), nil
}

// TestEnforcement_ConditionOperators_SDKIntegration drives the real
// EnforcementMiddleware with a typed S3 client to prove the IpAddress and
// Date condition operators are enforced end to end. httptest.Server
// connections originate from loopback, so aws:SourceIp always resolves to
// 127.0.0.1 here; the date conditions use a fixed reference far in the past
// so the outcome does not depend on when the test runs (no time.Sleep, no
// clock injection needed).
func TestEnforcement_ConditionOperators_SDKIntegration(t *testing.T) {
	t.Parallel()

	const farPast = "2020-01-01T00:00:00Z"

	tests := []struct {
		policy        map[string]any
		name          string
		expectAllowed bool
	}{
		{
			name: "allowed_when_source_ip_in_loopback_cidr",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{{
					"Effect":   "Allow",
					"Action":   []string{"s3:PutObject"},
					"Resource": []string{"arn:aws:s3:::allowed-bucket/*"},
					"Condition": map[string]any{
						"IpAddress": map[string]any{"aws:SourceIp": "127.0.0.1/32"},
					},
				}},
			},
			expectAllowed: true,
		},
		{
			name: "denied_when_source_ip_outside_cidr",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{{
					"Effect":   "Allow",
					"Action":   []string{"s3:PutObject"},
					"Resource": []string{"arn:aws:s3:::allowed-bucket/*"},
					"Condition": map[string]any{
						"IpAddress": map[string]any{"aws:SourceIp": "10.0.0.0/8"},
					},
				}},
			},
			expectAllowed: false,
		},
		{
			name: "allowed_when_date_less_than_still_in_future",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{{
					"Effect":   "Allow",
					"Action":   []string{"s3:PutObject"},
					"Resource": []string{"arn:aws:s3:::allowed-bucket/*"},
					"Condition": map[string]any{
						"DateGreaterThan": map[string]any{"aws:CurrentTime": farPast},
					},
				}},
			},
			expectAllowed: true,
		},
		{
			name: "denied_when_date_less_than_condition_cannot_hold",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{{
					"Effect":   "Allow",
					"Action":   []string{"s3:PutObject"},
					"Resource": []string{"arn:aws:s3:::allowed-bucket/*"},
					"Condition": map[string]any{
						"DateLessThan": map[string]any{"aws:CurrentTime": farPast},
					},
				}},
			},
			expectAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policyBytes, marshalErr := json.Marshal(tt.policy)
			require.NoError(t, marshalErr)

			const (
				accessKeyID = "AKIACONDITIONUSER"
				userName    = "condition-user"
			)

			backend := newMockEnforcementBackend()
			backend.users[userName] = &iam.User{
				UserName: userName,
				Arn:      "arn:aws:iam::000000000000:user/" + userName,
			}
			backend.keyMap[accessKeyID] = userName
			backend.policies[userName] = []string{string(policyBytes)}

			srv := setupEnforcementTestServer(t, backend)

			client, err := createTestS3Client(t.Context(), srv.URL, accessKeyID, "secret", "")
			require.NoError(t, err)

			_, err = client.PutObject(t.Context(), &s3sdk.PutObjectInput{
				Bucket: aws.String("allowed-bucket"),
				Key:    aws.String("data.json"),
			})

			if tt.expectAllowed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestEnforcement_MultiServiceSDKIntegration(t *testing.T) {
	t.Parallel()

	analystPolicy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect": "Allow",
				"Action": []string{"s3:PutObject", "s3:GetObject"},
				"Resource": []string{
					"arn:aws:s3:::allowed-bucket/*",
					"arn:aws:s3:::allowed-bucket",
				},
			},
			{
				"Effect": "Allow",
				"Action": []string{"dynamodb:PutItem", "dynamodb:GetItem"},
				"Resource": []string{
					"arn:aws:dynamodb:us-east-1:000000000000:table/allowed-table",
				},
			},
		},
	}
	analystPolicyBytes, marshalErr := json.Marshal(analystPolicy)
	require.NoError(t, marshalErr)
	analystPolicyDoc := string(analystPolicyBytes)

	tests := []struct {
		runTest       func(ctx context.Context, t *testing.T, endpoint string)
		name          string
		accessKeyID   string
		userName      string
		policies      []string
		expectAllowed bool
	}{
		{
			name:        "s3_put_object_allowed",
			accessKeyID: "AKIAALLOWEDUSER1",
			userName:    "analyst",
			policies:    []string{analystPolicyDoc},
			runTest: func(ctx context.Context, t *testing.T, endpoint string) {
				t.Helper()
				client, err := createTestS3Client(ctx, endpoint, "AKIAALLOWEDUSER1", "secret", "")
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3sdk.PutObjectInput{
					Bucket: aws.String("allowed-bucket"),
					Key:    aws.String("data.json"),
				})
				assert.NoError(t, err)
			},
		},
		{
			name:        "s3_put_object_denied_on_unauthorized_bucket",
			accessKeyID: "AKIAALLOWEDUSER2",
			userName:    "analyst2",
			policies:    []string{analystPolicyDoc},
			runTest: func(ctx context.Context, t *testing.T, endpoint string) {
				t.Helper()
				client, err := createTestS3Client(ctx, endpoint, "AKIAALLOWEDUSER2", "secret", "")
				require.NoError(t, err)

				_, err = client.PutObject(ctx, &s3sdk.PutObjectInput{
					Bucket: aws.String("restricted-bucket"),
					Key:    aws.String("data.json"),
				})
				assert.Error(t, err)
			},
		},
		{
			name:        "dynamodb_put_item_allowed",
			accessKeyID: "AKIAALLOWEDUSER3",
			userName:    "analyst3",
			policies:    []string{analystPolicyDoc},
			runTest: func(ctx context.Context, t *testing.T, endpoint string) {
				t.Helper()
				client, err := createTestDynamoDBClient(ctx, endpoint, "AKIAALLOWEDUSER3", "secret", "")
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodbsdk.PutItemInput{
					TableName: aws.String("allowed-table"),
					Item: map[string]ddbtypes.AttributeValue{
						"id": &ddbtypes.AttributeValueMemberS{Value: "123"},
					},
				})
				assert.NoError(t, err)
			},
		},
		{
			name:        "dynamodb_put_item_denied_returns_access_denied_error",
			accessKeyID: "AKIAALLOWEDUSER4",
			userName:    "analyst4",
			policies:    []string{analystPolicyDoc},
			runTest: func(ctx context.Context, t *testing.T, endpoint string) {
				t.Helper()
				client, err := createTestDynamoDBClient(ctx, endpoint, "AKIAALLOWEDUSER4", "secret", "")
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodbsdk.PutItemInput{
					TableName: aws.String("restricted-table"),
					Item: map[string]ddbtypes.AttributeValue{
						"id": &ddbtypes.AttributeValueMemberS{Value: "123"},
					},
				})
				assert.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			backend.users[tt.userName] = &iam.User{
				UserName: tt.userName,
				Arn:      "arn:aws:iam::000000000000:user/" + tt.userName,
			}
			backend.keyMap[tt.accessKeyID] = tt.userName
			backend.policies[tt.userName] = tt.policies

			srv := setupEnforcementTestServer(t, backend)

			tt.runTest(t.Context(), t, srv.URL)
		})
	}
}
