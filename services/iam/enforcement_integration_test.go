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
) (*httptest.Server, *mockEnforcementBackend) {
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

	return srv, backend
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

			srv, _ := setupEnforcementTestServer(t, backend)

			tt.runTest(t.Context(), t, srv.URL)
		})
	}
}
