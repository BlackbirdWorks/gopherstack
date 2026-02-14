package dashboard_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"Gopherstack/dashboard"

	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

func TestDashboardHandler(t *testing.T) {
	t.Parallel()
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test redirect from root
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/dashboard/dynamodb", w.Header().Get("Location"))

	// Test DynamoDB index (should render template successfully)
	// Note: The index page just renders the shell. Data is loaded via HTMX.
	// So we expect 200 OK.
	req = httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// It should return 200 because ListTables is called via HTMX, not initial render
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "DynamoDB Tables")
	assert.Contains(t, w.Body.String(), "DynamoDB Tables")
}

func TestDashboardCreate_Table(t *testing.T) {
	t.Parallel()
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Table POST
	form := url.Values{}
	form.Add("tableName", "test-table")
	form.Add("partitionKey", "id")
	form.Add("partitionKeyType", "S")

	req := httptest.NewRequest(http.MethodPost, "/dashboard/dynamodb/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// In this test environment with dummy credentials, CreateTable will fail.
	// The handler uses the HTMX pattern: 422 status with error in Hx-Trigger header.
	assert.NotEqual(t, http.StatusNotFound, w.Code)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.Contains(t, w.Header().Get("Hx-Trigger"), "error")
}

func TestDashboardDelete_Table(t *testing.T) {
	t.Parallel()
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Delete Table DELETE
	req := httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/table/test-table", nil)
	// Simulate request from list view
	req.Header.Set("Hx-Target", "table-list")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should fail with 500 because of dummy client, but confirms routing works
	// If routing failed, it would return 200 (detail page) or 404.
	// Since we expect error from DeleteTable, it should return 500 + error message.

	// Wait, dynamoDBDeleteTable returns http.Error(..., 500)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "Failed to delete table")
}

func TestDashboardCreate_Bucket(t *testing.T) {
	t.Parallel()
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Bucket POST
	form := url.Values{}
	form.Add("bucketName", "test-bucket")
	form.Add("versioning", "on")

	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// In this test environment with dummy credentials, CreateBucket will fail.
	// The handler uses the HTMX pattern: 422 status with error in Hx-Trigger header.
	assert.NotEqual(t, http.StatusNotFound, w.Code)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.Contains(t, w.Header().Get("Hx-Trigger"), "error")
}

func TestDashboardCreate_Bucket_Integration(t *testing.T) {
	t.Parallel()
	// Setup In-Memory Backend (mimic main.go)
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	apiMux := http.NewServeMux()
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Handler))
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	inMemClient := &dashboard.InMemClient{Handler: apiMux}

	// Setup Config
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local/s3")
	})

	ddbHandler := ddbbackend.NewHandler()
	handler := dashboard.NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Bucket POST
	form := url.Values{}
	form.Add("bucketName", "test-bucket-integ")
	form.Add("versioning", "off")

	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should SUCCEED (200 OK and NO error alert)
	require.Equal(t, http.StatusOK, w.Code)
	if strings.Contains(w.Body.String(), "alert-error") {
		t.Logf("Response Body: %s", w.Body.String())
		t.Fail()
	}

	// Verify bucket exists in backend
	_, err = s3Backend.HeadBucket("test-bucket-integ")
	require.NoError(t, err)
}
