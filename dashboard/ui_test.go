package dashboard

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

	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

func TestDashboardHandler(t *testing.T) {
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test redirect from root
	req := httptest.NewRequest("GET", "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/dashboard/dynamodb", w.Header().Get("Location"))

	// Test DynamoDB index (should render template successfully)
	// Note: The index page just renders the shell. Data is loaded via HTMX.
	// So we expect 200 OK.
	req = httptest.NewRequest("GET", "/dashboard/dynamodb", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// It should return 200 because ListTables is called via HTMX, not initial render
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "DynamoDB Tables")
	assert.Contains(t, w.Body.String(), "DynamoDB Tables")
}

func TestDashboardCreate_Table(t *testing.T) {
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Table POST
	form := url.Values{}
	form.Add("tableName", "test-table")
	form.Add("partitionKey", "id")
	form.Add("partitionKeyType", "S")

	req := httptest.NewRequest("POST", "/dashboard/dynamodb/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// In this test environment with dummy credentials, CreateTable will likely fail
	// because there is no backend listening.
	// However, we want to verify the routing reaches the handler.
	// If it reaches the handler, it will try to create table, fail, and render error.
	// So we expect 200 OK (because error is rendered as HTML alert) OR 500 depending on implementation.
	// My implementation returns 200 with error alert.

	// Let's verify it didn't return 404
	assert.NotEqual(t, http.StatusNotFound, w.Code)

	// It should probably return 200 with an error content because of the dummy client failure
	assert.Equal(t, http.StatusOK, w.Code)
	// Body should contain error message about connection or similar
	assert.Contains(t, w.Body.String(), "Error")
}

func TestDashboardDelete_Table(t *testing.T) {
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Delete Table DELETE
	req := httptest.NewRequest("DELETE", "/dashboard/dynamodb/table/test-table", nil)
	// Simulate request from list view
	req.Header.Set("HX-Target", "table-list")
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
	// Create dummy clients
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)

	ddbHandler := ddbbackend.NewHandler()
	s3Backend := s3backend.NewInMemoryBackend(nil)
	s3Handler := s3backend.NewHandler(s3Backend)

	handler := NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Bucket POST
	form := url.Values{}
	form.Add("bucketName", "test-bucket")
	form.Add("versioning", "on")

	req := httptest.NewRequest("POST", "/dashboard/s3/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// In this test environment with dummy credentials, CreateBucket will likely fail.
	// But valid routing + failure = 200 with error alert.
	assert.NotEqual(t, http.StatusNotFound, w.Code)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Error")
}

func TestDashboardCreate_Bucket_Integration(t *testing.T) {
	// Setup In-Memory Backend (mimic main.go)
	s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
	s3Handler := s3backend.NewHandler(s3Backend)

	apiMux := http.NewServeMux()
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Handler))
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Handler))

	inMemClient := &InMemClient{Handler: apiMux}

	// Setup Config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{URL: "http://local/s3", SigningRegion: "us-east-1"}, nil
				}
				return aws.Endpoint{URL: "http://local", SigningRegion: "us-east-1"}, nil
			},
		)),
	)
	assert.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	ddbHandler := ddbbackend.NewHandler()
	handler := NewHandler(ddbClient, s3Client, ddbHandler, s3Handler)

	// Test Create Bucket POST
	form := url.Values{}
	form.Add("bucketName", "test-bucket-integ")
	form.Add("versioning", "off")

	req := httptest.NewRequest("POST", "/dashboard/s3/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should SUCCEED (200 OK and NO error alert)
	assert.Equal(t, http.StatusOK, w.Code)
	if strings.Contains(w.Body.String(), "alert-error") {
		t.Logf("Response Body: %s", w.Body.String())
		t.Fail()
	}

	// Verify bucket exists in backend
	_, err = s3Backend.HeadBucket("test-bucket-integ")
	assert.NoError(t, err)
}
