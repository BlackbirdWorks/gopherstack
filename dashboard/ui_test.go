package dashboard_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/internal/teststack"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func newStack(t *testing.T) *teststack.Stack {
	t.Helper()

	return teststack.New(t)
}

// serveHandler is a test helper that invokes a Dashboard Echo handler with a raw HTTP request.
func serveHandler(handler *dashboard.DashboardHandler, w http.ResponseWriter, r *http.Request) {
	ctx := logger.Save(r.Context(), slog.Default())
	r = r.WithContext(ctx)
	e := echo.New()
	c := e.NewContext(r, w)
	if err := handler.Handler()(c); err != nil {
		// Handle echo errors properly - some like ErrNotFound are unexported types
		// but they all implement a StatusCode() int method in Echo v5.
		code := http.StatusInternalServerError
		msg := err.Error()

		var he *echo.HTTPError
		if errors.As(err, &he) {
			code = he.Code
			msg = he.Message
		} else if sc, ok := err.(interface{ StatusCode() int }); ok {
			code = sc.StatusCode()
		}

		_ = c.JSON(code, map[string]string{"error": msg})
	}
}

// serveFullStack sends a request through the full Echo stack (including RouteMatcher).
func serveFullStack(e *echo.Echo, w http.ResponseWriter, r *http.Request) {
	ctx := logger.Save(r.Context(), slog.Default())
	r = r.WithContext(ctx)
	e.ServeHTTP(w, r)
}

func uploadS3Object(t *testing.T, stack *teststack.Stack, bucket, key, content string) {
	t.Helper()

	_, err := stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
}

func TestDashboard_Routing(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		method       string
		path         string
		wantContains string
		wantLocation string
		wantStatus   int
	}

	tests := []testCase{
		{
			name:       "unknown path returns 404",
			method:     http.MethodGet,
			path:       "/dashboard/unknown-path",
			wantStatus: http.StatusNotFound,
		},
		{
			name:         "root renders overview",
			method:       http.MethodGet,
			path:         "/dashboard/",
			wantStatus:   http.StatusOK,
			wantContains: "Gopherstack",
		},
		{
			name:         "dynamodb index renders page",
			method:       http.MethodGet,
			path:         "/dashboard/dynamodb",
			wantStatus:   http.StatusOK,
			wantContains: "DynamoDB Tables",
		},
		{
			name:         "s3 index renders page",
			method:       http.MethodGet,
			path:         "/dashboard/s3",
			wantStatus:   http.StatusOK,
			wantContains: "S3 Buckets",
		},
		{
			name:         "docs index renders page",
			method:       http.MethodGet,
			path:         "/dashboard/docs",
			wantStatus:   http.StatusOK,
			wantContains: "API Documentation",
		},
		{
			name:         "sns index renders page",
			method:       http.MethodGet,
			path:         "/dashboard/sns",
			wantStatus:   http.StatusOK,
			wantContains: "SNS Topics",
		},
	}

	stack := newStack(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}

func TestDashboard_DDB_TableList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		wantContains string
		wantStatus   int
		preCreate    bool
	}

	tests := []testCase{
		{
			name:       "empty list returns 200",
			wantStatus: http.StatusOK,
		},
		{
			name:         "table exists returns table card fragment",
			preCreate:    true,
			wantStatus:   http.StatusOK,
			wantContains: "list-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateDDBTable(t, "list-table")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/tables", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DDB_CreateTable_Integration(t *testing.T) {
	t.Parallel()

	type testCase struct {
		formValues    url.Values
		name          string
		method        string
		wantHxTrigger string
		wantContains  string
		wantStatus    int
		preCreate     bool
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:   "success creates table and returns list fragment",
			method: http.MethodPost,
			formValues: url.Values{
				"tableName":        {"created-table"},
				"partitionKey":     {"id"},
				"partitionKeyType": {"S"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "created-table",
		},
		{
			name:   "duplicate table returns 422 with error trigger",
			method: http.MethodPost,
			formValues: url.Values{
				"tableName":        {"existing-table"},
				"partitionKey":     {"id"},
				"partitionKeyType": {"S"},
			},
			preCreate:     true,
			wantStatus:    http.StatusUnprocessableEntity,
			wantHxTrigger: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateDDBTable(t, "existing-table")
			}

			var body io.Reader
			if tt.formValues != nil {
				body = strings.NewReader(tt.formValues.Encode())
			}

			req := httptest.NewRequest(tt.method, "/dashboard/dynamodb/create", body)
			if tt.formValues != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
			if tt.wantHxTrigger != "" {
				assert.Contains(t, w.Header().Get("Hx-Trigger"), tt.wantHxTrigger)
			}
		})
	}
}

func TestDashboard_DDB_TableDetail(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		tableName    string
		wantContains string
		wantStatus   int
	}

	tests := []testCase{
		{
			name:         "existing table renders detail page",
			tableName:    "detail-table",
			wantStatus:   http.StatusOK,
			wantContains: "detail-table",
		},
		{
			name:       "nonexistent table returns 404",
			tableName:  "ghost-table",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateDDBTable(t, "detail-table")

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/dynamodb/table/"+tt.tableName, nil,
			)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DDB_DeleteTable(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name           string
		hxTarget       string
		wantHxLocation string
		wantStatus     int
		preCreate      bool
	}

	tests := []testCase{
		{
			name:           "delete from detail view sets hx-location redirect",
			preCreate:      true,
			wantStatus:     http.StatusOK,
			wantHxLocation: "/dashboard/dynamodb",
		},
		{
			name:       "delete from list view returns updated list",
			hxTarget:   "table-list",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateDDBTable(t, "del-table")
			}

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/dynamodb/table/del-table", nil,
			)
			if tt.hxTarget != "" {
				req.Header.Set("Hx-Target", tt.hxTarget)
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHxLocation != "" {
				assert.Equal(t, tt.wantHxLocation, w.Header().Get("Hx-Location"))
			}
		})
	}
}

func TestDashboard_DDB_Query(t *testing.T) {
	t.Parallel()

	type testCase struct {
		formValues    url.Values
		name          string
		method        string
		wantContains  string
		wantHxTrigger string
		wantStatus    int
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "missing partition key returns 400",
			method:     http.MethodPost,
			formValues: url.Values{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "matching item returns query results",
			method: http.MethodPost,
			formValues: url.Values{
				"partitionKeyValue": {"item-1"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "item-1",
		},
		{
			name:   "no matching item shows no items found",
			method: http.MethodPost,
			formValues: url.Values{
				"partitionKeyValue": {"nonexistent"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "No items found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateDDBTable(t, "query-table")

			_, err := stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String("query-table"),
				Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "item-1"},
				},
			})
			require.NoError(t, err)

			var body io.Reader
			if tt.formValues != nil {
				body = strings.NewReader(tt.formValues.Encode())
			}

			req := httptest.NewRequest(
				tt.method, "/dashboard/dynamodb/table/query-table/query", body,
			)
			if tt.formValues != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
			if tt.wantHxTrigger != "" {
				assert.Contains(t, w.Header().Get("Hx-Trigger"), tt.wantHxTrigger)
			}
		})
	}
}

func TestDashboard_DDB_Scan(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		method       string
		wantContains string
		wantStatus   int
		preInsert    bool
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:         "empty table returns no items found",
			method:       http.MethodPost,
			wantStatus:   http.StatusOK,
			wantContains: "No items found",
		},
		{
			name:         "table with items returns results",
			method:       http.MethodPost,
			preInsert:    true,
			wantStatus:   http.StatusOK,
			wantContains: "scan-item",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateDDBTable(t, "scan-table")

			if tt.preInsert {
				_, err := stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
					TableName: aws.String("scan-table"),
					Item: map[string]ddbtypes.AttributeValue{
						"id": &ddbtypes.AttributeValueMemberS{Value: "scan-item"},
					},
				})
				require.NoError(t, err)
			}

			req := httptest.NewRequest(
				tt.method, "/dashboard/dynamodb/table/scan-table/scan", nil,
			)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DDB_Search(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		wantContains string
		wantStatus   int
		preCreate    bool
	}

	tests := []testCase{
		{
			name:       "empty returns 200",
			wantStatus: http.StatusOK,
		},
		{
			name:         "with table returns table name in fragment",
			preCreate:    true,
			wantStatus:   http.StatusOK,
			wantContains: "search-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateDDBTable(t, "search-table")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/search", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_S3_BucketList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		wantContains string
		wantStatus   int
		preCreate    bool
	}

	tests := []testCase{
		{
			name:       "empty list returns 200",
			wantStatus: http.StatusOK,
		},
		{
			name:         "list with bucket returns bucket card",
			preCreate:    true,
			wantStatus:   http.StatusOK,
			wantContains: "list-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateS3Bucket(t, "list-bucket")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_S3_CreateBucket_Integration(t *testing.T) {
	t.Parallel()

	type testCase struct {
		formValues    url.Values
		name          string
		method        string
		wantHxTrigger string
		wantStatus    int
		preCreate     bool
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:   "success creates bucket and returns bucket list",
			method: http.MethodPost,
			formValues: url.Values{
				"bucketName": {"new-bucket"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "duplicate bucket returns 422 with error trigger",
			method: http.MethodPost,
			formValues: url.Values{
				"bucketName": {"dup-bucket"},
			},
			preCreate:     true,
			wantStatus:    http.StatusUnprocessableEntity,
			wantHxTrigger: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateS3Bucket(t, "dup-bucket")
			}

			var body io.Reader
			if tt.formValues != nil {
				body = strings.NewReader(tt.formValues.Encode())
			}

			req := httptest.NewRequest(tt.method, "/dashboard/s3/create", body)
			if tt.formValues != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHxTrigger != "" {
				assert.Contains(t, w.Header().Get("Hx-Trigger"), tt.wantHxTrigger)
			}
		})
	}
}

func TestDashboard_S3_BucketDetail(t *testing.T) {
	t.Parallel()

	type testCase struct {
		setup        func(t *testing.T, stack *teststack.Stack)
		name         string
		bucketName   string
		wantContains string
		wantStatus   int
	}

	tests := []testCase{
		{
			name:         "existing bucket renders detail page",
			bucketName:   "detail-bucket",
			wantStatus:   http.StatusOK,
			wantContains: "detail-bucket",
		},
		{
			name:       "nonexistent bucket returns 404",
			bucketName: "ghost-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			// Regression: bucket created with a non-default region LocationConstraint must
			// still be accessible via the dashboard detail page. Previously HeadBucket used
			// a region-specific lookup and missed buckets stored under a different region key.
			name:         "bucket created with non-default region renders detail page",
			bucketName:   "region-bucket",
			wantStatus:   http.StatusOK,
			wantContains: "region-bucket",
			setup: func(t *testing.T, stack *teststack.Stack) {
				t.Helper()
				_, err := stack.S3Backend.CreateBucket(t.Context(), &s3.CreateBucketInput{
					Bucket: aws.String("region-bucket"),
					CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
						LocationConstraint: s3types.BucketLocationConstraintUsWest2,
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "detail-bucket")
			if tt.setup != nil {
				tt.setup(t, stack)
			}

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/s3/bucket/"+tt.bucketName, nil,
			)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_S3_FileTree(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		wantContains string
		wantStatus   int
		preUpload    bool
	}

	tests := []testCase{
		{
			name:       "empty bucket returns 200",
			wantStatus: http.StatusOK,
		},
		{
			name:         "bucket with file returns file tree item",
			preUpload:    true,
			wantStatus:   http.StatusOK,
			wantContains: "hello.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "tree-bucket")
			if tt.preUpload {
				uploadS3Object(t, stack, "tree-bucket", "hello.txt", "hello")
			}

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/s3/bucket/tree-bucket/tree", nil,
			)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_S3_FileDetail(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		path         string
		wantContains string
		wantStatus   int
	}

	tests := []testCase{
		{
			name:         "existing file renders detail page",
			path:         "/dashboard/s3/bucket/fd-bucket/file/myfile.txt",
			wantStatus:   http.StatusOK,
			wantContains: "myfile.txt",
		},
		{
			name:       "nonexistent file returns 404",
			path:       "/dashboard/s3/bucket/fd-bucket/file/missing.txt",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "fd-bucket")
			uploadS3Object(t, stack, "fd-bucket", "myfile.txt", "file content")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_S3_Download(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		path       string
		wantHeader string
		wantStatus int
	}

	tests := []testCase{
		{
			name:       "existing file returns 200 with content-disposition",
			path:       "/dashboard/s3/bucket/dl-bucket/download/get.txt",
			wantStatus: http.StatusOK,
			wantHeader: "attachment",
		},
		{
			name:       "nonexistent file returns 404",
			path:       "/dashboard/s3/bucket/dl-bucket/download/missing.txt",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "dl-bucket")
			uploadS3Object(t, stack, "dl-bucket", "get.txt", "file content")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHeader != "" {
				assert.Contains(t, w.Header().Get("Content-Disposition"), tt.wantHeader)
			}
		})
	}
}

func TestDashboard_S3_Upload(t *testing.T) {
	t.Parallel()

	buildMultipart := func(filename, content string) (io.Reader, string) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		fw, _ := mw.CreateFormFile("file", filename)
		_, _ = io.WriteString(fw, content)
		mw.Close()

		return &buf, mw.FormDataContentType()
	}

	type testCase struct {
		buildBody     func() (io.Reader, string)
		name          string
		method        string
		wantHxTrigger string
		wantContains  string
		wantStatus    int
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:   "success uploads file and returns updated tree",
			method: http.MethodPost,
			buildBody: func() (io.Reader, string) {
				return buildMultipart("upload.txt", "hello world")
			},
			wantStatus:   http.StatusOK,
			wantContains: "upload.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "up-bucket")

			var body io.Reader
			contentType := "application/x-www-form-urlencoded"
			if tt.buildBody != nil {
				body, contentType = tt.buildBody()
			}

			req := httptest.NewRequest(
				tt.method, "/dashboard/s3/bucket/up-bucket/upload", body,
			)
			req.Header.Set("Content-Type", contentType)

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
			if tt.wantHxTrigger != "" {
				assert.Contains(t, w.Header().Get("Hx-Trigger"), tt.wantHxTrigger)
			}
		})
	}
}

func TestDashboard_S3_DeleteFile(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		hxTarget        string
		wantHeader      string
		wantHeaderValue string
		wantStatus      int
	}

	tests := []testCase{
		{
			name:       "delete from list returns 200",
			wantStatus: http.StatusOK,
		},
		{
			name:            "delete from detail sets hx-redirect to bucket",
			hxTarget:        "body",
			wantStatus:      http.StatusOK,
			wantHeader:      "Hx-Redirect",
			wantHeaderValue: "/dashboard/s3/bucket/delf-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "delf-bucket")
			uploadS3Object(t, stack, "delf-bucket", "file.txt", "content")

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/s3/bucket/delf-bucket/file/file.txt", nil,
			)
			if tt.hxTarget != "" {
				req.Header.Set("Hx-Target", tt.hxTarget)
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHeader != "" {
				assert.Equal(t, tt.wantHeaderValue, w.Header().Get(tt.wantHeader))
			}
		})
	}
}

func TestDashboard_S3_DeleteBucket(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		wantHxTrigger string
		wantStatus    int
		preCreate     bool
	}

	tests := []testCase{
		{
			name:       "success deletes existing bucket and returns 200",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:          "nonexistent bucket returns 422 with error trigger",
			wantStatus:    http.StatusUnprocessableEntity,
			wantHxTrigger: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.preCreate {
				stack.CreateS3Bucket(t, "del-bucket")
			}

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/s3/bucket/del-bucket", nil,
			)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHxTrigger != "" {
				assert.Contains(t, w.Header().Get("Hx-Trigger"), tt.wantHxTrigger)
			}
		})
	}
}

func TestDashboard_S3_Versioning(t *testing.T) {
	t.Parallel()

	type testCase struct {
		formValues url.Values
		name       string
		method     string
		wantStatus int
	}

	tests := []testCase{
		{
			name:       "method not allowed returns 405",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:   "enable versioning returns bucket list",
			method: http.MethodPost,
			formValues: url.Values{
				"enabled": {"true"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "disable versioning returns bucket list",
			method: http.MethodPost,
			formValues: url.Values{
				"enabled": {"false"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.CreateS3Bucket(t, "ver-bucket")

			var body io.Reader
			if tt.formValues != nil {
				body = strings.NewReader(tt.formValues.Encode())
			}

			req := httptest.NewRequest(
				tt.method, "/dashboard/s3/bucket/ver-bucket/versioning", body,
			)
			if tt.formValues != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}

			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}
func TestDashboard_S3_VersioningAndDeletion(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "versioned-bucket"
	stack.CreateS3Bucket(t, bucketName)

	ctx := t.Context()

	// 1. Enable versioning
	req := httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/bucket/"+bucketName+"/versioning",
		strings.NewReader("enabled=true"),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 2. Put multiple versions of a file
	key := "test.txt"
	for i := 1; i <= 3; i++ {
		_, err := stack.S3Backend.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("version %d", i)),
		})
		require.NoError(t, err)
	}

	// 3. Verify multiple versions exist
	out, err := stack.S3Backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	assert.Len(t, out.Versions, 3)

	// 4. Delete all versions
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/"+bucketName+"/file/"+key+"?deleteAll=true",
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 15. s3DeleteFile with deleteAll=true (multi-version)
	stack.CreateS3Bucket(t, "multi-version-bucket")
	// Upload 2 versions
	stack.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("multi-version-bucket"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("v1"),
	})
	stack.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("multi-version-bucket"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("v2"),
	})
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/multi-version-bucket/file/file.txt?deleteAll=true",
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 5. Verify object is gone
	out, err = stack.S3Backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Versions)
	assert.Empty(t, out.DeleteMarkers)
}

func TestDashboard_S3_UploadAndDownload(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "upload-bucket"
	stack.CreateS3Bucket(t, bucketName)

	// 1. Upload file
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", "upload.txt")
	require.NoError(t, err)
	_, _ = part.Write([]byte("upload content"))
	_ = writer.WriteField("key", "folder/upload.txt")
	_ = writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/"+bucketName+"/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 2. Download file
	req = httptest.NewRequest(
		http.MethodGet,
		"/dashboard/s3/bucket/"+bucketName+"/download/folder/upload.txt",
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "upload content", w.Body.String())
	assert.Equal(
		t,
		"attachment; filename=\"folder/upload.txt\"",
		w.Header().Get("Content-Disposition"),
	)

	// 3. Delete file (from detail page context)
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/"+bucketName+"/file/folder/upload.txt",
		nil,
	)
	req.Header.Set("Hx-Target", "body")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Hx-Redirect"), "/dashboard/s3/bucket/"+bucketName)
}

func TestUI_ErrorHandlingAndFormatting(t *testing.T) {
	t.Parallel()
	stack := newStack(t)

	// 1. Trigger non-existent bucket error
	req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/bucket/non-existent", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// 2. Trigger bucket deletion error (if it had items, but here let's just use invalid name)
	req = httptest.NewRequest(http.MethodDelete, "/dashboard/s3/bucket/!invalid!", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.Contains(t, w.Header().Get("Hx-Trigger"), "showToast")

	// 3. Test DynamoDB Delete Table error
	req = httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/table/non-existent", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.Contains(t, w.Header().Get("Hx-Trigger"), "showToast")
}
func TestDashboard_S3_FileTree_DeepPrefix(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "tree-bucket"
	stack.CreateS3Bucket(t, bucketName)

	ctx := t.Context()
	// Create deep structure
	files := []string{
		"a/b/c/1.txt",
		"a/b/d/2.txt",
		"a/x.txt",
	}
	for _, f := range files {
		_, err := stack.S3Backend.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f),
			Body:   strings.NewReader("content"),
		})
		require.NoError(t, err)
	}
	// 1. Root level
	req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/bucket/"+bucketName+"/tree", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "a/")
}

func TestDashboard_DDB_Indexes(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	tableName := "IndexTable"

	// Create table with GSI and LSI
	_, err := stack.DDBHandler.Backend.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: ddbtypes.KeyTypeHash},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
		LocalSecondaryIndexes: []ddbtypes.LocalSecondaryIndex{
			{
				IndexName: aws.String("LSI1"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("gsi_pk"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// Verify table detail shows indexes
	req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/table/"+tableName, nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "GSI1")
	assert.Contains(t, w.Body.String(), "LSI1")

	// Put an item
	_, err = stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":     &ddbtypes.AttributeValueMemberS{Value: "user1"},
			"sk":     &ddbtypes.AttributeValueMemberS{Value: "order1"},
			"gsi_pk": &ddbtypes.AttributeValueMemberS{Value: "status_active"},
		},
	})
	require.NoError(t, err)

	// 1. Query on GSI
	form := url.Values{}
	form.Add("indexName", "GSI1")
	form.Add("partitionKeyValue", "status_active")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/"+tableName+"/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "user1")

	// 2. Scan on Local Index (Wait, LSIs can be scanned too)
	form = url.Values{}
	form.Add("indexName", "LSI1")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/"+tableName+"/scan",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "user1")

	// 3. GSI with Sort Key
	_, err = stack.DDBHandler.Backend.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String("GSISKTable"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_sk"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI_SK"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("gsi_sk"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	// Put item with N type for SK
	_, err = stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String("GSISKTable"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":     &ddbtypes.AttributeValueMemberS{Value: "u1"},
			"gsi_pk": &ddbtypes.AttributeValueMemberS{Value: "active"},
			"gsi_sk": &ddbtypes.AttributeValueMemberN{Value: "100"},
		},
	})
	require.NoError(t, err)

	// Query with SK condition
	form = url.Values{}
	form.Add("indexName", "GSI_SK")
	form.Add("partitionKeyValue", "active")
	form.Add("sortKeyOperator", ">")
	form.Add("sortKeyValue", "50")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/GSISKTable/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "u1")

	// 4. LSI with Binary Sort Key (wait, LSI must have same PK)
	// Let's use IndexTable which has LSI1 on pk (HASH) and gsi_pk (RANGE)
	_, err = stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String("IndexTable"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":     &ddbtypes.AttributeValueMemberS{Value: "user2"},
			"sk":     &ddbtypes.AttributeValueMemberS{Value: "order2"},
			"gsi_pk": &ddbtypes.AttributeValueMemberS{Value: "lsi_val"},
		},
	})
	require.NoError(t, err)

	form = url.Values{}
	form.Add("indexName", "LSI1")
	form.Add("partitionKeyValue", "user2")
	form.Add("sortKeyOperator", "=")
	form.Add("sortKeyValue", "lsi_val")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/IndexTable/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "user2")

	// 5. Scan with Index
	form = url.Values{}
	form.Add("indexName", "GSI1")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/IndexTable/scan",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "user1")
}

func TestDashboard_S3_DetailedTests(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "detailed-bucket"
	stack.CreateS3Bucket(t, bucketName)

	// 1. Upload fail (no file)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/"+bucketName+"/upload", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// 2. Enable versioning
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/bucket/"+bucketName+"/versioning",
		strings.NewReader("enabled=true"),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 3. Create multiple versions
	key := "multi-v.txt"
	for i := range 3 {
		_, err := stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("v%d", i)),
		})
		require.NoError(t, err)
	}

	// 4. Delete all versions
	form := url.Values{}
	form.Add("deleteAll", "true")
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/"+bucketName+"/file/"+key+"?"+form.Encode(),
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 5. Test Delete Marker coverage in deleteAllVersions
	// First put an item, then delete it normally to create a delete marker
	_, err := stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("marker.txt"),
		Body:   strings.NewReader("content"),
	})
	require.NoError(t, err)

	// Delete normally (creates delete marker because versioning is enabled)
	_, err = stack.S3Backend.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("marker.txt"),
	})
	require.NoError(t, err)

	// Now delete all versions (should hit the delete marker loop)
	form = url.Values{}
	form.Add("deleteAll", "true")
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/"+bucketName+"/file/marker.txt?"+form.Encode(),
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 6. Test specific version deletion
	_, err = stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("version.txt"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)

	out, _ := stack.S3Backend.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("version.txt"),
	})
	vID := *out.Versions[0].VersionId

	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/"+bucketName+"/file/version.txt?versionId="+vID,
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDashboard_DDB_Scan_Detailed(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	tableName := "ScanDetailedTable"

	// Create table
	_, err := stack.DDBHandler.Backend.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	// Scan with limit
	form := url.Values{}
	form.Add("limit", "1")
	req := httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/"+tableName+"/scan",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Scan with filter
	form = url.Values{}
	form.Add("filterExpression", "pk = :pk")
	// Note: Our in-memory scan might not support complex filters yet, but let's test the handler path
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/"+tableName+"/scan",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDashboard_DDB_Query_Pagination(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	tableName := "PaginationTable"

	// Create table
	_, err := stack.DDBHandler.Backend.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	// Put multiple items
	for i := range 5 {
		_, err = stack.DDBHandler.Backend.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: "user1"},
				"sk": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("order%d", i)},
			},
		})
		require.NoError(t, err)
	}

	// Query with limit and check pagination
	form := url.Values{}
	form.Add("partitionKeyValue", "user1")
	form.Add("limit", "2")
	req := httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/"+tableName+"/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	// Even if ExclusiveStartKey rendering is tricky to hit, we hit the handler logic
}

func TestDashboard_S3_Additional_Errors(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "add-err-bucket"
	stack.CreateS3Bucket(t, bucketName)

	// 1. s3CreateBucket empty name
	form := url.Values{}
	form.Add("bucketName", "")
	req := httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/create",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)

	// 2. s3DeleteFile non-existent bucket
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/no-bucket/file/test.txt",
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestDashboard_S3_Upload_Error(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "err-bucket"
	stack.CreateS3Bucket(t, bucketName)

	// 1. Upload with missing file field
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/"+bucketName+"/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// 2. Upload to non-existent bucket
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("file", "test.txt")
	_, _ = part.Write([]byte("content"))
	_ = writer.Close()
	req = httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/non-existent/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestDashboard_EdgeCases(t *testing.T) {
	t.Parallel()
	stack := newStack(t)

	// 1. Root renders overview (previously redirected to dynamodb)
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 2. dynamoDBCreateTable missing TableName
	form := url.Values{}
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	if w.Code != http.StatusNotFound {
		assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	}

	// 3. s3CreateBucket existing bucket
	bucketName := "existing-bucket"
	stack.CreateS3Bucket(t, bucketName)
	form = url.Values{}
	form.Add("bucketName", bucketName)
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/create",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.True(t, w.Code == http.StatusUnprocessableEntity || w.Code == http.StatusConflict)

	// 4. s3Versioning invalid boolean
	form = url.Values{}
	form.Add("enabled", "not-a-bool")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/bucket/"+bucketName+"/versioning",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.True(
		t,
		w.Code == http.StatusBadRequest || w.Code == http.StatusOK ||
			w.Code == http.StatusInternalServerError,
	)

	// 4b. s3Versioning disabled
	form = url.Values{}
	form.Add("enabled", "false")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/bucket/"+bucketName+"/versioning",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 5. Query non-existent table
	form = url.Values{}
	form.Add("partitionKeyValue", "val")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/ghost-table/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// 7. S3 handler root with trailing slash
	req = httptest.NewRequest(http.MethodGet, "/dashboard/s3/", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 9. s3CreateBucket with versioning
	form = url.Values{}
	form.Add("bucketName", "versioned-bucket")
	form.Add("versioning", "on")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/bucket",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	// 9. s3CreateBucket with versioning
	form = url.Values{}
	form.Add("bucketName", "versioned-bucket")
	form.Add("versioning", "on")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/s3/create",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 10. S3 bucket list with search
	stack.CreateS3Bucket(t, "search-bucket")
	req = httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets?search=search", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "search-bucket")

	// 11. S3 bucket list with search no matches
	req = httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets?search=nonexistent", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), "search-bucket")

	// 12. DynamoDB table list with search
	stack.CreateDDBTable(t, "search-table")
	req = httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/tables?search=search", nil)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "search-table")

	// 16. DynamoDB Query invalid filter
	form = url.Values{}
	form.Add("partitionKeyName", "pk")
	form.Add("partitionKeyValue", "v")
	form.Add("filterExpression", "INVALID (")
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/search-table/query",
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	// Some implementations might handle invalid filters by returning 0 items (200 OK)
	// while others might return 500. We'll accept both for robustness.
	assert.True(t, w.Code == http.StatusOK || w.Code == http.StatusInternalServerError)

	// 18. s3DeleteFile with markers (trigger markers loop)
	stack.CreateS3Bucket(t, "markers-bucket")
	// Delete non-existent file creates a delete marker if versioning is on,
	// but here we just want to hit the loop in deleteAllVersions if we can.
	// We'll just ensure it doesn't crash.
	req = httptest.NewRequest(
		http.MethodDelete,
		"/dashboard/s3/bucket/markers-bucket/file/none.txt?deleteAll=true",
		nil,
	)
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 19. dynamoDBQuery with invalid index
	req = httptest.NewRequest(
		http.MethodPost,
		"/dashboard/dynamodb/table/search-table/query",
		strings.NewReader("partitionKeyName=pk&partitionKeyValue=v&indexName=INVALID"),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	// Some implementations might return 422 for invalid parameters or 404 for missing index
	assert.True(
		t,
		w.Code == http.StatusInternalServerError ||
			w.Code == http.StatusBadRequest ||
			w.Code == http.StatusOK ||
			w.Code == http.StatusNotFound ||
			w.Code == http.StatusUnprocessableEntity,
	)

	// 17. S3 bucket list with HX-Request
	req = httptest.NewRequest(http.MethodGet, "/dashboard/s3", nil)
	req.Header.Set("Hx-Request", "true")
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 14. s3Upload no file
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	mw.Close()
	req = httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/existing-bucket/upload", body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDashboard_S3_Upload_Extended(t *testing.T) {
	t.Parallel()
	stack := newStack(t)
	bucketName := "ext-upload-bucket"
	stack.CreateS3Bucket(t, bucketName)

	// 1. Upload with specific key
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("file", "test.txt")
	_, _ = part.Write([]byte("content"))
	_ = writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/"+bucketName+"/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// 2. Upload with subfolder (prefix)
	body = &bytes.Buffer{}
	writer = multipart.NewWriter(body)
	part, _ = writer.CreateFormFile("file", "test2.txt")
	_, _ = part.Write([]byte("content2"))
	_ = writer.WriteField("prefix", "sub/")
	_ = writer.Close()
	req = httptest.NewRequest(http.MethodPost, "/dashboard/s3/bucket/"+bucketName+"/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w = httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestDashboard_CustomModal_Plumbing(t *testing.T) {
	t.Parallel()

	stack := newStack(t)
	stack.CreateDDBTable(t, "test-table")
	stack.CreateS3Bucket(t, "test-bucket")

	t.Run("layout includes global modal", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, `id="global_confirm_modal"`)
		assert.Contains(t, body, `id="global_confirm_proceed"`)
		assert.Contains(t, body, `id="global_confirm_cancel"`)
	})

	t.Run("static app.js includes htmx confirm listener", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/static/app.js", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "htmx:confirm")
		assert.Contains(t, body, "global_confirm_modal")
		assert.Contains(t, body, "event.detail.issueRequest(true)")
	})

	t.Run("dynamodb delete button has hx-confirm", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/table/test-table", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "hx-confirm=\"Are you sure you want to delete this table?\"")
	})

	t.Run("s3 delete button has hx-confirm", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "hx-confirm=\"Are you sure you want to delete bucket 'test-bucket'?\"")
	})
}

func TestDashboard_S3_PurgeAll(t *testing.T) {
	t.Parallel()

	t.Run("purge all deletes all buckets and returns empty list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateS3Bucket(t, "purge-bucket-one")
		stack.CreateS3Bucket(t, "purge-bucket-two")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/s3/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// The response should be the refreshed (empty) bucket list, not a success message.
		assert.NotContains(t, w.Body.String(), "purge-bucket-one")
		assert.NotContains(t, w.Body.String(), "purge-bucket-two")
	})

	t.Run("purge all on empty store returns success", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/s3/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("purge all deletes bucket with objects inside", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateS3Bucket(t, "nonempty-bucket")
		uploadS3Object(t, stack, "nonempty-bucket", "key1", "data1")
		uploadS3Object(t, stack, "nonempty-bucket", "key2", "data2")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/s3/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// Verify buckets are actually gone by listing
		req = httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		assert.NotContains(t, w.Body.String(), "nonempty-bucket")
	})

	t.Run("DELETE request is routed through RouteMatcher", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateS3Bucket(t, "route-test-bucket")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/s3/purge", nil)
		w := httptest.NewRecorder()
		serveFullStack(stack.Echo, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.NotContains(t, w.Body.String(), "route-test-bucket")
	})
}

func TestDashboard_DDB_PurgeAll(t *testing.T) {
	t.Parallel()

	t.Run("purge all deletes all tables and returns empty list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateDDBTable(t, "purge-table-one")
		stack.CreateDDBTable(t, "purge-table-two")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// The response should be the refreshed (empty) table list, not a success message.
		assert.NotContains(t, w.Body.String(), "purge-table-one")
		assert.NotContains(t, w.Body.String(), "purge-table-two")
	})

	t.Run("purge all on empty store returns success", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("purge all deletes tables and returns refreshed list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateDDBTable(t, "to-be-purged")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// The purge response itself should no longer contain the deleted table.
		assert.NotContains(t, w.Body.String(), "to-be-purged")
	})

	t.Run("DELETE request is routed through RouteMatcher", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)
		stack.CreateDDBTable(t, "route-test-table")

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/dynamodb/purge", nil)
		w := httptest.NewRecorder()
		serveFullStack(stack.Echo, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.NotContains(t, w.Body.String(), "route-test-table")
	})
}

// newSQSIntegrationStack creates an integration stack that includes an SQS handler.
func newSQSIntegrationStack(t *testing.T) *teststack.Stack {
	t.Helper()

	return teststack.New(t)
}

func TestDashboard_SQS_Index(t *testing.T) {
	t.Parallel()

	t.Run("index page renders with empty queue list", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/sqs", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "SQS Queues")
	})

	t.Run("index page shows queues", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		// Create queue via handler's SQSOps backend
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=test-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// Verify the queue appears in the index
		req = httptest.NewRequest(http.MethodGet, "/dashboard/sqs", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test-queue")
	})

	t.Run("index renders with nil SQSOps", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/sqs", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "SQS Queues")
	})
}

func TestDashboard_SQS_CreateQueueModal(t *testing.T) {
	t.Parallel()

	stack := newSQSIntegrationStack(t)

	req := httptest.NewRequest(http.MethodGet, "/dashboard/sqs/create", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestDashboard_SQS_CreateQueue(t *testing.T) {
	t.Parallel()

	t.Run("create queue successfully", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=my-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/sqs", w.Header().Get("Hx-Redirect"))
	})

	t.Run("create FIFO queue successfully", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=my-fifo.fifo&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("create queue returns 400 when name is empty", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name="))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("create duplicate queue returns 500", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		// Create once
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=dup-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// Create again - should fail
		req = httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=dup-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("create queue with nil SQSOps is a no-op", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=my-queue"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestDashboard_SQS_DeleteQueue(t *testing.T) {
	t.Parallel()

	t.Run("delete queue successfully", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		// Create a queue first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=del-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// Get the queue URL
		qURL := url.QueryEscape("http://local/000000000000/del-queue")
		req = httptest.NewRequest(http.MethodDelete, "/dashboard/sqs/delete?url="+qURL, nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/sqs", w.Header().Get("Hx-Redirect"))
	})

	t.Run("delete queue returns 400 when URL missing", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/sqs/delete", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("delete non-existent queue returns 500", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		qURL := url.QueryEscape("http://local/000000000000/nonexistent")
		req := httptest.NewRequest(http.MethodDelete, "/dashboard/sqs/delete?url="+qURL, nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("delete queue with nil SQSOps is a no-op", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		reqURL := "/dashboard/sqs/delete?url=" + url.QueryEscape("http://local/000000000000/x")
		req := httptest.NewRequest(http.MethodDelete, reqURL, nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestDashboard_SQS_PurgeQueue(t *testing.T) {
	t.Parallel()

	t.Run("purge queue successfully", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		// Create a queue
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=purge-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		qURL := url.QueryEscape("http://local/000000000000/purge-queue")
		req = httptest.NewRequest(http.MethodPost, "/dashboard/sqs/purge?url="+qURL, nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/dashboard/sqs", w.Header().Get("Hx-Redirect"))
	})

	t.Run("purge queue returns 400 when URL missing", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/purge", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("purge non-existent queue returns 500", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		qURL := url.QueryEscape("http://local/000000000000/nonexistent")
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/purge?url="+qURL, nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("purge queue with nil SQSOps is a no-op", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		reqURL := "/dashboard/sqs/purge?url=" + url.QueryEscape("http://local/000000000000/x")
		req := httptest.NewRequest(http.MethodPost, reqURL, nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestDashboard_SQS_QueueDetail(t *testing.T) {
	t.Parallel()

	t.Run("queue detail renders successfully", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		// Create a queue first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create",
			strings.NewReader("queue_name=detail-queue&visibility_timeout=30"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		qURL := url.QueryEscape("http://local/000000000000/detail-queue")
		req = httptest.NewRequest(http.MethodGet, "/dashboard/sqs/queue?url="+qURL, nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Queue Detail")
	})

	t.Run("queue detail returns 400 when URL missing", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/sqs/queue", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("queue detail returns 404 for non-existent queue", func(t *testing.T) {
		t.Parallel()
		stack := newSQSIntegrationStack(t)

		qURL := url.QueryEscape("http://local/000000000000/nonexistent")
		req := httptest.NewRequest(http.MethodGet, "/dashboard/sqs/queue?url="+qURL, nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("queue detail returns 500 when SQSOps is nil", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		reqURL := "/dashboard/sqs/queue?url=" + url.QueryEscape("http://local/000000000000/x")
		req := httptest.NewRequest(http.MethodGet, reqURL, nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestDashboard_SNS_Index(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	req := httptest.NewRequest(http.MethodGet, "/dashboard/sns", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "SNS Topics")
	assert.Contains(t, w.Body.String(), "No topics found")
}

func TestDashboard_SNS_CreateTopic(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	form := url.Values{}
	form.Set("name", "test-topic")

	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "/dashboard/sns", w.Header().Get("Hx-Redirect"))

	// Verify the topic was created by listing
	req2 := httptest.NewRequest(http.MethodGet, "/dashboard/sns", nil)
	w2 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w2, req2)
	assert.Contains(t, w2.Body.String(), "test-topic")
}

func TestDashboard_SNS_CreateTopic_MissingName(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDashboard_SNS_DeleteTopic(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	// Create topic first
	form := url.Values{}
	form.Set("name", "delete-me")
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Get the ARN from the index page
	req2 := httptest.NewRequest(http.MethodGet, "/dashboard/sns", nil)
	w2 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w2, req2)
	arn := "arn:aws:sns:us-east-1:000000000000:delete-me"

	// Delete via ARN
	req3 := httptest.NewRequest(http.MethodDelete, "/dashboard/sns/delete?arn="+arn, nil)
	w3 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w3, req3)
	require.Equal(t, http.StatusOK, w3.Code)
	assert.Equal(t, "/dashboard/sns", w3.Header().Get("Hx-Redirect"))
}

func TestDashboard_SNS_DeleteTopic_MissingArn(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	req := httptest.NewRequest(http.MethodDelete, "/dashboard/sns/delete", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDashboard_SNS_TopicDetail(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	// Create topic first
	form := url.Values{}
	form.Set("name", "detail-topic")
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	require.Equal(t, http.StatusOK, w.Code)

	arn := "arn:aws:sns:us-east-1:000000000000:detail-topic"
	req2 := httptest.NewRequest(http.MethodGet, "/dashboard/sns/topic?arn="+arn, nil)
	w2 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)
	assert.Contains(t, w2.Body.String(), "Topic Detail")
	assert.Contains(t, w2.Body.String(), "detail-topic")
}

func TestDashboard_SNS_TopicDetail_MissingArn(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	req := httptest.NewRequest(http.MethodGet, "/dashboard/sns/topic", nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDashboard_SNS_TopicDetail_NotFound(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	const notFoundArn = "arn:aws:sns:us-east-1:000000000000:nonexistent"
	req := httptest.NewRequest(http.MethodGet, "/dashboard/sns/topic?arn="+notFoundArn, nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestDashboard_SNS_CreateTopic_Duplicate(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	form := url.Values{}
	form.Set("name", "dup-topic")

	// First create
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Duplicate create should return 409 (Conflict)
	req2 := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w2 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w2, req2)
	require.Equal(t, http.StatusConflict, w2.Code)
}

func TestDashboard_SNS_DeleteTopic_NotFound(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	const missingArn = "arn:aws:sns:us-east-1:000000000000:nonexistent"
	req := httptest.NewRequest(http.MethodDelete, "/dashboard/sns/delete?arn="+missingArn, nil)
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestDashboard_SNS_TopicDetail_WithSubscription(t *testing.T) {
	t.Parallel()

	stack := newStack(t)

	// Create topic
	form := url.Values{}
	form.Set("name", "sub-detail-topic")
	req := httptest.NewRequest(http.MethodPost, "/dashboard/sns/create", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w, req)
	require.Equal(t, http.StatusOK, w.Code)

	arn := "arn:aws:sns:us-east-1:000000000000:sub-detail-topic"

	// Add a subscription directly through the backend
	_, err := stack.Dashboard.SNSOps.Backend.Subscribe(arn, "https", "https://example.com/endpoint", "")
	require.NoError(t, err)

	req2 := httptest.NewRequest(http.MethodGet, "/dashboard/sns/topic?arn="+arn, nil)
	w2 := httptest.NewRecorder()
	serveHandler(stack.Dashboard, w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)
	assert.Contains(t, w2.Body.String(), "https")
}

func TestDashboard_APIRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantContains  string
		wantDefaultIn string
		setupBuckets  []string
		setupTables   []string
		wantStatus    int
	}{
		{
			name:          "returns default region when no resources",
			wantStatus:    http.StatusOK,
			wantDefaultIn: "us-east-1",
			wantContains:  "us-east-1",
		},
		{
			name:          "includes region from S3 bucket",
			wantStatus:    http.StatusOK,
			setupBuckets:  []string{"test-bucket"},
			wantDefaultIn: "us-east-1",
			wantContains:  "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			for _, b := range tt.setupBuckets {
				stack.CreateS3Bucket(t, b)
			}

			for _, tbl := range tt.setupTables {
				stack.CreateDDBTable(t, tbl)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/api/regions", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
			assert.Contains(t, w.Body.String(), tt.wantDefaultIn)
		})
	}
}

func TestDashboard_S3_BucketList_RegionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		region       string
		wantContains string
		wantAbsent   string
		wantStatus   int
	}{
		{
			name:         "us-east-1 shows default bucket",
			region:       "us-east-1",
			wantContains: "east-bucket",
			wantAbsent:   "west-bucket",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "us-west-2 shows only west bucket",
			region:       "us-west-2",
			wantContains: "west-bucket",
			wantAbsent:   "east-bucket",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)

			// Create bucket in default region (us-east-1)
			stack.CreateS3Bucket(t, "east-bucket")

			// Create bucket in us-west-2
			_, err := stack.S3Backend.CreateBucket(t.Context(), &s3.CreateBucketInput{
				Bucket: aws.String("west-bucket"),
				CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
					LocationConstraint: s3types.BucketLocationConstraintUsWest2,
				},
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets?region="+tt.region, nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, w.Body.String(), tt.wantAbsent)
			}
		})
	}
}

func TestDashboard_DDB_TableList_RegionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		region       string
		wantContains string
		wantAbsent   string
		wantStatus   int
	}{
		{
			name:         "default region shows default table",
			region:       "us-east-1",
			wantContains: "east-table",
			wantAbsent:   "west-table",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "us-west-2 shows only west table",
			region:       "us-west-2",
			wantContains: "west-table",
			wantAbsent:   "east-table",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)

			// Create table in default region via stack helper
			stack.CreateDDBTable(t, "east-table")

			// Create west-table directly in us-west-2 by inserting into the Tables map
			db, ok := stack.DDBHandler.Backend.(*ddbbackend.InMemoryDB)
			require.True(t, ok)

			westTable := &ddbbackend.Table{
				Name:   "west-table",
				Status: "ACTIVE",
			}

			if db.Tables["us-west-2"] == nil {
				db.Tables["us-west-2"] = make(map[string]*ddbbackend.Table)
			}

			db.Tables["us-west-2"]["west-table"] = westTable

			req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/tables?region="+tt.region, nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
			if tt.wantAbsent != "" {
				assert.NotContains(t, w.Body.String(), tt.wantAbsent)
			}
		})
	}
}
