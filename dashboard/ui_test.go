package dashboard_test

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"Gopherstack/dashboard"

	ddbbackend "Gopherstack/dynamodb"
	s3backend "Gopherstack/s3"
)

// integrationStack holds the fully wired in-memory test stack.
type integrationStack struct {
	handler    *dashboard.Handler
	s3Backend  *s3backend.InMemoryBackend
	ddbHandler *ddbbackend.Handler
}

func newIntegrationStack(t *testing.T) *integrationStack {
	t.Helper()

	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3Hndlr := s3backend.NewHandler(s3Bk)
	ddbHndlr := ddbbackend.NewHandler()

	apiMux := http.NewServeMux()
	apiMux.Handle("/s3", http.StripPrefix("/s3", s3Hndlr))
	apiMux.Handle("/s3/", http.StripPrefix("/s3", s3Hndlr))
	apiMux.Handle("/", ddbHndlr)

	inMemClient := &dashboard.InMemClient{Handler: apiMux}

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
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

	h := dashboard.NewHandler(ddbClient, s3Client, ddbHndlr, s3Hndlr)

	return &integrationStack{
		handler:    h,
		s3Backend:  s3Bk,
		ddbHandler: ddbHndlr,
	}
}

func newDDBTable(t *testing.T, stack *integrationStack, tableName string) {
	t.Helper()

	_, err := stack.ddbHandler.DB.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
}

func newS3Bucket(t *testing.T, stack *integrationStack, bucketName string) {
	t.Helper()

	_, err := stack.s3Backend.CreateBucket(
		t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	require.NoError(t, err)
}

func uploadS3Object(t *testing.T, stack *integrationStack, bucket, key, content string) {
	t.Helper()

	_, err := stack.s3Backend.PutObject(t.Context(), &s3.PutObjectInput{
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
			name:         "root redirects to dynamodb",
			method:       http.MethodGet,
			path:         "/dashboard/",
			wantStatus:   http.StatusFound,
			wantLocation: "/dashboard/dynamodb",
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
	}

	stack := newIntegrationStack(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

func TestDashboard_DynamoDB_TableList(t *testing.T) {
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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newDDBTable(t, stack, "list-table")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/tables", nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DynamoDB_CreateTable_Integration(t *testing.T) {
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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newDDBTable(t, stack, "existing-table")
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
			stack.handler.ServeHTTP(w, req)

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

func TestDashboard_DynamoDB_TableDetail(t *testing.T) {
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

			stack := newIntegrationStack(t)
			newDDBTable(t, stack, "detail-table")

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/dynamodb/table/"+tt.tableName, nil,
			)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DynamoDB_DeleteTable(t *testing.T) {
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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newDDBTable(t, stack, "del-table")
			}

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/dynamodb/table/del-table", nil,
			)
			if tt.hxTarget != "" {
				req.Header.Set("Hx-Target", tt.hxTarget)
			}

			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantHxLocation != "" {
				assert.Equal(t, tt.wantHxLocation, w.Header().Get("Hx-Location"))
			}
		})
	}
}

func TestDashboard_DynamoDB_Query(t *testing.T) {
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

			stack := newIntegrationStack(t)
			newDDBTable(t, stack, "query-table")

			_, err := stack.ddbHandler.DB.PutItem(&dynamodb.PutItemInput{
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
			stack.handler.ServeHTTP(w, req)

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

func TestDashboard_DynamoDB_Scan(t *testing.T) {
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

			stack := newIntegrationStack(t)
			newDDBTable(t, stack, "scan-table")

			if tt.preInsert {
				_, err := stack.ddbHandler.DB.PutItem(&dynamodb.PutItemInput{
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
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantContains != "" {
				assert.Contains(t, w.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestDashboard_DynamoDB_Search(t *testing.T) {
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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newDDBTable(t, stack, "search-table")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/dynamodb/search", nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newS3Bucket(t, stack, "list-bucket")
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/s3/buckets", nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newS3Bucket(t, stack, "dup-bucket")
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
			stack.handler.ServeHTTP(w, req)

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "detail-bucket")

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/s3/bucket/"+tt.bucketName, nil,
			)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "tree-bucket")
			if tt.preUpload {
				uploadS3Object(t, stack, "tree-bucket", "hello.txt", "hello")
			}

			req := httptest.NewRequest(
				http.MethodGet, "/dashboard/s3/bucket/tree-bucket/tree", nil,
			)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "fd-bucket")
			uploadS3Object(t, stack, "fd-bucket", "myfile.txt", "file content")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "dl-bucket")
			uploadS3Object(t, stack, "dl-bucket", "get.txt", "file content")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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
			wantStatus:    http.StatusOK,
			wantHxTrigger: "fileUploaded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "up-bucket")

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
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
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

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "delf-bucket")
			uploadS3Object(t, stack, "delf-bucket", "file.txt", "content")

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/s3/bucket/delf-bucket/file/file.txt", nil,
			)
			if tt.hxTarget != "" {
				req.Header.Set("Hx-Target", tt.hxTarget)
			}

			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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

			stack := newIntegrationStack(t)
			if tt.preCreate {
				newS3Bucket(t, stack, "del-bucket")
			}

			req := httptest.NewRequest(
				http.MethodDelete, "/dashboard/s3/bucket/del-bucket", nil,
			)
			w := httptest.NewRecorder()
			stack.handler.ServeHTTP(w, req)

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
			method: http.MethodPut,
			formValues: url.Values{
				"enabled": {"true"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "disable versioning returns bucket list",
			method: http.MethodPut,
			formValues: url.Values{
				"enabled": {"false"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newIntegrationStack(t)
			newS3Bucket(t, stack, "ver-bucket")

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
			stack.handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}
