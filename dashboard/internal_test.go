package dashboard

import (
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

func TestInternal_ToAttributeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expectedValue types.AttributeValue
		name          string
		inputValue    string
		inputType     types.ScalarAttributeType
	}{
		{
			name:          "number attribute",
			inputValue:    "100",
			inputType:     types.ScalarAttributeTypeN,
			expectedValue: &types.AttributeValueMemberN{Value: "100"},
		},
		{
			name:          "binary attribute",
			inputValue:    "Hello",
			inputType:     types.ScalarAttributeTypeB,
			expectedValue: &types.AttributeValueMemberB{Value: []byte("Hello")},
		},
		{
			name:          "string attribute",
			inputValue:    "World",
			inputType:     types.ScalarAttributeTypeS,
			expectedValue: &types.AttributeValueMemberS{Value: "World"},
		},
		{
			name:          "unknown type defaults to string",
			inputValue:    "Default",
			inputType:     "UNKNOWN",
			expectedValue: &types.AttributeValueMemberS{Value: "Default"},
		},
		{
			name:          "float number",
			inputValue:    "10.5",
			inputType:     types.ScalarAttributeTypeN,
			expectedValue: &types.AttributeValueMemberN{Value: "10.5"},
		},
		{
			name:          "binary data",
			inputValue:    "data",
			inputType:     types.ScalarAttributeTypeB,
			expectedValue: &types.AttributeValueMemberB{Value: []byte("data")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}

			val := h.toAttributeValue(tt.inputValue, tt.inputType)
			require.True(t, reflect.DeepEqual(tt.expectedValue, val),
				"expected %#v, got %#v", tt.expectedValue, val)
		})
	}
}

func TestInternal_RenderTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupLayout  func() *template.Template
		templateName string
		expectCode   int
	}{
		{
			name: "render with error - bad field",
			setupLayout: func() *template.Template {
				return template.Must(template.New("test").Parse("{{.BadField}}"))
			},
			templateName: "test",
			expectCode:   http.StatusInternalServerError,
		},
		{
			name: "render success with valid template",
			setupLayout: func() *template.Template {
				tmpl := template.New("layout.html")
				tmpl.Parse(`{{define "layout.html"}}<html>{{template "doc.html" .}}</html>{{end}}`)
				tmpl.Parse(`{{define "doc.html"}}content{{end}}`)

				return tmpl
			},
			templateName: "doc.html",
			expectCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{
				layout: tt.setupLayout(),
				Logger: slog.Default(),
			}

			w := httptest.NewRecorder()
			h.renderTemplate(w, tt.templateName, nil)
			require.Equal(t, tt.expectCode, w.Code)
		})
	}
}

func TestInternal_RenderFragment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLayout func() *template.Template
		name        string
		expectCode  int
	}{
		{
			name: "render error - bad field",
			setupLayout: func() *template.Template {
				return template.Must(template.New("test").Parse("{{.BadField}}"))
			},
			expectCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{
				layout: tt.setupLayout(),
				Logger: slog.Default(),
			}

			w := httptest.NewRecorder()
			h.renderFragment(w, "test", struct{}{})
			require.Equal(t, tt.expectCode, w.Code)
		})
	}
}

func TestInternal_ExtractKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expectPK string
		expectSK string
		schema   []types.KeySchemaElement
	}{
		{
			name: "with partition and sort keys",
			schema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			expectPK: "pk",
			expectSK: "sk",
		},
		{
			name:     "with empty schema",
			schema:   nil,
			expectPK: "",
			expectSK: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}
			pk, sk := h.extractKeys(tt.schema)
			require.Equal(t, tt.expectPK, pk)
			require.Equal(t, tt.expectSK, sk)
		})
	}
}

func TestInternal_ResolveKeySchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		desc         *dynamodb.DescribeTableOutput
		indexName    string
		expectPK     string
		expectSK     string
		expectPKType types.ScalarAttributeType
		expectSKType types.ScalarAttributeType
	}{
		{
			name: "table with partition and sort keys",
			desc: &dynamodb.DescribeTableOutput{
				Table: &types.TableDescription{
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
					},
					AttributeDefinitions: []types.AttributeDefinition{
						{
							AttributeName: aws.String("pk"),
							AttributeType: types.ScalarAttributeTypeS,
						},
						{
							AttributeName: aws.String("sk"),
							AttributeType: types.ScalarAttributeTypeN,
						},
					},
				},
			},
			indexName:    "",
			expectPK:     "pk",
			expectSK:     "sk",
			expectPKType: types.ScalarAttributeTypeS,
			expectSKType: types.ScalarAttributeTypeN,
		},
		{
			name: "table with no keys",
			desc: &dynamodb.DescribeTableOutput{
				Table: &types.TableDescription{
					TableName: aws.String("table"),
				},
			},
			indexName:    "non-existent",
			expectPK:     "",
			expectSK:     "",
			expectPKType: "",
			expectSKType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}
			pk, sk, pkt, skt := h.resolveKeySchema(tt.desc, tt.indexName)
			require.Equal(t, tt.expectPK, pk)
			require.Equal(t, tt.expectSK, sk)
			require.Equal(t, tt.expectPKType, pkt)
			require.Equal(t, tt.expectSKType, skt)
		})
	}
}

func TestInternal_ExtractIndexInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		indexName       *string
		proj            *types.Projection
		name            string
		expectIndexName string
		expectPK        string
		expectPKType    string
		expectSK        string
		expectSKType    string
		expectProjType  string
		schema          []types.KeySchemaElement
		attrs           []types.AttributeDefinition
	}{
		{
			name:      "LSI with projection",
			indexName: aws.String("my-lsi"),
			schema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			proj: &types.Projection{
				ProjectionType:   types.ProjectionTypeInclude,
				NonKeyAttributes: []string{"attr1"},
			},
			attrs: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			},
			expectIndexName: "my-lsi",
			expectPK:        "pk",
			expectPKType:    "S",
			expectSK:        "sk",
			expectSKType:    "N",
			expectProjType:  "INCLUDE",
		},
		{
			name:            "nil projection defaults",
			indexName:       aws.String("idx"),
			schema:          nil,
			proj:            nil,
			attrs:           nil,
			expectIndexName: "idx",
			expectProjType:  "INCLUDE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}
			info := h.extractIndexInfo(tt.indexName, tt.schema, tt.proj, tt.attrs)
			require.Equal(t, tt.expectIndexName, info.IndexName)
			require.Equal(t, tt.expectPK, info.PartitionKey)
			require.Equal(t, tt.expectPKType, info.PartitionKeyType)
			require.Equal(t, tt.expectSK, info.SortKey)
			require.Equal(t, tt.expectSKType, info.SortKeyType)
			require.Equal(t, tt.expectProjType, info.ProjectionType)
		})
	}
}

func TestInternal_ExtractTableInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		desc           *types.TableDescription
		expectName     string
		expectPK       string
		expectSK       string
		expectItemCnt  int64
		expectGSICount int
		expectLSICount int
	}{
		{
			name: "table with sort key",
			desc: &types.TableDescription{
				TableName: aws.String("full-table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
				},
				ItemCount: aws.Int64(5),
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("idx1"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gpk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
			},
			expectName:     "full-table",
			expectPK:       "pk",
			expectSK:       "sk",
			expectItemCnt:  5,
			expectGSICount: 1,
			expectLSICount: 0,
		},
		{
			name: "table with GSI and LSI",
			desc: &types.TableDescription{
				TableName: aws.String("table"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("gsi"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gp"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				LocalSecondaryIndexes: []types.LocalSecondaryIndexDescription{
					{
						IndexName: aws.String("lsi"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
							{AttributeName: aws.String("ls"), KeyType: types.KeyTypeRange},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
					},
				},
			},
			expectName:     "table",
			expectPK:       "pk",
			expectGSICount: 1,
			expectLSICount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}
			info := h.extractTableInfo(tt.desc)
			require.Equal(t, tt.expectName, info.TableName)
			require.Equal(t, tt.expectPK, info.PartitionKey)
			if tt.expectSK != "" {
				require.Equal(t, tt.expectSK, info.SortKey)
			}
			require.Equal(t, tt.expectGSICount, info.GSICount)
			require.Equal(t, tt.expectLSICount, info.LSICount)
		})
	}
}

func TestInternal_ResolveIndexKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		desc      *dynamodb.DescribeTableOutput
		indexName string
		expectPK  string
		expectSK  string
	}{
		{
			name: "resolve index keys success",
			desc: &dynamodb.DescribeTableOutput{
				Table: &types.TableDescription{
					GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
						{
							IndexName: aws.String("idx1"),
							KeySchema: []types.KeySchemaElement{
								{AttributeName: aws.String("gpk"), KeyType: types.KeyTypeHash},
								{AttributeName: aws.String("gsk"), KeyType: types.KeyTypeRange},
							},
						},
					},
				},
			},
			indexName: "idx1",
			expectPK:  "gpk",
			expectSK:  "gsk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{}
			pk, sk := h.resolveIndexKeys(tt.desc, tt.indexName)
			require.Equal(t, tt.expectPK, pk)
			require.Equal(t, tt.expectSK, sk)
		})
	}
}

func TestInternal_RenderQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		expectContent string
		result        QueryResult
		expectCode    int
	}{
		{
			name: "render with pagination",
			result: QueryResult{
				Items: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "val1"}},
				},
				LastEvaluatedKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "val1"},
				},
				Count: 1,
			},
			expectCode:    http.StatusOK,
			expectContent: "val1",
		},
		{
			name: "render full results",
			result: QueryResult{
				Items: []map[string]types.AttributeValue{
					{
						"pk":   &types.AttributeValueMemberS{Value: "v1"},
						"sk":   &types.AttributeValueMemberN{Value: "100"},
						"bin":  &types.AttributeValueMemberB{Value: []byte("raw")},
						"bool": &types.AttributeValueMemberBOOL{Value: true},
						"null": &types.AttributeValueMemberNULL{Value: true},
					},
				},
				Count:        1,
				ScannedCount: 2,
			},
			expectCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{Logger: slog.Default()}
			w := httptest.NewRecorder()
			h.renderQueryResults(w, tt.result, "test-table", "", "")
			require.Equal(t, tt.expectCode, w.Code)
			if tt.expectContent != "" {
				require.Contains(t, w.Body.String(), tt.expectContent)
			}
		})
	}
}

func TestInternal_ParseQueryRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formData        url.Values
		name            string
		expectIndexName string
		expectSKVal2    string
		expectOK        bool
	}{
		{
			name:     "empty request",
			formData: url.Values{},
			expectOK: false,
		},
		{
			name: "full request",
			formData: func() url.Values {
				form := url.Values{}
				form.Add("indexName", "idx")
				form.Add("partitionKeyName", "pk")
				form.Add("partitionKeyValue", "v")
				form.Add("sortKeyOperator", "=")
				form.Add("sortKeyValue", "sv1")
				form.Add("sortKeyValue2", "sv2")
				form.Add("filterExpression", "f")
				form.Add("limit", "10")

				return form
			}(),
			expectOK:        true,
			expectIndexName: "idx",
			expectSKVal2:    "sv2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			h := &DashboardHandler{Logger: slog.Default()}
			req := httptest.NewRequest(
				http.MethodPost,
				"/test",
				strings.NewReader(tt.formData.Encode()),
			)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			params, ok := h.parseQueryRequest(w, req)
			require.Equal(t, tt.expectOK, ok)
			if tt.expectOK {
				require.Equal(t, tt.expectIndexName, params.IndexName)
				require.Equal(t, tt.expectSKVal2, params.SortKeyValue2)
			}
		})
	}
}

func TestInMemClient_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		path             string
		expectedBodyText string
		expectedStatus   int
	}{
		{
			name:             "successful request",
			path:             "/test",
			expectedStatus:   http.StatusOK,
			expectedBodyText: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			_ = ctx
			var capturedHeader string

			mux := http.NewServeMux()
			mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
				capturedHeader = r.Header.Get(chaos.HeaderDashboard)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok"))
			})
			c := &InMemClient{Handler: mux}
			req := httptest.NewRequest(http.MethodGet, "http://localhost"+tt.path, nil)
			resp, err := c.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, tt.expectedStatus, resp.StatusCode)
			assert.Equal(t, "true", capturedHeader, "expected dashboard bypass header to be set")

			if tt.expectedBodyText != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, tt.expectedBodyText, string(body))
			}
		})
	}
}

func TestInMemClient_Do(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		path           string
		wantBodyText   string
		wantDashHeader string
		wantStatus     int
	}{
		{
			name:           "sets dashboard bypass header",
			path:           "/test",
			wantStatus:     http.StatusOK,
			wantBodyText:   "ok",
			wantDashHeader: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var capturedHeader string

			mux := http.NewServeMux()
			mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
				capturedHeader = r.Header.Get(chaos.HeaderDashboard)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok"))
			})

			c := &InMemClient{Handler: mux}
			req := httptest.NewRequest(http.MethodGet, "http://localhost"+tt.path, nil)
			resp, err := c.Do(req)
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, tt.wantDashHeader, capturedHeader, "expected dashboard bypass header to be set")

			if tt.wantBodyText != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, tt.wantBodyText, string(body))
			}
		})
	}
}
