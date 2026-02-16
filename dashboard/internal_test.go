package dashboard

import (
	"html/template"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternal_ToAttributeValue(t *testing.T) {
	t.Parallel()
	h := &Handler{}

	val := h.toAttributeValue("100", types.ScalarAttributeTypeN)
	assert.Equal(t, &types.AttributeValueMemberN{Value: "100"}, val)

	val = h.toAttributeValue("Hello", types.ScalarAttributeTypeB)
	assert.Equal(t, &types.AttributeValueMemberB{Value: []byte("Hello")}, val)

	val = h.toAttributeValue("World", types.ScalarAttributeTypeS)
	assert.Equal(t, &types.AttributeValueMemberS{Value: "World"}, val)

	// Test default case
	val = h.toAttributeValue("Default", "UNKNOWN")
	assert.Equal(t, &types.AttributeValueMemberS{Value: "Default"}, val)
}

func TestInternal_RenderTemplate_Error(t *testing.T) {
	t.Parallel()
	tmpl := template.Must(template.New("test").Parse("{{.BadField}}"))
	h := &Handler{
		layout: tmpl,
		Logger: slog.Default(),
	}

	w := httptest.NewRecorder()
	h.renderTemplate(w, "test", struct{}{})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestInternal_RenderFragment_Error(t *testing.T) {
	t.Parallel()
	tmpl := template.Must(template.New("test").Parse("{{.BadField}}"))
	h := &Handler{
		layout: tmpl,
		Logger: slog.Default(),
	}

	w := httptest.NewRecorder()
	h.renderFragment(w, "test", struct{}{})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestInternal_ExtractKeys(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	schema := []types.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
	}
	pk, sk := h.extractKeys(schema)
	assert.Equal(t, "pk", pk)
	assert.Equal(t, "sk", sk)
}

func TestInternal_ResolveKeySchema_Table(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	desc := &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			},
		},
	}
	pk, sk, pkt, skt := h.resolveKeySchema(desc, "")
	assert.Equal(t, "pk", pk)
	assert.Equal(t, "sk", sk)
	assert.Equal(t, types.ScalarAttributeTypeS, pkt)
	assert.Equal(t, types.ScalarAttributeTypeN, skt)
}

func TestInternal_ExtractIndexInfo_LSI(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	name := aws.String("my-lsi")
	schema := []types.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
	}
	proj := &types.Projection{
		ProjectionType:   types.ProjectionTypeInclude,
		NonKeyAttributes: []string{"attr1"},
	}
	attrs := []types.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
	}
	info := h.extractIndexInfo(name, schema, proj, attrs)
	assert.Equal(t, "my-lsi", info.IndexName)
	assert.Equal(t, "pk", info.PartitionKey)
	assert.Equal(t, "S", info.PartitionKeyType)
	assert.Equal(t, "sk", info.SortKey)
	assert.Equal(t, "N", info.SortKeyType)
	assert.Equal(t, "INCLUDE", info.ProjectionType)
}

func TestInternal_ExtractTableInfo_WithSK(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	desc := &types.TableDescription{
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
	}
	info := h.extractTableInfo(desc)
	assert.Equal(t, "full-table", info.TableName)
	assert.Equal(t, "pk", info.PartitionKey)
	assert.Equal(t, "sk", info.SortKey)
	assert.Equal(t, 1, info.GSICount)
}

func TestInternal_ToAttributeValue_Extended(t *testing.T) {
	t.Parallel()
	h := &Handler{}

	val := h.toAttributeValue("10.5", types.ScalarAttributeTypeN)
	assert.Equal(t, &types.AttributeValueMemberN{Value: "10.5"}, val)

	val = h.toAttributeValue("binary", types.ScalarAttributeTypeB)
	assert.Equal(t, &types.AttributeValueMemberB{Value: []byte("binary")}, val)
}

func TestInternal_ResolveIndexKeys_Success(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	desc := &dynamodb.DescribeTableOutput{
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
	}
	pk, sk := h.resolveIndexKeys(desc, "idx1")
	assert.Equal(t, "gpk", pk)
	assert.Equal(t, "gsk", sk)
}

func TestInternal_RenderQueryResults_WithPagination(t *testing.T) {
	t.Parallel()
	h := &Handler{
		Logger: slog.Default(),
	}
	result := QueryResult{
		Items: []map[string]types.AttributeValue{
			{"pk": &types.AttributeValueMemberS{Value: "val1"}},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "val1"},
		},
		Count: 1,
	}
	w := httptest.NewRecorder()
	h.renderQueryResults(w, result)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "val1")
}

func TestInMemClient_RoundTrip(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	c := &InMemClient{Handler: mux}
	req := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
	resp, err := c.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestInternal_ToAttributeValue_Binary(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	val := h.toAttributeValue("data", types.ScalarAttributeTypeB)
	assert.Equal(t, &types.AttributeValueMemberB{Value: []byte("data")}, val)
}

func TestInternal_ExtractIndexInfo_EdgeCases(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	// Nil projection
	info := h.extractIndexInfo(aws.String("idx"), nil, nil, nil)
	assert.Equal(t, "idx", info.IndexName)
	assert.Equal(t, "INCLUDE", info.ProjectionType) // Default if nil
}

func TestInternal_ResolveKeySchema_Error(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	desc := &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{
			TableName: aws.String("table"),
		},
	}
	pk, sk, pkt, skt := h.resolveKeySchema(desc, "non-existent")
	assert.Empty(t, pk)
	assert.Empty(t, sk)
	assert.Empty(t, pkt)
	assert.Empty(t, skt)
}

func TestInternal_ExtractKeys_Empty(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	pk, sk := h.extractKeys(nil)
	assert.Empty(t, pk)
	assert.Empty(t, sk)
}

func TestInternal_ParseQueryRequest_Empty(t *testing.T) {
	t.Parallel()
	h := &Handler{Logger: slog.Default()}
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	_, ok := h.parseQueryRequest(w, req)
	assert.False(t, ok)
	assert.Contains(t, []int{http.StatusUnprocessableEntity, http.StatusBadRequest, http.StatusNotFound}, w.Code)
}

func TestInternal_ExtractTableInfo_GSILSI(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	desc := &types.TableDescription{
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
	}
	info := h.extractTableInfo(desc)
	assert.Equal(t, 1, info.GSICount)
	assert.Equal(t, 1, info.LSICount)
	assert.Equal(t, "gsi", info.GlobalSecondaryIndexes[0].IndexName)
	assert.Equal(t, "lsi", info.LocalSecondaryIndexes[0].IndexName)
}

func TestInternal_RenderTemplate_Success(t *testing.T) {
	t.Parallel()
	h := &Handler{
		layout: template.Must(template.New("layout.html").Parse(`{{define "layout.html"}}ok{{end}}`)),
		Logger: slog.Default(),
	}
	w := httptest.NewRecorder()
	// Use an existing template file to avoid ParseFS error
	h.renderTemplate(w, "doc.html", nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestInternal_RenderQueryResults_Full(t *testing.T) {
	t.Parallel()
	h := &Handler{Logger: slog.Default()}
	result := QueryResult{
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
	}
	w := httptest.NewRecorder()
	h.renderQueryResults(w, result)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestInternal_ParseQueryRequest_Full(t *testing.T) {
	t.Parallel()
	h := &Handler{Logger: slog.Default()}
	form := url.Values{}
	form.Add("indexName", "idx")
	form.Add("partitionKeyName", "pk")
	form.Add("partitionKeyValue", "v")
	form.Add("sortKeyOperator", "=")
	form.Add("sortKeyValue", "sv1")
	form.Add("sortKeyValue2", "sv2")
	form.Add("filterExpression", "f")
	form.Add("limit", "10")
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	params, ok := h.parseQueryRequest(w, req)
	assert.True(t, ok)
	assert.Equal(t, "idx", params.IndexName)
	assert.Equal(t, "sv2", params.SortKeyValue2)
}
