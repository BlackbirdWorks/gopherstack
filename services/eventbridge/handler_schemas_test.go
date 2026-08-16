package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestHandler_Schema_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "h-schema-reg"})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "CreateSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
		"Type":         "OpenApi3",
		"Content":      `{"openapi":"3.0.0","info":{"title":"Test","version":"1.0"},"paths":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-schema")

	rec = auditMakeRequest(t, h, e, "DescribeSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListSchemas", map[string]any{
		"RegistryName": "h-schema-reg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-schema")

	rec = auditMakeRequest(t, h, e, "SearchSchemas", map[string]any{
		"RegistryName": "h-schema-reg",
		"Keywords":     "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
		"Content":      `{"openapi":"3.0.0","info":{"title":"Test","version":"2.0"},"paths":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"2"`)

	rec = auditMakeRequest(t, h, e, "ListSchemaVersions", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeSchemaVersion", map[string]any{
		"RegistryName":  "h-schema-reg",
		"SchemaName":    "h-schema",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteSchemaVersion", map[string]any{
		"RegistryName":  "h-schema-reg",
		"SchemaName":    "h-schema",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CodeBinding_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "h-cb-reg"})
	require.NoError(t, err)
	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "h-cb-reg",
		SchemaName:   "h-cb-schema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "PutCodeBinding", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
		"Language":     "Python36",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_COMPLETE")

	rec = auditMakeRequest(t, h, e, "DescribeCodeBinding", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
		"Language":     "Python36",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListCodeBindings", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Python36")

	rec = auditMakeRequest(t, h, e, "GetCodeBindingSource", map[string]any{
		"RegistryName":  "h-cb-reg",
		"SchemaName":    "h-cb-schema",
		"Language":      "Python36",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GetDiscoveredSchema(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "GetDiscoveredSchema", map[string]any{
		"Events": []string{`{"source":"myapp","detail-type":"T","detail":{}}`},
		"Type":   "OpenApi3",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "openapi")
}

func TestHandler_CreateSchema_InvalidTypeReturns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typ        string
		wantStatus int
	}{
		{name: "valid type OpenApi3 returns 200", typ: "OpenApi3", wantStatus: http.StatusOK},
		{name: "valid type JSONSchemaDraft4 returns 200", typ: "JSONSchemaDraft4", wantStatus: http.StatusOK},
		{name: "invalid type returns 400", typ: "Avro", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := newBackend()
			h := eventbridge.NewHandler(b)

			_, err := b.CreateRegistry(
				context.Background(),
				eventbridge.CreateRegistryInput{RegistryName: "type-h-reg"},
			)
			require.NoError(t, err)

			rec := auditMakeRequest(t, h, e, "CreateSchema", map[string]any{
				"RegistryName": "type-h-reg",
				"SchemaName":   "s-" + tt.typ,
				"Type":         tt.typ,
				"Content":      `{"openapi":"3.0.0","info":{"title":"T","version":"1.0"},"paths":{}}`,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidParameterException")
			}
		})
	}
}

func schemasBatch2Setup(t *testing.T, registryName, schemaName string, extraVersions int) *eventbridge.InMemoryBackend {
	t.Helper()

	b := newBackend()
	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: registryName})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: registryName,
		SchemaName:   schemaName,
		Type:         "OpenApi3",
		Content:      `{"openapi":"3.0.0","info":{"title":"S","version":"1.0"},"paths":{}}`,
	})
	require.NoError(t, err)

	for i := range extraVersions {
		_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
			RegistryName: registryName,
			SchemaName:   schemaName,
			Content:      `{"openapi":"3.0.0","info":{"title":"S","version":"` + string(rune('2'+i)) + `"},"paths":{}}`,
		})
		require.NoError(t, err)
	}

	return b
}

func TestHandler_DeleteSchemaVersion_LastVersionReturns400(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := schemasBatch2Setup(t, "dv-reg", "dv-schema", 0)
	h := eventbridge.NewHandler(b)

	// Attempting to delete the only version returns 400.
	rec := auditMakeRequest(t, h, e, "DeleteSchemaVersion", map[string]any{
		"RegistryName":  "dv-reg",
		"SchemaName":    "dv-schema",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidParameterException", body["__type"])

	// Schema still exists and is intact.
	described, descErr := b.DescribeSchema(context.Background(), "dv-reg", "dv-schema", "")
	require.NoError(t, descErr)
	assert.Equal(t, "1", described.SchemaVersion)
}
