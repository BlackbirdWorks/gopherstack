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

// ---------------------------------------------------------------------------
// Issue 1: CreateSchema rejects invalid Type values
//
// AWS only accepts "OpenApi3" and "JSONSchemaDraft4" as schema types.
// Supplying any other value must return InvalidParameterException (400).
// Previous implementation accepted any non-empty string.
// ---------------------------------------------------------------------------

func TestSchemasBatch2_CreateSchema_InvalidTypeRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     string
		wantErr bool
	}{
		{name: "OpenApi3 is valid", typ: "OpenApi3", wantErr: false},
		{name: "JSONSchemaDraft4 is valid", typ: "JSONSchemaDraft4", wantErr: false},
		{name: "unknown type is rejected", typ: "SomethingElse", wantErr: true},
		{name: "lower-case openapi3 is rejected", typ: "openapi3", wantErr: true},
		{name: "OPENAPI3 all-caps is rejected", typ: "OPENAPI3", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "type-reg"})
			require.NoError(t, err)

			_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
				RegistryName: "type-reg",
				SchemaName:   "my-schema",
				Type:         tt.typ,
				Content:      `{"openapi":"3.0.0","info":{"title":"T","version":"1.0"},"paths":{}}`,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemasBatch2_Handler_CreateSchema_InvalidTypeReturns400(t *testing.T) {
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

			_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "type-h-reg"})
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

// ---------------------------------------------------------------------------
// Issue 2: Built-in registries cannot be deleted or created
//
// AWS has two read-only registries: "aws.events" and "discovered-schemas".
// Attempting to delete them returns ForbiddenException (403).
// Attempting to create a registry with these names also returns ForbiddenException (403).
// Previous implementation allowed both operations.
// ---------------------------------------------------------------------------

func TestSchemasBatch2_DeleteBuiltinRegistry_Forbidden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registryName string
		wantForbid   bool
	}{
		{name: "aws.events cannot be deleted", registryName: "aws.events", wantForbid: true},
		{name: "discovered-schemas cannot be deleted", registryName: "discovered-schemas", wantForbid: true},
		{name: "user registry can be deleted", registryName: "my-custom-registry", wantForbid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if !tt.wantForbid {
				_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: tt.registryName})
				require.NoError(t, err)
			}

			err := b.DeleteRegistry(context.Background(), tt.registryName)

			if tt.wantForbid {
				require.Error(t, err)
				assert.ErrorIs(t, err, eventbridge.ErrForbiddenOperation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemasBatch2_CreateBuiltinRegistry_Forbidden(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registryName string
		wantForbid   bool
	}{
		{name: "aws.events cannot be created", registryName: "aws.events", wantForbid: true},
		{name: "discovered-schemas cannot be created", registryName: "discovered-schemas", wantForbid: true},
		{name: "user registry can be created", registryName: "my-valid-registry", wantForbid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: tt.registryName})

			if tt.wantForbid {
				require.Error(t, err)
				assert.ErrorIs(t, err, eventbridge.ErrForbiddenOperation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemasBatch2_Handler_BuiltinRegistry_Returns403(t *testing.T) {
	t.Parallel()

	builtinNames := []string{"aws.events", "discovered-schemas"}

	for _, name := range builtinNames {
		t.Run("delete-"+name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := newBackend()
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "DeleteRegistry", map[string]any{
				"RegistryName": name,
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "ForbiddenException")
		})

		t.Run("create-"+name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := newBackend()
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "CreateRegistry", map[string]any{
				"RegistryName": name,
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "ForbiddenException")
		})
	}
}

// ---------------------------------------------------------------------------
// Issue 3: DeleteSchemaVersion rejects deletion of the last remaining version
//
// AWS returns BadRequestException when you attempt to delete the only remaining
// version of a schema. Versions 2+ can be deleted. Version 1 cannot be deleted
// while it is the only version.
// Previous implementation deleted the last version, leaving a stale schema.
// ---------------------------------------------------------------------------

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

func TestSchemasBatch2_DeleteSchemaVersion_LastVersionRejected(t *testing.T) {
	t.Parallel()

	t.Run("deleting only version is rejected", func(t *testing.T) {
		t.Parallel()

		b := schemasBatch2Setup(t, "only-reg", "only-schema", 0)
		err := b.DeleteSchemaVersion(context.Background(), "only-reg", "only-schema", "1")
		require.Error(t, err)
		assert.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
	})

	t.Run("deleting older version of two succeeds", func(t *testing.T) {
		t.Parallel()

		b := schemasBatch2Setup(t, "two-reg", "two-schema", 1)
		require.NoError(t, b.DeleteSchemaVersion(context.Background(), "two-reg", "two-schema", "1"))
	})

	t.Run("deleting latest of two versions succeeds", func(t *testing.T) {
		t.Parallel()

		b := schemasBatch2Setup(t, "latest-reg", "latest-schema", 1)
		require.NoError(t, b.DeleteSchemaVersion(context.Background(), "latest-reg", "latest-schema", "2"))
	})
}

func TestSchemasBatch2_Handler_DeleteSchemaVersion_LastVersionReturns400(t *testing.T) {
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
