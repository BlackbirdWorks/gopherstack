package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestSchema_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "schema-reg"})
	require.NoError(t, err)

	content := `{"openapi":"3.0.0","info":{"title":"MySchema","version":"1.0"},"paths":{}}`
	schema, err := b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "schema-reg",
		SchemaName:   "MySchema",
		Type:         "OpenApi3",
		Content:      content,
		Description:  "first schema",
	})
	require.NoError(t, err)
	assert.Equal(t, "MySchema", schema.SchemaName)
	assert.Equal(t, "1", schema.SchemaVersion)
	assert.Equal(t, content, schema.Content)
	assert.NotEmpty(t, schema.SchemaArn)

	described, err := b.DescribeSchema(context.Background(), "schema-reg", "MySchema", "")
	require.NoError(t, err)
	assert.Equal(t, "MySchema", described.SchemaName)

	updated, err := b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "schema-reg",
		SchemaName:   "MySchema",
		Content:      `{"openapi":"3.0.0","info":{"title":"MySchema","version":"2.0"},"paths":{}}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "2", updated.SchemaVersion)

	schemas, _, err := b.ListSchemas(context.Background(), "schema-reg", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, schemas, 1)

	err = b.DeleteSchema(context.Background(), "schema-reg", "MySchema")
	require.NoError(t, err)

	_, err = b.DescribeSchema(context.Background(), "schema-reg", "MySchema", "")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestSchema_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-schema-reg"})
	require.NoError(t, err)

	input := eventbridge.CreateSchemaInput{
		RegistryName: "dup-schema-reg",
		SchemaName:   "MySchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	}

	_, err = b.CreateSchema(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestSchema_RegistryDeleteCascadeSchemas(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "cascade-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "cascade-reg",
		SchemaName:   "CascadedSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	err = b.DeleteRegistry(context.Background(), "cascade-reg")
	require.NoError(t, err)

	// Schema should no longer be accessible.
	_, err = b.DescribeSchema(context.Background(), "cascade-reg", "CascadedSchema", "")
	require.Error(t, err)
}

func TestSchema_SearchByKeyword(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "search-reg"})
	require.NoError(t, err)

	for _, name := range []string{"OrderSchema", "UserSchema", "PaymentSchema"} {
		_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
			RegistryName: "search-reg",
			SchemaName:   name,
			Type:         "OpenApi3",
			Content:      `{"title":"` + name + `"}`,
		})
		require.NoError(t, err)
	}

	results, _, err := b.SearchSchemas(context.Background(), "search-reg", "Order", "", 0)
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "OrderSchema", results[0].SchemaName)
}

func TestSchemaVersions_ListAndDescribe(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "ver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Content:      `{"v":3}`,
	})
	require.NoError(t, err)

	versions, _, err := b.ListSchemaVersions(context.Background(), "ver-reg", "VersionedSchema", "", 0)
	require.NoError(t, err)
	assert.Len(t, versions, 3)
	assert.Equal(t, "1", versions[0].SchemaVersion)
	assert.Equal(t, "3", versions[2].SchemaVersion)

	sv, err := b.DescribeSchemaVersion(context.Background(), "ver-reg", "VersionedSchema", "2")
	require.NoError(t, err)
	assert.Equal(t, "2", sv.SchemaVersion)
	assert.Contains(t, sv.Content, `"v":2`)
}

func TestSchemaVersions_DeleteSpecificVersion(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "delver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "delver-reg",
		SchemaName:   "DelSchema",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "delver-reg",
		SchemaName:   "DelSchema",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	err = b.DeleteSchemaVersion(context.Background(), "delver-reg", "DelSchema", "1")
	require.NoError(t, err)

	versions, _, err := b.ListSchemaVersions(context.Background(), "delver-reg", "DelSchema", "", 0)
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, "2", versions[0].SchemaVersion)
}

func TestSchemaVersions_DescribeWithVersion(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "descver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "descver-reg",
		SchemaName:   "S",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "descver-reg",
		SchemaName:   "S",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	// Describe with specific version.
	sv, err := b.DescribeSchema(context.Background(), "descver-reg", "S", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", sv.SchemaVersion)
	assert.Contains(t, sv.Content, `"v":1`)

	// Describe without version returns latest.
	latest, err := b.DescribeSchema(context.Background(), "descver-reg", "S", "")
	require.NoError(t, err)
	assert.Equal(t, "2", latest.SchemaVersion)
}

func TestCodeBinding_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "cb-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	binding, err := b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Python36",
	})
	require.NoError(t, err)
	assert.Equal(t, "Python36", binding.Language)
	assert.Equal(t, "CREATE_COMPLETE", binding.Status)

	described, err := b.DescribeCodeBinding(context.Background(), eventbridge.DescribeCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Python36",
	})
	require.NoError(t, err)
	assert.Equal(t, "Python36", described.Language)

	_, err = b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Java8",
	})
	require.NoError(t, err)

	bindings, _, err := b.ListCodeBindings(context.Background(), eventbridge.ListCodeBindingsInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
	})
	require.NoError(t, err)
	assert.Len(t, bindings, 2)
}

func TestCodeBinding_GetSource(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "src-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "src-reg",
		SchemaName:   "SrcSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	_, err = b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "src-reg",
		SchemaName:   "SrcSchema",
		Language:     "TypeScript3",
	})
	require.NoError(t, err)

	src, err := b.GetCodeBindingSource(context.Background(), "src-reg", "SrcSchema", "TypeScript3", "1")
	require.NoError(t, err)
	assert.NotEmpty(t, src)
}

func TestCodeBinding_NotFoundErrors(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "nf-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "nf-reg",
		SchemaName:   "NFSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	_, err = b.DescribeCodeBinding(context.Background(), eventbridge.DescribeCodeBindingInput{
		RegistryName: "nf-reg",
		SchemaName:   "NFSchema",
		Language:     "Go1",
	})
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestGetDiscoveredSchema_ReturnsContent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	content, err := b.GetDiscoveredSchema(context.Background(), eventbridge.GetDiscoveredSchemaInput{
		Events: []string{`{"source":"myapp","detail-type":"OrderPlaced","detail":{"orderId":"o1"}}`},
		Type:   "OpenApi3",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, content)
}

func TestGetDiscoveredSchema_RejectsEmptyEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetDiscoveredSchema(context.Background(), eventbridge.GetDiscoveredSchemaInput{
		Events: []string{},
		Type:   "OpenApi3",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestCreateSchema_InvalidTypeRejected(t *testing.T) {
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

func TestDeleteSchemaVersion_LastVersionRejected(t *testing.T) {
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
