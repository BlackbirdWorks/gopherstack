package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// fullRegisterConnectionTypeSpec returns a glue.RegisterConnectionTypeSpec
// satisfying every required member of RegisterConnectionType
// (glue@v1.152.0 api_op_RegisterConnectionType.go:38-70: ConnectionProperties,
// ConnectorAuthenticationConfiguration, IntegrationType and RestConfiguration).
func fullRegisterConnectionTypeSpec() glue.RegisterConnectionTypeSpec {
	return glue.RegisterConnectionTypeSpec{
		IntegrationType:      "REST",
		ConnectionProperties: map[string]any{"Url": map[string]any{"Name": "endpoint"}},
		ConnectorAuthenticationConfiguration: map[string]any{
			"AuthenticationTypes": []any{"BASIC"},
		},
		RestConfiguration: map[string]any{},
	}
}

// newBackendWithConn returns a backend seeded with a single JDBC connection.
func newBackendWithConn(t *testing.T, connName string) *glue.InMemoryBackend {
	t.Helper()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateConnection(connName, "JDBC", map[string]string{
		"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
	}, nil)
	require.NoError(t, err)

	return b
}

// TestBackend_DescribeEntity verifies real schema Fields are returned for known
// entities and EntityNotFoundException for unknown entities / missing connections.
func TestBackend_DescribeEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connName   string
		queryConn  string
		entity     string
		wantFields []string
		wantErr    bool
	}{
		{
			name:       "known Account entity returns schema",
			connName:   "c",
			queryConn:  "c",
			entity:     "Account",
			wantFields: []string{"Id", "Name", "AnnualRevenue", "NumberOfEmployees", "IsActive"},
		},
		{
			name:       "case insensitive entity lookup",
			connName:   "c",
			queryConn:  "c",
			entity:     "cOnTaCt",
			wantFields: []string{"Id", "FirstName", "LastName", "Email"},
		},
		{
			name:      "unknown entity is not found",
			connName:  "c",
			queryConn: "c",
			entity:    "Nonexistent",
			wantErr:   true,
		},
		{
			name:      "missing connection is not found",
			connName:  "c",
			queryConn: "other",
			entity:    "Account",
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackendWithConn(t, tc.connName)

			fields, err := b.DescribeEntity(tc.queryConn, tc.entity)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, fields)

			got := make(map[string]glue.EntityField, len(fields))
			for _, f := range fields {
				got[f.FieldName] = f
				assert.NotEmpty(t, f.FieldType, "field %s must have a FieldType", f.FieldName)
			}

			for _, want := range tc.wantFields {
				assert.Contains(t, got, want)
			}
		})
	}
}

// TestBackend_ListEntities verifies the entity catalog is returned sorted with
// accurate descriptors, and that a missing connection errors.
func TestBackend_ListEntities(t *testing.T) {
	t.Parallel()

	b := newBackendWithConn(t, "c")

	entities, err := b.ListEntities("c", "")
	require.NoError(t, err)
	require.NotEmpty(t, entities)

	names := make([]string, 0, len(entities))
	for _, e := range entities {
		assert.NotEmpty(t, e.EntityName)
		names = append(names, e.EntityName)
	}

	assert.Subset(t, names, []string{"Account", "Contact", "Customer", "Order", "Product"})

	// Sorted ascending by upper-cased name.
	assert.IsIncreasing(t, names)

	_, err = b.ListEntities("missing", "")
	require.Error(t, err)
}

// TestBackend_GetEntityRecords verifies deterministic, schema-shaped sample records,
// pagination, limit clamping, and EntityNotFound for unknown entities.
func TestBackend_GetEntityRecords(t *testing.T) {
	t.Parallel()

	t.Run("records conform to schema", func(t *testing.T) {
		t.Parallel()

		b := newBackendWithConn(t, "c")

		records, token, err := b.GetEntityRecords("c", "Order", 0, "")
		require.NoError(t, err)
		require.NotEmpty(t, records)
		assert.Empty(t, token)

		for _, r := range records {
			assert.Contains(t, r, "Id")
			assert.Contains(t, r, "TotalAmount")
			assert.Contains(t, r, "OrderDate")
			// Timestamp fields must marshal as epoch numbers, never time strings.
			_, ok := r["OrderDate"].(float64)
			assert.True(t, ok, "OrderDate must be an epoch number")
		}
	})

	t.Run("determinism", func(t *testing.T) {
		t.Parallel()

		b := newBackendWithConn(t, "c")

		first, _, err := b.GetEntityRecords("c", "Account", 0, "")
		require.NoError(t, err)
		second, _, err := b.GetEntityRecords("c", "Account", 0, "")
		require.NoError(t, err)
		assert.Equal(t, first, second)
	})

	t.Run("pagination via limit and token", func(t *testing.T) {
		t.Parallel()

		b := newBackendWithConn(t, "c")

		page1, token, err := b.GetEntityRecords("c", "Account", 2, "")
		require.NoError(t, err)
		assert.Len(t, page1, 2)
		require.NotEmpty(t, token)

		page2, _, err := b.GetEntityRecords("c", "Account", 2, token)
		require.NoError(t, err)
		assert.NotEmpty(t, page2)
		assert.NotEqual(t, page1[0]["Id"], page2[0]["Id"])
	})

	t.Run("unknown entity not found", func(t *testing.T) {
		t.Parallel()

		b := newBackendWithConn(t, "c")

		_, _, err := b.GetEntityRecords("c", "Ghost", 0, "")
		require.Error(t, err)
	})

	t.Run("missing connection not found", func(t *testing.T) {
		t.Parallel()

		b := newBackendWithConn(t, "c")

		_, _, err := b.GetEntityRecords("missing", "Account", 0, "")
		require.Error(t, err)
	})
}

// TestBackend_ConnectionTypeRegistry verifies the connection-type registry:
// built-ins are undeletable, custom types can be registered/deleted, and lookups
// are case-insensitive.
func TestBackend_ConnectionTypeRegistry(t *testing.T) {
	t.Parallel()

	t.Run("built-in is undeletable", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		err := b.DeleteConnectionType("SALESFORCE")
		require.ErrorIs(t, err, glue.ErrConnectionTypeBuiltIn)
	})

	t.Run("unknown delete is not found", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		err := b.DeleteConnectionType("NoSuchThing")
		require.Error(t, err)
		require.NotErrorIs(t, err, glue.ErrConnectionTypeBuiltIn)
	})

	t.Run("register then delete custom", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		info, err := b.RegisterConnectionType("my-conn", "desc", fullRegisterConnectionTypeSpec())
		require.NoError(t, err)
		assert.Equal(t, "MY-CONN", info.ConnectionType)
		assert.False(t, info.BuiltIn)
		assert.Equal(t, 1, glue.CustomConnectionTypeCountForTest(b))

		// Describe finds it (case-insensitive).
		got, err := b.DescribeConnectionType("MY-conn")
		require.NoError(t, err)
		assert.Equal(t, "MY-CONN", got.ConnectionType)

		require.NoError(t, b.DeleteConnectionType("my-conn"))
		assert.Equal(t, 0, glue.CustomConnectionTypeCountForTest(b))

		// Second delete is not found.
		require.Error(t, b.DeleteConnectionType("my-conn"))
	})

	t.Run("register reserved built-in rejected", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		_, err := b.RegisterConnectionType("jdbc", "", fullRegisterConnectionTypeSpec())
		require.Error(t, err)
	})

	t.Run("list includes built-ins and custom sorted", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		_, err := b.RegisterConnectionType("aaa-custom", "", fullRegisterConnectionTypeSpec())
		require.NoError(t, err)

		list := b.ListConnectionTypes()
		require.NotEmpty(t, list)

		names := make([]string, 0, len(list))
		found := false
		for _, info := range list {
			names = append(names, info.ConnectionType)
			if info.ConnectionType == "AAA-CUSTOM" {
				found = true
			}
		}

		assert.True(t, found, "custom type must be listed")
		assert.Contains(t, names, "JDBC")
		assert.IsIncreasing(t, names)
	})

	t.Run("describe unknown not found", func(t *testing.T) {
		t.Parallel()

		b := glue.NewInMemoryBackend("000000000000", "us-east-1")

		_, err := b.DescribeConnectionType("does-not-exist")
		require.Error(t, err)
	})
}
