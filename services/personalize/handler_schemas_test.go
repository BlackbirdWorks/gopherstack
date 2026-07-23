package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Schema_FieldRetention(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	schemaContent := `{"type":"record","name":"Interactions","namespace":"com.amazonaws.personalize.schema",` +
		`"fields":[{"name":"USER_ID","type":"string"},{"name":"ITEM_ID","type":"string"},` +
		`{"name":"TIMESTAMP","type":"long"}],"version":"1.0"}`
	rec := personalizeDo(t, h, "CreateSchema", map[string]any{
		"name":   "my-schema",
		"schema": schemaContent,
		"domain": "ECOMMERCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	schemaArn := personalizeUnmarshal(t, rec)["schemaArn"].(string)
	assert.NotEmpty(t, schemaArn)

	rec = personalizeDo(t, h, "DescribeSchema", map[string]any{"schemaArn": schemaArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	s := m["schema"].(map[string]any)
	assert.Equal(t, "my-schema", s["name"])
	assert.Equal(t, schemaContent, s["schema"])
	assert.Equal(t, "ECOMMERCE", s["domain"])
	assert.NotEmpty(t, s["creationDateTime"])
}

// TestPersonalize_Schema_InvalidDomain locks that CreateSchema rejects a
// domain outside the real types.Domain enum (ECOMMERCE/VIDEO_ON_DEMAND);
// real AWS rejects an unrecognized value rather than silently accepting it.
func TestPersonalize_Schema_InvalidDomain(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateSchema", map[string]any{
		"name":   "bad-domain-schema",
		"schema": `{"type":"record"}`,
		"domain": "NOT_A_REAL_DOMAIN",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "InvalidInputException", m["__type"])
}

func TestPersonalize_Schema_List(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	personalizeDo(t, h, "CreateSchema", map[string]any{"name": "s1"})
	personalizeDo(t, h, "CreateSchema", map[string]any{"name": "s2"})

	rec := personalizeDo(t, h, "ListSchemas", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := personalizeUnmarshal(t, rec)
	schemas := listed["schemas"].([]any)
	assert.Len(t, schemas, 2)
}

// --- Solution ---
