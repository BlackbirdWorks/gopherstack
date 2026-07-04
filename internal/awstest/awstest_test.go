package awstest_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/awstest"
)

func TestNewContextNilBody(t *testing.T) {
	t.Parallel()

	c, rec := awstest.NewContext(http.MethodGet, "/health", nil)
	require.NotNil(t, c)
	require.NotNil(t, rec)
	assert.Equal(t, http.MethodGet, c.Request().Method)
	assert.Equal(t, "/health", c.Request().URL.Path)
}

func TestNewJSONContextSetsBodyAndHeader(t *testing.T) {
	t.Parallel()

	type payload struct {
		Name string `json:"name"`
	}
	c, _ := awstest.NewJSONContext(t, http.MethodPost, "/", payload{Name: "x"})

	assert.Equal(t, "application/json", c.Request().Header.Get("Content-Type"))

	var got payload
	require.NoError(t, c.Bind(&got))
	assert.Equal(t, "x", got.Name)
}

func TestDecodeJSON(t *testing.T) {
	t.Parallel()

	_, rec := awstest.NewContext(http.MethodGet, "/", nil)
	rec.Body.WriteString(`{"k":"v"}`)

	var got map[string]string
	awstest.DecodeJSON(t, rec, &got)
	assert.Equal(t, map[string]string{"k": "v"}, got)
}
