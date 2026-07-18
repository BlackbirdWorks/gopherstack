package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_Model_Schema(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	schema := `{"type":"object","properties":{"id":{"type":"string"}}}`
	model, err := b.CreateModel(api.APIID, apigatewayv2.CreateModelInput{
		Name:        "MyModel",
		Schema:      schema,
		ContentType: "application/json",
	})
	require.NoError(t, err)
	assert.Equal(t, schema, model.Schema)
	assert.Equal(t, "application/json", model.ContentType)

	got, err := b.GetModel(api.APIID, model.ModelID)
	require.NoError(t, err)
	assert.Equal(t, schema, got.Schema)
}
